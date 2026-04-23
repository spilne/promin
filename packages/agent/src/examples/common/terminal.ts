/**
 * Terminal renderer for console agent examples.
 *
 * Features:
 *   - Animated braille spinner with elapsed time (Claude Code style)
 *   - Synchronized output mode (\x1b[?2026h) to prevent flicker
 *   - printAbove() — interrupt-safe output that restores the readline prompt
 *   - showPane() — transient command overlay that erases on dismiss (any key)
 *   - Clay-colored ❯ prompt matching Anthropic's brand color
 *   - Multi-line spinner erase with \x1b[J (handles wrapped labels)
 *   - Wide-character (CJK / emoji) column-width support
 *
 * Usage:
 *   const term = new Terminal(rl);
 *   term.startSpinner("thinking...");
 *   term.startSpinner("→ readFile");          // relabels, keeps elapsed time
 *   term.stopSpinner();
 *   term.printAbove("[scheduler] fired");
 *   await term.showPane("Title", ["line 1", "line 2"]);  // dismisses on keypress
 *   term.close();                             // on exit
 */

import type { Interface } from "node:readline";
import { TerminalIO } from "./terminal-io.ts";

export interface TreeNode {
  label: string;
  children: TreeNode[];
  expanded: boolean;
}

/**
 * Renderer interface for agent UI output — the boundary between agent logic
 * and any concrete display implementation (terminal, web, native, test stub).
 *
 * Covers the agent-facing surface only. Terminal-specific concerns
 * (prompt animation, readline state flags) live on the Terminal subtype.
 */
export interface AgentUIRenderer {
  /** Start or relabel the activity spinner. */
  startSpinner(label: string): void;
  /** Stop the spinner and erase the status line. Idempotent. */
  stopSpinner(): void;
  /** Milliseconds elapsed since the spinner started. 0 when not spinning. */
  readonly elapsedMs: number;
  /**
   * Buffer a streaming text chunk for batched output.
   * Coalesces chunks arriving in the same event-loop tick into one write.
   */
  writeChunk(s: string): void;
  /** Flush any buffered chunks immediately. Call at end-of-stream. */
  flushChunks(): void;
  /**
   * Print lines above the current spinner / prompt without disrupting the display.
   * Safe to call at any time (scheduler ticks, Ctrl+C handlers, etc.).
   */
  printAbove(...lines: string[]): void;
  /** Show a transient bordered pane. Resolves when the user dismisses it. */
  showPane(title: string, lines: string[]): Promise<void>;
  /** Show a keyboard-navigable tree pane. Resolves when the user dismisses it. */
  showInteractiveTree(title: string, roots: TreeNode[]): Promise<void>;
  /** Release all resources. Call on process / component exit. */
  close(): void;
}

const FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const FRAMES_PLAIN = ["-", "\\", "|", "/"];
const FRAME_MS = 80;

const CLAY = "\x1b[38;2;217;119;87m"; // Anthropic clay #d97757
const DIM = "\x1b[2m";
const RST = "\x1b[0m";

// Readline ignore markers: escape codes inside \x01…\x02 don't count toward
// visible width, so cursor-left / line-wrap stay correct.
const RI = "\x01";
const RE = "\x02";

// Two-chevron animated prompt frames: clay ❯❯ wave (full → half → dim → half).
const PROMPT_FRAMES = [
  `${CLAY}❯${RST}${DIM}${CLAY}❯${RST}`, // bright + dim
  `${DIM}${CLAY}❯${RST}${CLAY}❯${RST}`, // dim + bright
] as const;
const PROMPT_FRAME_MS = 600;

/** readline.question() prompt string — clay ❯❯, newline before.
 *  Wrapped in RL_PROMPT_IGNORE markers so readline counts width correctly. */
export const PROMPT = `\n${RI}${CLAY}${RE}❯❯${RI}${RST}${RE} `;
/** Plain-text fallback prompt for NO_COLOR / dumb terminals. */
export const PLAIN_PROMPT = "\n> ";

/** Returns true when the terminal cannot render ANSI color or is not a TTY. */
export function isDumbTerminal(): boolean {
  return (
    (!!process.env.NO_COLOR && process.env.NO_COLOR !== "") ||
    process.env.TERM === "dumb" ||
    !process.stdout.isTTY
  );
}

export class Terminal implements AgentUIRenderer {
  /** True while readline.question() is waiting for input. */
  inPrompt = false;

  /**
   * Suppress spinner renders — set during background scheduler turns so their
   * tool activity doesn't corrupt the user-facing status line.
   */
  suppress = false;

  /** True when the agent streamed text on the current line without a trailing \n. */
  agentHasTextOnLine = false;

  /** True when NO_COLOR is set, TERM=dumb, or stdout is not a TTY. */
  readonly noColor: boolean;

  private readonly _rl: Interface;
  private readonly _io: TerminalIO;
  private _activeSignal: AbortSignal | null = null;
  private _timer: ReturnType<typeof setInterval> | null = null;
  private _frame = 0;
  private _label = "";
  private _startMs = 0;
  private _renderedLines = 1;
  private _promptTimer: ReturnType<typeof setInterval> | null = null;
  private _promptFrame = 0;
  private _chunkBuf = "";
  private _chunkFlush: ReturnType<typeof setImmediate> | null = null;

  constructor(rl: Interface) {
    this._rl = rl;
    this._io = new TerminalIO(rl);
    this.noColor = isDumbTerminal();
  }

  /** The prompt string to pass to rl.question() — colored or plain based on noColor. */
  get promptStr(): string {
    return this.noColor ? PLAIN_PROMPT : PROMPT;
  }

  /**
   * Start or relabel the spinner.
   *
   * - First call: starts the animation timer, resets elapsed clock.
   *   Inserts a \n if agent text is on the current line.
   * - Subsequent calls while spinning: updates label only (elapsed keeps running).
   */
  startSpinner(label: string): void {
    this._label = label;
    if (this._timer) {
      this._render(); // relabel in place
      return;
    }
    this._startMs = Date.now();
    this._frame = 0;
    if (this.agentHasTextOnLine) {
      process.stdout.write("\n");
      this.agentHasTextOnLine = false;
    }
    this._timer = setInterval(() => {
      this._frame++;
      this._render();
    }, FRAME_MS);
    this._render();
  }

  /** Stop the spinner and erase the status line. Idempotent. */
  stopSpinner(): void {
    if (!this._timer) return;
    clearInterval(this._timer);
    this._timer = null;
    this._erase();
  }

  /**
   * Print lines above the readline prompt, restoring the prompt + typed text.
   * When not at a prompt (agent mid-turn), clears the spinner and writes inline.
   * Safe to call at any time: from scheduler ticks, Ctrl+C handlers, etc.
   */
  printAbove(...lines: string[]): void {
    if (this._timer) this._erase(); // clear spinner line first

    this._sync(() => {
      if (this.inPrompt) {
        // biome-ignore lint/suspicious/noExplicitAny: readline.line is public but not in @types
        const typed = (this._rl as any).line ?? "";
        process.stdout.write("\r\x1b[K");
        for (const line of lines) process.stdout.write(`${line}\n`);
        // Redraw prompt + in-progress input using the current animation frame.
        const frame = PROMPT_FRAMES[this._promptFrame];
        process.stdout.write(`\n${frame} ${typed}`);
      } else {
        if (this.agentHasTextOnLine) process.stdout.write("\n");
        process.stdout.write("\r\x1b[K");
        for (const line of lines) process.stdout.write(`${line}\n`);
        this.agentHasTextOnLine = false;
      }
    });
  }

  /**
   * Show a transient command pane — a bordered overlay with a title and lines.
   * Waits for any keypress, then erases the pane if it fits on screen.
   * Content is never added to scroll history when it fits; for oversized output
   * it remains visible and the user can scroll up.
   *
   * Call with `await` from command handlers between readline questions.
   */
  async showPane(title: string, lines: string[]): Promise<void> {
    let cols = process.stdout.columns ?? 80;
    let rows = process.stdout.rows ?? 24;

    const buildOut = (c: number): string[] => {
      const hr = `${DIM}${"─".repeat(c)}${RST}`;
      return [
        "",
        hr,
        `  ${CLAY}${title}${RST}`,
        hr,
        ...lines,
        hr,
        `  ${DIM}any key · close${RST}`,
        hr,
        "",
      ];
    };

    let out = buildOut(cols);
    for (const line of out) process.stdout.write(`${line}\n`);

    const onResize = () => {
      const prevLen = out.length;
      cols = process.stdout.columns ?? 80;
      rows = process.stdout.rows ?? 24;
      process.stdout.write(`\x1b[${prevLen}A\x1b[0J`);
      out = buildOut(cols);
      for (const line of out) process.stdout.write(`${line}\n`);
    };

    process.stdout.on("resize", onResize);
    await this._waitForKey();
    process.stdout.off("resize", onResize);

    // Erase only if the pane fits — if it scrolled off the top, leave it.
    if (out.length < rows - 1) {
      process.stdout.write(`\x1b[${out.length}A\x1b[0J`);
    }
  }

  /**
   * Render an interactive keyboard-navigable tree pane.
   * ↑↓ navigate · Space/→/Enter expand/collapse · ← collapse or go to parent · q/Esc dismiss.
   * Erases itself on exit (like showPane). Call with `await` between readline questions.
   */
  async showInteractiveTree(title: string, roots: TreeNode[]): Promise<void> {
    let cols = process.stdout.columns ?? 80;
    let rows = process.stdout.rows ?? 24;
    let hr = `${DIM}${"─".repeat(cols)}${RST}`;
    // 4 header lines: "", hr, title, hr
    // 4 footer lines: hr, help, hr, ""
    let VIEW_H = Math.max(1, rows - 8);

    type FlatEntry = { node: TreeNode; depth: number };

    const flatten = (): FlatEntry[] => {
      const out: FlatEntry[] = [];
      const walk = (nodes: TreeNode[], d: number) => {
        for (const n of nodes) {
          out.push({ node: n, depth: d });
          if (n.expanded && n.children.length) walk(n.children, d + 1);
        }
      };
      walk(roots, 0);
      return out;
    };

    const clip = (s: string, w: number): string => {
      if (Terminal._visibleLen(s) <= w) return s;
      // eslint-disable-next-line no-control-regex
      const stripped = s.replace(/\u001b\[[0-9;]*[A-Za-z]/g, "");
      return `${Terminal._clipToWidth(stripped, w - 1)}…`;
    };

    let cursor = 0;
    let scrollTop = 0;
    let rendered = 0;

    const render = (flat: FlatEntry[]) => {
      if (!flat.length) cursor = 0;
      else {
        if (cursor < 0) cursor = 0;
        if (cursor >= flat.length) cursor = flat.length - 1;
      }
      if (cursor < scrollTop) scrollTop = cursor;
      if (cursor >= scrollTop + VIEW_H) scrollTop = cursor - VIEW_H + 1;

      const out: string[] = ["", hr, `  ${CLAY}${title}${RST}`, hr];

      const slice = flat.slice(scrollTop, scrollTop + VIEW_H);
      for (let i = 0; i < VIEW_H; i++) {
        const entry = slice[i];
        if (!entry) {
          out.push("");
          continue;
        }
        const { node, depth } = entry;
        const sel = scrollTop + i === cursor;
        const toggle = node.children.length ? (node.expanded ? "▾ " : "▸ ") : "  ";
        const indent = "  ".repeat(depth);
        const label = clip(`${indent}${toggle}${node.label}`, cols - 4);
        out.push(sel ? `${CLAY}❯${RST} ${label}` : `  ${label}`);
      }

      const note =
        flat.length > VIEW_H
          ? `   ${DIM}${scrollTop + 1}–${Math.min(scrollTop + VIEW_H, flat.length)}/${flat.length}${RST}`
          : "";
      out.push(
        hr,
        `  ${DIM}↑↓ navigate  PgUp/PgDn scroll  Home/End jump  space/→ expand  ← collapse  q close${note}${RST}`,
        hr,
        "",
      );

      this._sync(() => {
        if (rendered > 0) process.stdout.write(`\x1b[${rendered}A\x1b[0J`);
        for (const line of out) process.stdout.write(`${line}\n`);
      });
      rendered = out.length;
    };

    if (!roots.length) {
      // Nothing to navigate — fall back to static pane.
      await this.showPane(title, ["(no data yet)"]);
      return;
    }

    let flat = flatten();
    render(flat);

    const onResize = () => {
      cols = process.stdout.columns ?? 80;
      rows = process.stdout.rows ?? 24;
      hr = `${DIM}${"─".repeat(cols)}${RST}`;
      VIEW_H = Math.max(1, rows - 8);
      render(flat);
    };
    process.stdout.on("resize", onResize);

    while (true) {
      const key = await this._readKey();
      if (key === "q" || key === "\x1b") break;
      if (key === "\x03") {
        process.stdout.write("\n");
        process.exit(0);
      }

      if (key === "up") {
        cursor = Math.max(0, cursor - 1);
      } else if (key === "down") {
        cursor = Math.min(flat.length - 1, cursor + 1);
      } else if (key === " " || key === "\r" || key === "right") {
        const entry = flat[cursor];
        if (entry?.node.children.length) {
          entry.node.expanded = !entry.node.expanded;
          flat = flatten();
          if (cursor >= flat.length) cursor = flat.length - 1;
        }
      } else if (key === "left") {
        const entry = flat[cursor];
        if (entry) {
          if (entry.node.expanded && entry.node.children.length) {
            entry.node.expanded = false;
            flat = flatten();
          } else if (entry.depth > 0) {
            for (let i = cursor - 1; i >= 0; i--) {
              if (flat[i].depth < entry.depth) {
                cursor = i;
                break;
              }
            }
          }
        }
      } else if (key === "pageup") {
        cursor = Math.max(0, cursor - VIEW_H);
      } else if (key === "pagedown") {
        cursor = Math.min(flat.length - 1, cursor + VIEW_H);
      } else if (key === "home") {
        cursor = 0;
      } else if (key === "end") {
        cursor = Math.max(0, flat.length - 1);
      }
      render(flat);
    }

    process.stdout.off("resize", onResize);
    if (rendered < rows - 1) {
      this._sync(() => process.stdout.write(`\x1b[${rendered}A\x1b[0J`));
    }
  }

  /**
   * Start the ❯❯ prompt animation. Call immediately after rl.question().
   * Every 600ms the two chevrons swap brightness, creating a gentle wave.
   * readline still owns input — we just overwrite the visual line in place.
   */
  startPromptAnimation(): void {
    this._promptFrame = 0;
    this._promptTimer = setInterval(() => {
      this._promptFrame = (this._promptFrame + 1) % PROMPT_FRAMES.length;
      this._redrawPrompt();
    }, PROMPT_FRAME_MS);
  }

  /** Stop the prompt animation (call when the user submits input). */
  stopPromptAnimation(): void {
    if (this._promptTimer) {
      clearInterval(this._promptTimer);
      this._promptTimer = null;
    }
  }

  /** Milliseconds elapsed since the spinner started. 0 when the spinner is not running. */
  get elapsedMs(): number {
    return this._timer ? Date.now() - this._startMs : 0;
  }

  /**
   * Buffer a streaming text chunk for low-flicker batched output.
   * Coalesces all chunks that arrive in the same event-loop tick into a single
   * stdout.write, reducing per-chunk syscall overhead and visible stutter.
   */
  writeChunk(s: string): void {
    this._chunkBuf += s;
    if (!this._chunkFlush) {
      this._chunkFlush = setImmediate(() => {
        this._chunkFlush = null;
        const buf = this._chunkBuf;
        this._chunkBuf = "";
        if (buf) process.stdout.write(buf);
      });
    }
  }

  /** Flush any buffered chunks immediately. Call at end-of-stream. */
  flushChunks(): void {
    if (this._chunkFlush) {
      clearImmediate(this._chunkFlush);
      this._chunkFlush = null;
    }
    const buf = this._chunkBuf;
    this._chunkBuf = "";
    if (buf) process.stdout.write(buf);
  }

  /**
   * Show a compact dropdown menu below the current prompt line.
   *
   * Designed for use inside an async readline completer callback — readline
   * pauses input processing while the completer runs, so this has exclusive
   * access to stdin with no readline interference.
   *
   * - Returns the selected item on Enter.
   * - Returns null on Escape, Ctrl+C-exit, or any non-navigation key.
   * - Returns the single item immediately when `items.length === 1`.
   *
   * Rendering uses DEC cursor save/restore (ESC 7 / ESC 8) so the prompt
   * cursor position is preserved regardless of how many items are drawn.
   */
  async showInlineMenu(items: string[]): Promise<string | null> {
    if (items.length === 0) return null;
    if (items.length === 1) return items[0]!;

    const MAX_VISIBLE = 10;
    let cursor = 0;
    let scrollOffset = 0;
    let rendered = 0; // lines drawn below the prompt

    const draw = (): void => {
      this._sync(() => {
        // ESC 7 = save cursor (at current input position)
        process.stdout.write("\x1b7");
        if (rendered > 0) {
          // Erase previous render: go to next line col-0, erase to screen bottom, restore
          process.stdout.write("\r\n\x1b[0J\x1b8\x1b7");
        }
        const end = Math.min(scrollOffset + MAX_VISIBLE, items.length);
        for (let i = scrollOffset; i < end; i++) {
          const sel = i === cursor;
          const label = items[i]!;
          if (sel) {
            process.stdout.write(`\r\n  ${CLAY}❯${RST} ${label}\x1b[K`);
          } else {
            process.stdout.write(`\r\n  ${DIM}  ${label}${RST}\x1b[K`);
          }
        }
        const extra = items.length - end;
        if (extra > 0) {
          process.stdout.write(`\r\n  ${DIM}…${extra} more${RST}\x1b[K`);
        }
        rendered = end - scrollOffset + (extra > 0 ? 1 : 0);
        process.stdout.write("\x1b8"); // ESC 8 = restore cursor to input position
      });
    };

    const erase = (): void => {
      if (rendered === 0) return;
      this._sync(() => process.stdout.write("\x1b7\r\n\x1b[0J\x1b8"));
      rendered = 0;
    };

    draw();

    while (true) {
      const key = await this._readKey();
      if (key === "up") {
        cursor = cursor > 0 ? cursor - 1 : items.length - 1;
        if (cursor < scrollOffset) scrollOffset = cursor;
        else if (cursor === items.length - 1)
          scrollOffset = Math.max(0, items.length - MAX_VISIBLE);
        draw();
      } else if (key === "down") {
        cursor = cursor < items.length - 1 ? cursor + 1 : 0;
        if (cursor >= scrollOffset + MAX_VISIBLE) scrollOffset = cursor - MAX_VISIBLE + 1;
        else if (cursor === 0) scrollOffset = 0;
        draw();
      } else if (key === "\r" || key === "\n") {
        erase();
        return items[cursor] ?? null;
      } else {
        erase();
        if (key === "\x03") {
          process.stdout.write("\n");
          process.exit(0);
        }
        return null;
      }
    }
  }

  /**
   * Register the active turn's AbortSignal so that a pending ask() prompt
   * is rejected immediately when the turn is aborted (e.g. via Ctrl+C).
   * Call with null to clear after the turn completes.
   */
  setActiveSignal(signal: AbortSignal | null): void {
    this._activeSignal = signal;
  }

  /**
   * Stop the spinner, suspend rendering, show a readline question, then restore.
   * Centralises the suppress/inPrompt dance so callers cannot forget a step
   * and accidentally let the spinner overwrite the question prompt.
   *
   * If an active signal (set via setActiveSignal) is already aborted, or aborts
   * while waiting, the promise rejects with an AbortError so the turn can unwind.
   */
  ask(question: string): Promise<string> {
    const signal = this._activeSignal;
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(Object.assign(new Error("Aborted"), { name: "AbortError" }));
        return;
      }
      this.stopSpinner();
      this.suppress = true;
      this.inPrompt = true;
      let settled = false;

      const onAbort = () => {
        if (settled) return;
        settled = true;
        this.inPrompt = false;
        // Write a synthetic newline so the pending readline 'line' listener fires
        // with an empty answer and is removed — prevents it from consuming the
        // next real user input at the REPL prompt.
        this._rl.write("\n");
        this.suppress = false;
        reject(Object.assign(new Error("Aborted"), { name: "AbortError" }));
      };

      signal?.addEventListener("abort", onAbort, { once: true });

      this._rl.question(`\n${question}: `, (v) => {
        signal?.removeEventListener("abort", onAbort);
        if (settled) return;
        settled = true;
        this.suppress = false;
        this.inPrompt = false;
        process.stdout.write("\n");
        resolve(v.trim());
      });
    });
  }

  /**
   * Print a full-width horizontal rule — use between turns to separate responses.
   * Uses dim `─` on color terminals, plain `-` on NO_COLOR / dumb terminals.
   */
  printRule(): void {
    const cols = process.stdout.columns ?? 80;
    const rule = this.noColor ? `-`.repeat(cols) : `${DIM}${"─".repeat(cols)}${RST}`;
    this._sync(() => process.stdout.write(`${rule}\n`));
  }

  /** Release all timers and restore terminal state. Call on process exit. */
  close(): void {
    this.stopSpinner();
    this.stopPromptAnimation();
    this.flushChunks();
    this._io.dispose();
  }

  // Delegate exclusive-listener key I/O to TerminalIO.
  private _waitForKey(): Promise<void> {
    return this._io.waitForKey();
  }

  private _readKey(): Promise<string> {
    return this._io.readKey();
  }

  private _redrawPrompt(): void {
    if (!this.inPrompt) return;
    const frame = PROMPT_FRAMES[this._promptFrame];
    // biome-ignore lint/suspicious/noExplicitAny: readline internals
    const typed = (this._rl as any).line ?? "";
    // biome-ignore lint/suspicious/noExplicitAny: readline internals
    const cursor = (this._rl as any).cursor ?? typed.length;
    this._sync(() => {
      process.stdout.write(`\r\x1b[K${frame} ${typed}`);
      // Reposition cursor if user moved it left inside the typed text.
      if (cursor < typed.length) process.stdout.write(`\x1b[${typed.length - cursor}D`);
    });
  }

  // Returns the terminal column width of a Unicode code point.
  // Double-width: CJK ideographs, Hangul, fullwidth forms, and most emoji.
  private static _charWidth(cp: number): number {
    if (cp < 0x1100) return 1;
    if (
      cp <= 0x115f || // Hangul Jamo
      cp === 0x2329 ||
      cp === 0x232a || // CJK angle brackets
      (cp >= 0x2e80 && cp <= 0x303e) || // CJK Radicals / Kangxi
      (cp >= 0x3041 && cp <= 0x9fff) || // Hiragana … CJK Unified Ideographs
      (cp >= 0xa000 && cp <= 0xa4cf) || // Yi
      (cp >= 0xa960 && cp <= 0xa97f) || // Hangul Jamo Extended-A
      (cp >= 0xac00 && cp <= 0xd7af) || // Hangul Syllables
      (cp >= 0xf900 && cp <= 0xfaff) || // CJK Compatibility Ideographs
      (cp >= 0xfe10 && cp <= 0xfe6f) || // Vertical / CJK Compatibility Forms
      (cp >= 0xff01 && cp <= 0xff60) || // Fullwidth Latin & punctuation
      (cp >= 0xffe0 && cp <= 0xffe6) || // Fullwidth signs
      (cp >= 0x1b000 && cp <= 0x1b0ff) || // Kana Supplement
      (cp >= 0x1f004 && cp <= 0x1f251) || // Enclosed CJK / Mahjong / Playing-card
      (cp >= 0x1f300 && cp <= 0x1f9ff) || // Misc symbols, emoticons, transport…
      (cp >= 0x1fa00 && cp <= 0x1faff) || // Chess, medical, newer emoji
      (cp >= 0x20000 && cp <= 0x2fffd) || // CJK Extension B–F
      (cp >= 0x30000 && cp <= 0x3fffd) // CJK Extension G+
    )
      return 2;
    return 1;
  }

  // Strip ANSI CSI sequences then sum column widths of each code point.
  // Uses _charWidth so CJK / fullwidth / emoji all count correctly.
  // eslint-disable-next-line no-control-regex
  private static _visibleLen(s: string): number {
    const stripped = s.replace(/\u001b\[[0-9;]*[A-Za-z]/g, "");
    let w = 0;
    for (const ch of stripped) w += Terminal._charWidth(ch.codePointAt(0) ?? 0);
    return w;
  }

  // Truncate a plain (ANSI-stripped) string to at most `cols` terminal columns.
  private static _clipToWidth(s: string, cols: number): string {
    let w = 0;
    let result = "";
    for (const ch of s) {
      const cw = Terminal._charWidth(ch.codePointAt(0) ?? 0);
      if (w + cw > cols) break;
      result += ch;
      w += cw;
    }
    return result;
  }

  private _render(): void {
    if (this.suppress) return;
    const frames = this.noColor ? FRAMES_PLAIN : FRAMES;
    const frame = frames[this._frame % frames.length];
    const elapsed = ((Date.now() - this._startMs) / 1000).toFixed(1);
    const line = this.noColor
      ? `${frame} ${this._label} (${elapsed}s)`
      : `${DIM}${frame} ${this._label} (${elapsed}s)${RST}`;
    const cols = process.stdout.columns ?? 80;
    const newLines = Math.max(1, Math.ceil(Terminal._visibleLen(line) / cols));
    this._sync(() => {
      if (this._renderedLines > 1) process.stdout.write(`\x1b[${this._renderedLines - 1}A`);
      process.stdout.write(`\r\x1b[J${line}`);
    });
    this._renderedLines = newLines;
  }

  private _erase(): void {
    this._sync(() => {
      if (this._renderedLines > 1) process.stdout.write(`\x1b[${this._renderedLines - 1}A`);
      process.stdout.write("\r\x1b[J");
    });
    this._renderedLines = 1;
  }

  // Synchronized output mode — batches writes into a single terminal repaint.
  // Prevents partial-frame flicker on fast-scrolling or high-latency terminals.
  // Gracefully ignored by terminals that don't support DEC mode 2026.
  private _sync(fn: () => void): void {
    this._io.sync(fn);
  }
}
