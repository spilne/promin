/**
 * Terminal renderer for console agent examples.
 *
 * Features:
 *   - Animated braille spinner with elapsed time (Claude Code style)
 *   - Synchronized output mode (\x1b[?2026h) to prevent flicker
 *   - printAbove() — interrupt-safe output that restores the readline prompt
 *   - showPane() — transient command overlay that erases on dismiss (any key)
 *   - Clay-colored ❯ prompt matching Anthropic's brand color
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

export interface TreeNode {
  label: string;
  children: TreeNode[];
  expanded: boolean;
}

const FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
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

export class Terminal {
  /** True while readline.question() is waiting for input. */
  inPrompt = false;

  /**
   * Suppress spinner renders — set during background scheduler turns so their
   * tool activity doesn't corrupt the user-facing status line.
   */
  suppress = false;

  /** True when the agent streamed text on the current line without a trailing \n. */
  agentHasTextOnLine = false;

  private readonly _rl: Interface;
  private _timer: ReturnType<typeof setInterval> | null = null;
  private _frame = 0;
  private _label = "";
  private _startMs = 0;
  private _promptTimer: ReturnType<typeof setInterval> | null = null;
  private _promptFrame = 0;
  private _chunkBuf = "";
  private _chunkFlush: ReturnType<typeof setImmediate> | null = null;

  constructor(rl: Interface) {
    this._rl = rl;
    this._initBracketedPaste();
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
    const cols = process.stdout.columns ?? 80;
    const rows = process.stdout.rows ?? 24;
    const hr = `${DIM}${"─".repeat(cols)}${RST}`;

    const out = [
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

    for (const line of out) process.stdout.write(`${line}\n`);

    await this._waitForKey();

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
    const cols = process.stdout.columns ?? 80;
    const rows = process.stdout.rows ?? 24;
    const hr = `${DIM}${"─".repeat(cols)}${RST}`;
    // 4 header lines: "", hr, title, hr
    // 4 footer lines: hr, help, hr, ""
    const VIEW_H = Math.max(1, rows - 8);

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

    // Strip ANSI codes to get visible length; truncate long lines naively.
    const clip = (s: string, w: number): string => {
      const plain = s.replace(/\x1b\[[^m]*m/g, "");
      return plain.length > w ? `${plain.slice(0, w - 1)}…` : s;
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
      out.push(hr, `  ${DIM}↑↓ navigate  space/→ expand  ← collapse  q close${note}${RST}`, hr, "");

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
      }
      render(flat);
    }

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

  /** Release all timers and restore terminal state. Call on process exit. */
  close(): void {
    this.stopSpinner();
    this.stopPromptAnimation();
    this.flushChunks();
    if (process.stdout.isTTY) process.stdout.write("\x1b[?2004l"); // disable bracketed paste
  }

  // Enable bracketed paste mode so pasted content doesn't trigger premature readline submission.
  //
  // Without this, raw mode (which readline uses on TTYs) makes any \r or \n in the clipboard —
  // including a common trailing newline — indistinguishable from Enter.
  //
  // With bracketed paste enabled, the terminal wraps paste with \x1b[200~ ... \x1b[201~.
  // We intercept stdin, strip the markers, and replace newlines inside the paste with spaces
  // before readline sees the data. The filter is installed as the sole 'data' listener; the
  // original readline listener is captured and called by the filter, so _waitForKey / _readKey
  // (which temporarily swap all listeners) continue to work correctly.
  private _initBracketedPaste(): void {
    if (!process.stdin.isTTY) return;
    process.stdout.write("\x1b[?2004h");

    const upstream = process.stdin.rawListeners("data") as ((b: Buffer) => void)[];
    for (const l of upstream) process.stdin.removeListener("data", l);

    let inPaste = false;
    process.stdin.on("data", (raw: Buffer) => {
      let s = raw.toString();

      if (s.includes("\x1b[200~")) {
        inPaste = true;
        s = s.replace(/\x1b\[200~/g, "");
      }
      if (s.includes("\x1b[201~")) {
        inPaste = false;
        s = s.replace(/\x1b\[201~/g, "");
      }
      if (inPaste) s = s.replace(/[\r\n]/g, " ");

      if (!s) return;
      const buf = Buffer.from(s);
      for (const l of upstream) l(buf);
    });
  }

  // Read a single raw keypress without letting readline also process it.
  //
  // Why not rl.pause() + setRawMode: rl.pause() pauses the stdin stream, but
  // when we resume it readline's own data listener is still attached and receives
  // the keypress, corrupting its line buffer and causing the next prompt to erase
  // typed text as the animation redraws stale rl.line content.
  //
  // Instead: briefly remove all existing stdin data listeners (readline's included),
  // install ours, then restore them after the keypress. readline already called
  // setRawMode(true) during construction so individual keypresses arrive without Enter.
  // Ctrl+C (\x03) exits instead of dismissing.
  private _waitForKey(): Promise<void> {
    return new Promise<void>((resolve) => {
      const saved = process.stdin.rawListeners("data") as ((...args: unknown[]) => void)[];
      for (const l of saved) process.stdin.removeListener("data", l);

      const onData = (key: Buffer) => {
        process.stdin.removeListener("data", onData);
        for (const l of saved) process.stdin.on("data", l);
        if (key[0] === 3) {
          process.stdout.write("\n");
          process.exit(0);
        }
        resolve();
      };
      process.stdin.on("data", onData);
    });
  }

  // Read a single raw keypress and return it as a logical key string.
  // Uses the same exclusive stdin listener pattern as _waitForKey().
  private _readKey(): Promise<string> {
    return new Promise<string>((resolve) => {
      const saved = process.stdin.rawListeners("data") as ((...args: unknown[]) => void)[];
      for (const l of saved) process.stdin.removeListener("data", l);
      const onData = (buf: Buffer) => {
        process.stdin.removeListener("data", onData);
        for (const l of saved) process.stdin.on("data", l);
        const s = buf.toString();
        if (s === "\x1b[A") resolve("up");
        else if (s === "\x1b[B") resolve("down");
        else if (s === "\x1b[C") resolve("right");
        else if (s === "\x1b[D") resolve("left");
        else resolve(s);
      };
      process.stdin.on("data", onData);
    });
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

  private _render(): void {
    if (this.suppress) return;
    const frame = FRAMES[this._frame % FRAMES.length];
    const elapsed = ((Date.now() - this._startMs) / 1000).toFixed(1);
    this._sync(() => {
      process.stdout.write(`\r\x1b[K${DIM}${frame} ${this._label} (${elapsed}s)${RST}`);
    });
  }

  private _erase(): void {
    this._sync(() => process.stdout.write("\r\x1b[K"));
  }

  // Synchronized output mode — batches writes into a single terminal repaint.
  // Prevents partial-frame flicker on fast-scrolling or high-latency terminals.
  // Gracefully ignored by terminals that don't support DEC mode 2026.
  private _sync(fn: () => void): void {
    process.stdout.write("\x1b[?2026h");
    fn();
    process.stdout.write("\x1b[?2026l");
  }
}
