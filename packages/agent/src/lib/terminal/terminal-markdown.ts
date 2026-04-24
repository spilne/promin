/**
 * Streaming markdown → ANSI renderer for terminal agent output.
 *
 * Design: push() accepts arbitrary streaming chunks and returns ANSI-styled
 * text for all newly-complete lines. flush() drains any remaining partial line
 * at end of stream. Code-fence state is preserved across push() calls so
 * fences that span chunk boundaries are handled correctly.
 *
 * Supported syntax:
 *   Block:   # h1  ## h2  ### h3  > blockquote  - / * unordered  1. ordered
 *   Fence:   ```lang … ``` (nested not supported) — syntax-highlighted via cli-highlight
 *   Inline:  **bold**  *italic*  `code`  [text](url)  https://auto-link
 *   OSC 8:   markdown links, auto-links, and ./relative /absolute file paths
 */

import { join } from "node:path";
import { highlight, supportsLanguage } from "cli-highlight";

const BOLD = "\x1b[1m";
const ITAL = "\x1b[3m";
const DIM = "\x1b[2m";
const RST = "\x1b[0m";
const CLAY = "\x1b[38;2;217;119;87m";

/**
 * Emit an OSC 8 hyperlink (clickable in iTerm2, WezTerm, Kitty, Ghostty, and
 * recent versions of Terminal.app and GNOME Terminal).
 * In unsupported terminals the text appears normally with no visual artifacts.
 */
export function osc8(url: string, text: string): string {
  return `\x1b]8;;${url}\x07${text}\x1b]8;;\x07`;
}

export interface MarkdownRendererConfig {
  /** Terminal column width — used for code-fence border length. Default: 80. */
  width?: number;
  /**
   * Workspace root directory. When set, relative file paths in agent output
   * (e.g. `./src/foo.ts`, `packages/bar/index.ts`) are wrapped in OSC 8
   * `file://` hyperlinks so Cmd+click opens them in the editor.
   */
  workspace?: string;
}

export class MarkdownRenderer {
  private _state: "normal" | "code" = "normal";
  private _codeLang = "";
  private _buf = "";
  private readonly _width: number;
  private readonly _workspace: string | undefined;

  constructor(config: MarkdownRendererConfig = {}) {
    this._width = config.width ?? 80;
    this._workspace = config.workspace;
  }

  /**
   * Feed a streaming chunk. Returns ANSI-rendered text for every complete line
   * (i.e. lines ending with \n) seen so far. Partial lines are buffered.
   */
  push(chunk: string): string {
    this._buf += chunk;
    const lines = this._buf.split("\n");
    this._buf = lines.pop() ?? ""; // last element: partial line or ""
    return lines.map((l) => this._line(l) + "\n").join("");
  }

  /**
   * Drain any buffered partial line. Call once at end of stream.
   * Also closes any unclosed code fence so the output is always well-formed.
   */
  flush(): string {
    let out = this._buf ? this._line(this._buf) : "";
    this._buf = "";
    if (this._state === "code") {
      this._state = "normal";
      const closer = DIM + "─".repeat(Math.max(0, this._width)) + RST;
      out = out ? `${out}\n${closer}` : closer;
    }
    return out;
  }

  private _line(raw: string): string {
    // Code fence detection (``` or ~~~)
    const trimmed = raw.trimStart();
    if (trimmed.startsWith("```") || trimmed.startsWith("~~~")) {
      if (this._state === "code") {
        this._state = "normal";
        return DIM + "─".repeat(Math.max(0, this._width)) + RST;
      }
      this._codeLang = trimmed.slice(3).trim().toLowerCase();
      this._state = "code";
      const label = this._codeLang ? ` ${this._codeLang} ` : "";
      const fill = "─".repeat(Math.max(0, this._width - 2 - label.length));
      return `${DIM}──${label}${fill}${RST}`;
    }

    if (this._state === "code") {
      return `  ${this._highlightLine(raw)}`;
    }

    return this._block(raw);
  }

  private _highlightLine(raw: string): string {
    if (!this._codeLang || !raw.trim()) return raw;
    try {
      if (!supportsLanguage(this._codeLang)) return raw;
      return highlight(raw, { language: this._codeLang, ignoreIllegals: true });
    } catch {
      return raw;
    }
  }

  private _block(line: string): string {
    if (line.startsWith("### ")) return `${DIM}${BOLD}${this._inline(line.slice(4))}${RST}`;
    if (line.startsWith("## ")) return `${BOLD}${this._inline(line.slice(3))}${RST}`;
    if (line.startsWith("# ")) return `${CLAY}${BOLD}${this._inline(line.slice(2))}${RST}`;
    if (line.startsWith("> ")) return `${DIM}│${RST} ${this._inline(line.slice(2))}`;

    const ul = line.match(/^(\s*)[*\-] (.*)/);
    if (ul) return `${ul[1]}  ${DIM}•${RST} ${this._inline(ul[2])}`;

    const ol = line.match(/^(\s*)(\d+)\. (.*)/);
    if (ol) return `${ol[1]}  ${ol[2]}. ${this._inline(ol[3])}`;

    return this._inline(line);
  }

  private _inline(s: string): string {
    return (
      s
        // Bold: **text** — process before italic to avoid ** conflict
        .replace(/\*\*([^*\n]+?)\*\*/g, `${BOLD}$1${RST}`)
        // Italic: *text* — negative lookbehind prevents matching **
        .replace(/(?<!\*)\*([^*\n]+?)\*(?!\*)/g, `${ITAL}$1${RST}`)
        // Inline code — dim backtick style
        .replace(/`([^`\n]+)`/g, `${DIM}\`$1\`${RST}`)
        // Markdown links [text](url) — must run before auto-link so the URL
        // inside the generated OSC 8 sequence is not re-matched below.
        .replace(/\[([^\]\n]+)\]\(([^)\n]+)\)/g, (_, text, url) => osc8(url, text))
        // Auto-links https?:// — (?<!;;) prevents re-matching URLs already
        // embedded in OSC 8 sequences produced by the markdown link step above.
        .replace(/(?<!;;)\bhttps?:\/\/[^\s<>"'`,;()[\]{}]+/g, (url) => osc8(url, url))
        // File paths: ./relative, ../up — ending in a known extension.
        // (?<![`\w/]) prevents matching mid-path; returns plain text when no workspace.
        .replace(
          /(?<![`\w/])((?:\.\.?\/)[^\s"'`,;:!?()[\]{}]+\.(?:ts|js|tsx|jsx|mts|mjs|cjs|json|md|py|go|rs|css|html|sh|yaml|yml|toml|lock))(?![`\w])/g,
          (path) => {
            if (!this._workspace) return path;
            const abs = join(this._workspace, path);
            return osc8(`file://${abs}`, path);
          },
        )
        // Absolute paths /foo/bar.ts — (?<![`\w.;:/]) prevents matching paths
        // already inside file:/// sequences (// and / are preceded by / or :)
        // or after a dot (./…) or semicolon (;;…).
        .replace(
          /(?<![`\w.;:/])(\/[^\s"'`,;:!?()[\]{}]+\.(?:ts|js|tsx|jsx|mts|mjs|cjs|json|md|py|go|rs|css|html|sh|yaml|yml|toml))(?![`\w])/g,
          (path) => osc8(`file://${path}`, path),
        )
    );
  }
}
