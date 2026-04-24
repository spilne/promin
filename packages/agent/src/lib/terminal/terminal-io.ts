/**
 * Low-level terminal I/O layer.
 *
 * Handles raw stdout writes, synchronized output mode (DEC mode 2026),
 * bracketed paste filtering, and exclusive-listener single-key reading.
 * Terminal (the coordinator) delegates all side-effectful I/O here.
 */

import type { Interface } from "node:readline";

export class TerminalIO {
  constructor(rl: Interface) {
    // rl is accepted so callers can pass an Interface without casting — unused beyond that.
    void rl;
    if (process.stdin.isTTY) {
      process.stdout.write("\x1b[?2004h"); // bracketed paste on
      this._initBracketedPaste();
    }
  }

  get columns(): number {
    return process.stdout.columns ?? 80;
  }
  get rows(): number {
    return process.stdout.rows ?? 24;
  }

  /**
   * Wrap writes in DEC synchronized output mode (2026) to prevent mid-frame
   * flicker. Gracefully ignored by terminals that don't support it.
   */
  sync(fn: () => void): void {
    process.stdout.write("\x1b[?2026h");
    fn();
    process.stdout.write("\x1b[?2026l");
  }

  /** Disable bracketed paste. Call on process exit. */
  dispose(): void {
    if (process.stdout.isTTY) process.stdout.write("\x1b[?2004l");
  }

  /**
   * Read a single raw keypress and return it as a logical key string.
   * Briefly removes all stdin listeners so readline does not also consume the key.
   */
  readKey(): Promise<string> {
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
        else if (s === "\x1b[5~" || s === "\x1b[I") resolve("pageup");
        else if (s === "\x1b[6~" || s === "\x1b[G") resolve("pagedown");
        else if (s === "\x1b[H" || s === "\x1b[1~" || s === "\x1bOH") resolve("home");
        else if (s === "\x1b[F" || s === "\x1b[4~" || s === "\x1bOF") resolve("end");
        else resolve(s);
      };
      process.stdin.on("data", onData);
    });
  }

  /**
   * Read and discard a single keypress. Ctrl+C exits the process.
   * Briefly removes all stdin listeners so readline does not also consume the key.
   */
  waitForKey(): Promise<void> {
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

  // Intercept stdin data before readline sees it to filter bracketed paste sequences.
  //
  // Without this, raw mode makes any \r or \n in clipboard content indistinguishable
  // from Enter, triggering premature readline submission. With it, paste content is
  // wrapped in \x1b[200~...\x1b[201~ and we replace interior newlines with spaces.
  private _initBracketedPaste(): void {
    const upstream = process.stdin.rawListeners("data") as ((b: Buffer) => void)[];
    for (const l of upstream) process.stdin.removeListener("data", l);

    let inPaste = false;
    process.stdin.on("data", (raw: Buffer) => {
      let s = raw.toString();

      if (s.includes("\x1b[200~")) {
        inPaste = true;
        s = s.replace(/\x1b\[200~/g, "");
      }
      // Replace newlines before checking for the end marker so that start + end
      // in the same data chunk (short pastes) still gets newlines collapsed.
      if (inPaste) s = s.replace(/[\r\n]/g, " ");
      if (s.includes("\x1b[201~")) {
        inPaste = false;
        s = s.replace(/\x1b\[201~/g, "");
      }

      // Shift+Enter — various terminal encodings.
      // Convert to backslash + Enter so readline's \ continuation mode handles it.
      s = s.replace(/\x1b\[13~|\x1b\[27;2;13~|\x1b\[13;2u/g, "\\\r");

      if (!s) return;
      const buf = Buffer.from(s);
      for (const l of upstream) l(buf);
    });
  }
}
