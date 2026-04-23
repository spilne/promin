/**
 * Shared REPL infrastructure for console agent examples.
 *
 * Extracts the three pieces that are identical across console-chat and town-chat:
 *   - abortable()       — wraps an AsyncIterable so it stops on AbortSignal
 *   - ConsoleRunner     — manages AbortController state, streaming turns, SIGINT
 */

import type { Interface } from "node:readline";
import { MarkdownRenderer } from "./terminal-markdown.ts";
import type { Terminal } from "./terminal.ts";
import type { UsageTracker } from "../console-usage.ts";

// ---- abortable ----

export async function* abortable(
  source: AsyncIterable<string>,
  signal: AbortSignal,
): AsyncGenerator<string> {
  if (signal.aborted) return;
  const iter = source[Symbol.asyncIterator]();
  const abortPromise = new Promise<void>((r) =>
    signal.addEventListener("abort", () => r(), { once: true }),
  );
  while (true) {
    let aborted = false;
    const result = await Promise.race([
      iter.next(),
      abortPromise.then(() => {
        aborted = true;
        return { done: true as const, value: "" };
      }),
    ]);
    if (aborted || result.done) break;
    yield result.value;
  }
}

// ---- ConsoleRunner ----

export interface RunTurnOptions {
  /** Spinner label shown while waiting for the first token. Default: "thinking..." */
  initialSpinner?: string;
  /** Label printed before the first output chunk, e.g. "Agent" or "Director". Default: "Agent" */
  label?: string;
  /** Workspace path passed to MarkdownRenderer for relative-file OSC-8 links. */
  workspace?: string;
  /**
   * Max additional retries when the stream throws and `retryOn` returns true.
   * Default: 0 (no retries).
   */
  maxRetries?: number;
  /** Predicate deciding whether an error is retryable. Default: always false. */
  retryOn?: (err: Error) => boolean;
  /** Spinner label during a retry pause, given the attempt number (1-based). */
  retrySpinner?: (attempt: number) => string;
}

export interface RunTurnResult {
  aborted: boolean;
  error?: Error;
}

export class ConsoleRunner {
  private _currentAc: AbortController | null = null;
  private _lastCtrlC = 0;
  private _savedPlaceholder = "";

  constructor(
    private readonly term: Terminal,
    private readonly usage: UsageTracker,
  ) {}

  get currentAc(): AbortController | null {
    return this._currentAc;
  }

  async runTurn(
    getStream: (signal: AbortSignal) => AsyncIterable<string>,
    opts: RunTurnOptions = {},
  ): Promise<RunTurnResult> {
    const {
      initialSpinner = "thinking...",
      label = "Agent",
      workspace,
      maxRetries = 0,
      retryOn = () => false,
      retrySpinner = (n) => `settling… (${n})`,
    } = opts;

    const ac = new AbortController();
    this._currentAc = ac;
    this.term.agentHasTextOnLine = false;
    this.term.startSpinner(initialSpinner);
    this.usage.resetTurn();

    const md = new MarkdownRenderer({ width: process.stdout.columns ?? 80, workspace });
    let labelShown = false;
    let streamError: Error | undefined;
    let attempts = 0;

    while (true) {
      try {
        for await (const chunk of abortable(getStream(ac.signal), ac.signal)) {
          this.term.stopSpinner();
          if (!labelShown) {
            process.stdout.write(`\n${label}:\n`);
            labelShown = true;
          }
          const rendered = md.push(chunk);
          if (rendered) this.term.writeChunk(rendered);
          this.term.agentHasTextOnLine = true;
        }
        this.term.flushChunks();
        const tail = md.flush();
        if (tail) process.stdout.write(tail);
        streamError = undefined;
        break;
      } catch (err) {
        this.term.flushChunks();
        const e = err instanceof Error ? err : new Error(String(err));
        if (attempts < maxRetries && retryOn(e) && !ac.signal.aborted) {
          attempts++;
          this.term.startSpinner(retrySpinner(attempts));
          await new Promise((r) => setTimeout(r, 500));
          continue;
        }
        streamError = e;
        break;
      }
    }

    this._currentAc = null;
    this.term.stopSpinner();

    return { aborted: ac.signal.aborted, error: streamError };
  }

  /**
   * Wire up the standard Ctrl+C behaviour:
   *   - First Ctrl+C during a turn: abort it (calls onInterrupt if provided).
   *   - First Ctrl+C at idle prompt: print hint.
   *   - Second Ctrl+C within 2 s at idle prompt: call onExit.
   */
  setupSigInt(
    rl: Interface,
    opts: {
      onInterrupt?: () => void;
      /** Called after the "(Ctrl+C again to exit)" hint — typically to reprint the prompt. */
      onIdleHint?: () => void;
      onExit: () => void;
    },
  ): void {
    rl.on("SIGINT", () => {
      if (this._currentAc) {
        this.term.stopSpinner();
        if (this.term.agentHasTextOnLine) process.stdout.write("\n");
        process.stdout.write("\x1b[2m(interrupted)\x1b[0m\n");
        this._currentAc.abort();
        this._currentAc = null;
        opts.onInterrupt?.();
      } else {
        const now = Date.now();
        if (now - this._lastCtrlC < 2_000) {
          process.stdout.write("\n");
          opts.onExit();
        } else {
          this._lastCtrlC = now;
          // biome-ignore lint/suspicious/noExplicitAny: readline internals
          this._savedPlaceholder = (rl as any).line ?? "";
          process.stdout.write("\n\x1b[2m(Ctrl+C again to exit)\x1b[0m\n");
          opts.onIdleHint?.();
        }
      }
    });
  }

  /** Restore a saved prompt placeholder (called after SIGINT reprints the prompt). */
  get savedPlaceholder(): string {
    return this._savedPlaceholder;
  }

  clearSavedPlaceholder(): void {
    this._savedPlaceholder = "";
  }
}
