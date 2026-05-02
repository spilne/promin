// ---------------------------------------------------------------------------
// `runCursorSession` — one prompt, one child-process lifecycle.
//
// Spawns the Cursor agent CLI with `--print --output-format stream-json
// --stream-partial-output --force --trust`, pipes stdout through the
// NDJSON line parser, and exposes:
//
//   - an async iterator of typed `CursorEvent`s (text deltas, tool call
//     transitions, the final result, system metadata, errors)
//   - a promise that resolves with the captured final result when the
//     child exits cleanly
//
// Used by both surfaces:
//   - `cursor-coding-tool.ts` consumes the events to build a structured
//     tool result
//   - `cursor-agent.ts` translates them to `AgentEvent`s and exposes
//     them on `AgentRunOutput.fullStream`
//
// The transport (`CursorTransport`) is injectable so tests pass a fake
// child without touching `child_process`. Default uses `Bun.spawn`.
// ---------------------------------------------------------------------------

import type {
  CursorAssistantFrame,
  CursorFrame,
  CursorResultFrame,
  CursorSystemInitFrame,
  CursorToolCallFrame,
} from "./frames.ts";
import { assistantFrameText } from "./frames.ts";
import { NdJsonLineParser } from "./parse.ts";

/** What we pass to the CLI to start one prompt turn. */
export interface CursorSessionRequest {
  readonly prompt: string;
  /** Override the binary name. Default: "agent". */
  readonly command?: string;
  readonly model?: string;
  /** `--workspace` — Cursor's cwd-equivalent. */
  readonly workspace?: string;
  /** `--worktree` — fresh git worktree per session. */
  readonly worktree?: boolean;
  /** `--trust` — skip Cursor's first-run trust prompt. Default: true. */
  readonly trust?: boolean;
  /** `--sandbox enabled|disabled`. Default: "enabled". */
  readonly sandbox?: "enabled" | "disabled";
  /** `--resume <session_id>` — continue an existing Cursor session. */
  readonly resumeSessionId?: string;
  /** Extra raw args appended verbatim. Use for flags we haven't typed yet. */
  readonly extraArgs?: ReadonlyArray<string>;
  /** Env vars to merge over `process.env`. `CURSOR_API_KEY` lives here. */
  readonly env?: Readonly<Record<string, string | undefined>>;
  /** Abort the underlying child. */
  readonly signal?: AbortSignal;
}

/** Per-event normalized over the NDJSON frames. */
export type CursorEvent =
  | { readonly type: "session-init"; readonly sessionId: string; readonly model?: string }
  | { readonly type: "text-delta"; readonly delta: string }
  | {
      readonly type: "tool-call-start";
      readonly callId: string;
      readonly call: CursorToolCallFrame;
    }
  | { readonly type: "tool-call-end"; readonly callId: string; readonly call: CursorToolCallFrame }
  | { readonly type: "result"; readonly result: CursorResultFrame }
  | { readonly type: "stderr"; readonly text: string }
  | { readonly type: "frame"; readonly frame: CursorFrame };

/** Final accumulated outcome — same data the tool returns at the end. */
export interface CursorSessionResult {
  /** Cursor's session_id from the `system init` frame, or null if it never arrived. */
  readonly sessionId: string | null;
  /**
   * Final answer text. We prefer the `result.result` field (Cursor's
   * authoritative answer) and fall back to the concatenation of
   * assistant deltas if `result` was absent.
   */
  readonly text: string;
  /** True when `result.is_error` was set OR the child exited non-zero. */
  readonly isError: boolean;
  /** Concatenated stderr output. Cursor doesn't emit a structured error frame. */
  readonly stderr: string;
  /** Tool calls observed, paired by call_id. */
  readonly toolCalls: ReadonlyArray<{
    readonly callId: string;
    readonly started: CursorToolCallFrame;
    readonly completed?: CursorToolCallFrame;
  }>;
  /** Process exit code. -1 if killed by signal. */
  readonly exitCode: number;
}

/**
 * Pluggable child-process transport so tests can drive the parser
 * without spawning anything. The default uses `Bun.spawn` and matches
 * the lifecycle of a real Cursor invocation.
 */
export interface CursorTransport {
  spawn(args: ReadonlyArray<string>, opts: TransportSpawnOptions): CursorChild;
}

export interface TransportSpawnOptions {
  readonly env?: Readonly<Record<string, string | undefined>>;
  readonly signal?: AbortSignal;
}

export interface CursorChild {
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr: AsyncIterable<Uint8Array>;
  /** Resolves with the exit code (`-1` for signal kills). */
  readonly exited: Promise<number>;
  kill(): void;
}

/**
 * Default transport — spawns the real Cursor CLI via `Bun.spawn`.
 * Stdout/stderr are returned as readable streams converted to async
 * iterables of Uint8Array. Stdin is closed since `--print` mode reads
 * the prompt from argv.
 *
 * `Bun.spawn` throws synchronously when the binary isn't on PATH (the
 * usual case: Cursor CLI not installed). We catch it and return a
 * CursorChild that emits the failure as a structured stderr line plus
 * exitCode -2 so the rest of the pipeline (events / result / agent
 * stream) handles it the same way as any other "Cursor exited with
 * an error" path — no crashing the request handler.
 */
export const defaultCursorTransport: CursorTransport = {
  spawn(args, opts) {
    let proc: ReturnType<typeof Bun.spawn>;
    try {
      proc = Bun.spawn({
        cmd: [...args],
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
        env: {
          ...(process.env as Record<string, string>),
          ...(opts.env as Record<string, string>),
        },
      });
    } catch (err) {
      const cmd = args[0] ?? "agent";
      const message =
        (err as { code?: string }).code === "ENOENT"
          ? `Cursor CLI not found on PATH (looked for "${cmd}"). Install with:\n  curl https://cursor.com/install -fsS | bash\nThen ensure ~/.local/bin (or wherever the installer placed it) is on PATH.`
          : `Failed to spawn Cursor CLI ("${cmd}"): ${(err as Error).message ?? String(err)}`;
      return spawnFailureChild(message);
    }
    if (opts.signal) {
      const onAbort = () => {
        try {
          proc.kill();
        } catch {
          // Already exited.
        }
      };
      if (opts.signal.aborted) onAbort();
      else opts.signal.addEventListener("abort", onAbort, { once: true });
    }
    return {
      stdout: streamToAsyncIterable(proc.stdout as ReadableStream<Uint8Array>),
      stderr: streamToAsyncIterable(proc.stderr as ReadableStream<Uint8Array>),
      exited: proc.exited.then((code) => (typeof code === "number" ? code : -1)),
      kill: () => {
        try {
          proc.kill();
        } catch {
          // Already exited — fine.
        }
      },
    };
  },
};

/**
 * Build a CursorChild that emits one stderr line, no stdout, and exits
 * with `-2` (our sentinel for "spawn failed before the binary even
 * ran"). Lets the session pipeline treat spawn failure as the same
 * kind of error path as a non-zero CLI exit.
 */
function spawnFailureChild(message: string): CursorChild {
  const enc = new TextEncoder();
  async function* stdout() {
    // No bytes — the parser flushes nothing.
  }
  async function* stderr() {
    yield enc.encode(`${message}\n`);
  }
  return {
    stdout: stdout(),
    stderr: stderr(),
    exited: Promise.resolve(-2),
    kill: () => {},
  };
}

async function* streamToAsyncIterable(
  stream: ReadableStream<Uint8Array>,
): AsyncIterable<Uint8Array> {
  const reader = stream.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      if (value) yield value;
    }
  } finally {
    reader.releaseLock();
  }
}

/**
 * Build the argv for one Cursor session. Pure — no side effects, easy
 * to unit-test that the right flags get passed for a given config.
 */
export function buildCursorArgs(req: CursorSessionRequest): string[] {
  const args: string[] = [
    req.command ?? "agent",
    "-p",
    "--output-format",
    "stream-json",
    "--stream-partial-output",
    "--force",
  ];
  if (req.trust !== false) args.push("--trust");
  if (req.sandbox) args.push("--sandbox", req.sandbox);
  if (req.workspace) args.push("--workspace", req.workspace);
  if (req.worktree) args.push("--worktree");
  if (req.model) args.push("--model", req.model);
  if (req.resumeSessionId) args.push("--resume", req.resumeSessionId);
  if (req.extraArgs && req.extraArgs.length > 0) args.push(...req.extraArgs);
  // Prompt is a positional argument; Cursor reads it as `[prompt]`.
  args.push(req.prompt);
  return args;
}

/**
 * Drive one Cursor prompt turn end-to-end.
 *
 * Returns:
 *   - `events` — async iterable of `CursorEvent`s; consume with `for await`
 *   - `result` — promise of the final `CursorSessionResult` (resolves
 *     when the child process exits, AFTER `events` has been fully drained)
 */
export function runCursorSession(
  req: CursorSessionRequest,
  transport: CursorTransport = defaultCursorTransport,
): { events: AsyncIterable<CursorEvent>; result: Promise<CursorSessionResult> } {
  const args = buildCursorArgs(req);
  const child = transport.spawn(args, {
    ...(req.env !== undefined && { env: req.env }),
    ...(req.signal !== undefined && { signal: req.signal }),
  });

  const stderrChunks: string[] = [];
  const toolCalls = new Map<
    string,
    { started: CursorToolCallFrame; completed?: CursorToolCallFrame }
  >();
  let sessionId: string | null = null;
  let resultFrame: CursorResultFrame | null = null;
  // Cursor's assistant frames carry CUMULATIVE text — frame N starts
  // with frame N-1's text plus new tokens (verified against
  // roshan-c/cursor-acp's prefix-slicing implementation). We track the
  // last seen cumulative string and emit only the new suffix as the
  // delta.
  let accumulatedText = "";

  // ---- single-producer drain ------------------------------------------------
  // We drain stdout / stderr / exit in ONE background task and push
  // CursorEvents into a queue. Consumers (the events iterator + the
  // result promise) read from the queue or wait on `done`. This avoids
  // the race where `result` resolves on child.exited before the events
  // iterator has finished mutating shared state.
  const queue: CursorEvent[] = [];
  const waiters: Array<(ev: CursorEvent | null) => void> = [];
  let drainDone = false;
  let drainErr: unknown = null;
  const drainComplete = (async () => {
    const parser = new NdJsonLineParser();
    const decErr = new TextDecoder();
    const stderrTask = (async () => {
      for await (const chunk of child.stderr) {
        const text = decErr.decode(chunk, { stream: true });
        stderrChunks.push(text);
        emit({ type: "stderr", text });
      }
    })();
    try {
      for await (const chunk of child.stdout) {
        parser.push(chunk);
        for (const frame of parser.take()) {
          for (const ev of normalizeFrame(frame)) emit(ev);
        }
      }
      parser.flush();
      for (const frame of parser.take()) {
        for (const ev of normalizeFrame(frame)) emit(ev);
      }
      await stderrTask;
      await child.exited;
    } catch (err) {
      drainErr = err;
    } finally {
      drainDone = true;
      while (waiters.length > 0) waiters.shift()!(null);
    }
  })();

  function emit(ev: CursorEvent): void {
    if (waiters.length > 0) waiters.shift()!(ev);
    else queue.push(ev);
  }

  function makeReader(): () => Promise<CursorEvent | null> {
    let cursor = 0;
    return () => {
      if (cursor < queue.length) {
        const ev = queue[cursor++];
        return Promise.resolve(ev ?? null);
      }
      if (drainDone) {
        if (drainErr) return Promise.reject(drainErr);
        return Promise.resolve(null);
      }
      return new Promise<CursorEvent | null>((resolve) => waiters.push(resolve));
    };
  }

  async function* iterEvents(): AsyncIterable<CursorEvent> {
    const next = makeReader();
    while (true) {
      const ev = await next();
      if (ev === null) return;
      yield ev;
    }
  }

  function* normalizeFrame(frame: CursorFrame): Iterable<CursorEvent> {
    yield { type: "frame", frame };
    // Cursor stamps every frame with `session_id` once a session is
    // established — capture greedily so multi-turn callers can `--resume`
    // even on shells where the `system init` frame arrives interleaved.
    captureSessionId(frame);
    switch (frame.type) {
      case "system": {
        const init = frame as CursorSystemInitFrame;
        const ev: CursorEvent = {
          type: "session-init",
          sessionId: init.session_id,
          ...(init.model !== undefined && { model: init.model }),
        };
        yield ev;
        return;
      }
      case "assistant": {
        const a = frame as CursorAssistantFrame;
        const cumulative = assistantFrameText(a);
        if (cumulative.length === 0) return;
        // Prefix-slice: each frame's text is the entire answer so far.
        // The new delta is whatever extends beyond what we've seen.
        let delta: string;
        if (cumulative.startsWith(accumulatedText)) {
          delta = cumulative.slice(accumulatedText.length);
        } else {
          // The new text doesn't extend the prior cumulative — Cursor
          // restarted (rare; happens after a tool call). Treat the
          // whole new text as a delta and reset the cursor.
          delta = cumulative;
        }
        accumulatedText = cumulative;
        if (delta.length > 0) yield { type: "text-delta", delta };
        return;
      }
      case "tool_call": {
        const t = frame as CursorToolCallFrame;
        if (t.subtype === "started") {
          toolCalls.set(t.call_id, { started: t });
          yield { type: "tool-call-start", callId: t.call_id, call: t };
        } else if (t.subtype === "completed") {
          const existing = toolCalls.get(t.call_id);
          if (existing) existing.completed = t;
          else toolCalls.set(t.call_id, { started: t, completed: t });
          yield { type: "tool-call-end", callId: t.call_id, call: t };
        }
        return;
      }
      case "result": {
        resultFrame = frame as CursorResultFrame;
        yield { type: "result", result: resultFrame };
        return;
      }
      default:
        return;
    }
  }

  function captureSessionId(frame: CursorFrame): void {
    if (sessionId !== null) return;
    const candidate = (frame as { session_id?: unknown }).session_id;
    if (typeof candidate === "string" && candidate.length > 0) {
      sessionId = candidate;
    }
  }

  const result: Promise<CursorSessionResult> = drainComplete.then(async () => {
    const exitCode = await child.exited;
    const stderrText = stderrChunks.join("");
    // Prefer Cursor's authoritative `result.result` text. Fall back to
    // the cumulative assistant text when the run completed without a
    // result frame. As a last resort — when the run errored before
    // producing any text, like a spawn failure — surface stderr so the
    // caller actually sees what went wrong instead of an empty answer.
    const isError = (resultFrame?.is_error ?? false) || exitCode !== 0;
    let text: string;
    if (resultFrame?.result !== undefined && resultFrame.result.length > 0) {
      text = resultFrame.result;
    } else if (accumulatedText.length > 0) {
      text = accumulatedText;
    } else if (isError && stderrText.length > 0) {
      text = stderrText.trim();
    } else {
      text = "";
    }
    return {
      sessionId,
      text,
      isError,
      stderr: stderrText,
      toolCalls: Array.from(toolCalls.entries()).map(([callId, v]) => {
        const out: {
          callId: string;
          started: CursorToolCallFrame;
          completed?: CursorToolCallFrame;
        } = { callId, started: v.started };
        if (v.completed !== undefined) out.completed = v.completed;
        return out;
      }),
      exitCode,
    };
  });

  return { events: iterEvents(), result };
}
