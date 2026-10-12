// ---------------------------------------------------------------------------
// SSE plumbing for agent turns.
//
// A turn's events don't go straight to the client that started it. A pump
// reads the run's `fullStream` into a `TurnStream`: a bounded, numbered
// buffer of encoded SSE frames. Each client is a cursor into that buffer,
// served by `TurnStreamHub.respond`:
//
//   - Event ids. Every frame carries `id: <turnId>:<seq>`. A thread turn's
//     `turnId` is the turn's workflow id (`threadTurnWorkflowId` of the
//     namespace, thread and turn key), so the id names the engine's turn
//     and the frame's place in it.
//   - Detach, not cancel. A client that goes away only drops its cursor;
//     the pump keeps reading, the turn commits normally and the turn lease
//     is released when the run settles. Only the cancel route (or a
//     one-shot `/stream`, which has nothing to commit) cancels the run.
//   - Reattach. A thread turn's stream stays in the hub while it runs and
//     for `retentionMs` after; a client resumes it with `Last-Event-ID`
//     (or `?turnId=&from=`) and gets the frames after that id, then the
//     live tail.
//   - Backpressure. A response is pulled: frames are enqueued only while
//     the response's queue has room (`desiredSize > 0`), so a slow client
//     never grows a per-client queue. The turn's buffer keeps at most
//     `maxBufferBytes`; a client whose cursor falls behind what the buffer
//     still holds gets `event: error {error: "slow_consumer"}` and is
//     closed (`replay_unavailable` when it asked to resume from there). A
//     one-shot stream closed that way cancels its run, like a disconnect.
//     Kept turns share a byte budget (`maxRetainedBytes`): beyond it the
//     oldest finished turns are dropped first, and a finished turn keeps
//     only its frames, not the run.
//   - Heartbeats. An SSE comment every `heartbeatMs` (on the hub's
//     WallClock) keeps idle proxies from cutting a long tool call.
//
// Limits: the hub lives in this process. A client must reattach to the
// replica that runs the turn, and a restart loses the buffers (the turn
// itself is durable: its committed messages are in the thread). Replay
// from the start of a turn works while the turn's frames fit in
// `maxBufferBytes`; after that, read the thread's messages instead.
// ---------------------------------------------------------------------------

import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  TurnKeyConflictError,
  type AgentRunOutput,
} from "@promin/agent";
import { SystemWallClock, WorkflowSuspendedError, type WallClock } from "@promin/workflow";
import { createHash } from "node:crypto";

const SSE_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  // Disable Nginx / Cloud Run buffering so clients see events as they happen.
  "x-accel-buffering": "no",
} as const;

/** Default bytes of encoded frames one turn keeps. */
export const DEFAULT_TURN_STREAM_BUFFER_BYTES = 4 * 1_048_576;
/** Default time a finished turn's frames stay for reattach. */
export const DEFAULT_TURN_STREAM_RETENTION_MS = 5 * 60_000;
/** Default interval of SSE heartbeat comments. */
export const DEFAULT_SSE_HEARTBEAT_MS = 15_000;
/** Default number of turn streams kept (live ones are never dropped). */
export const DEFAULT_MAX_TURN_STREAMS = 1_000;
/** Default bytes of frames all kept turn streams hold together. */
export const DEFAULT_MAX_RETAINED_BYTES = 64 * 1_048_576;
/** Default frames a response queues before the hub stops writing to it. */
export const DEFAULT_SSE_QUEUE_FRAMES = 64;

export interface TurnStreamLimits {
  /** Bytes of encoded frames one turn keeps for reattach and slow readers. */
  readonly maxBufferBytes?: number;
  /** How long a finished turn's frames stay for reattach, ms. */
  readonly retentionMs?: number;
  /** Interval of SSE heartbeat comments, ms. */
  readonly heartbeatMs?: number;
  /** Turn streams kept; the oldest finished one is dropped beyond it. */
  readonly maxTurns?: number;
  /** Bytes of frames all kept turns hold together; the oldest finished ones are dropped beyond it. */
  readonly maxRetainedBytes?: number;
  /** Frames one response queues before the hub waits for the client to read. */
  readonly queueFrames?: number;
}

/** Whose thread a turn stream belongs to: only that scope may reattach or cancel. */
export interface TurnStreamOwner {
  readonly agentId: string;
  readonly namespaceId: string;
  readonly resourceId: string;
  readonly threadId: string;
}

interface Frame {
  readonly seq: number;
  readonly bytes: Uint8Array;
}

const encoder = new TextEncoder();

function digestOf(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

/** The bounded, numbered frame buffer of one turn. */
export class TurnStream {
  readonly id: string;
  readonly owner: TurnStreamOwner | undefined;
  private readonly frames: Frame[] = [];
  private readonly maxBufferBytes: number;
  private cancelRun: () => Promise<void>;
  private readonly taskDigest: string | undefined;
  private bytes = 0;
  private first = 0;
  private next = 0;
  private ended = false;
  private waiters: Array<() => void> = [];
  /** Resolves when the run has settled and its `onSettled` ran. */
  settled: Promise<void> = Promise.resolve();

  constructor(params: {
    readonly id: string;
    readonly owner?: TurnStreamOwner;
    readonly maxBufferBytes: number;
    readonly cancel: () => Promise<void>;
    /** The turn's task, compared (by digest) when a retry attaches. */
    readonly task?: string;
  }) {
    this.id = params.id;
    this.owner = params.owner;
    this.maxBufferBytes = params.maxBufferBytes;
    this.cancelRun = params.cancel;
    this.taskDigest = params.task !== undefined ? digestOf(params.task) : undefined;
  }

  /** Bytes of frames buffered. */
  get bufferedBytes(): number {
    return this.bytes;
  }

  /** Whether `task` is the task this turn was started with (true when none was recorded). */
  isTask(task: string): boolean {
    return this.taskDigest === undefined || this.taskDigest === digestOf(task);
  }

  /** Drop the reference to the run once it has settled: a kept turn holds only its frames. */
  releaseRun(): void {
    this.cancelRun = async () => {};
  }

  /** Seq of the oldest frame still buffered. */
  get firstSeq(): number {
    return this.first;
  }

  /** Seq the next frame gets. */
  get nextSeq(): number {
    return this.next;
  }

  /** True once the turn's last frame is in. */
  get done(): boolean {
    return this.ended;
  }

  /** Append a frame (`event` null: a plain `data:` frame), dropping the oldest beyond the cap. */
  append(params: { readonly event: string | null; readonly data: unknown }): void {
    if (this.ended) return;
    const seq = this.next++;
    const event = params.event ? `event: ${params.event}\n` : "";
    const bytes = encoder.encode(
      `id: ${this.id}:${seq}\n${event}data: ${JSON.stringify(params.data)}\n\n`,
    );
    this.frames.push({ seq, bytes });
    this.bytes += bytes.byteLength;
    while (this.bytes > this.maxBufferBytes && this.frames.length > 1) {
      const dropped = this.frames.shift()!;
      this.bytes -= dropped.bytes.byteLength;
      this.first = dropped.seq + 1;
    }
    this.wake();
  }

  /** Mark the turn's frames complete. */
  end(): void {
    if (this.ended) return;
    this.ended = true;
    this.wake();
  }

  /** The buffered frame `seq`, if it is still (or already) there. */
  frameAt(seq: number): Frame | undefined {
    if (seq < this.first) return undefined;
    return this.frames[seq - this.first];
  }

  /** Resolves on the next append or end. */
  changed(): Promise<void> {
    if (this.ended) return Promise.resolve();
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  /** Cancel the run (it ends as `cancelled`) and wait until it has stopped. */
  cancel(): Promise<void> {
    return this.cancelRun();
  }

  private wake(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) w();
  }
}

/** `<turnId>:<seq>` of an event id, or null when it isn't one. */
export function parseEventId(raw: string | null): { turnId: string; seq: number } | null {
  if (!raw) return null;
  const at = raw.lastIndexOf(":");
  if (at <= 0) return null;
  const seq = Number(raw.slice(at + 1));
  if (!Number.isSafeInteger(seq) || seq < 0) return null;
  return { turnId: raw.slice(0, at), seq };
}

function sameOwner(a: TurnStreamOwner | undefined, b: TurnStreamOwner): boolean {
  return (
    a !== undefined &&
    a.agentId === b.agentId &&
    a.namespaceId === b.namespaceId &&
    a.resourceId === b.resourceId &&
    a.threadId === b.threadId
  );
}

/**
 * The turn streams of this process: starts the pump of each turn, keeps
 * thread turns' streams for reattach and serves SSE responses from them.
 */
export class TurnStreamHub {
  private readonly clock: WallClock;
  private readonly maxBufferBytes: number;
  private readonly retentionMs: number;
  private readonly heartbeatMs: number;
  private readonly maxTurns: number;
  private readonly maxRetainedBytes: number;
  private readonly queueFrames: number;
  private readonly streams = new Map<string, TurnStream>();
  private readonly evictions = new Map<string, { clear(): void }>();

  constructor(params: { readonly clock?: WallClock; readonly limits?: TurnStreamLimits } = {}) {
    const limits = params.limits ?? {};
    this.clock = params.clock ?? SystemWallClock;
    this.maxBufferBytes = limits.maxBufferBytes ?? DEFAULT_TURN_STREAM_BUFFER_BYTES;
    this.retentionMs = limits.retentionMs ?? DEFAULT_TURN_STREAM_RETENTION_MS;
    this.heartbeatMs = limits.heartbeatMs ?? DEFAULT_SSE_HEARTBEAT_MS;
    this.maxTurns = limits.maxTurns ?? DEFAULT_MAX_TURN_STREAMS;
    this.maxRetainedBytes = limits.maxRetainedBytes ?? DEFAULT_MAX_RETAINED_BYTES;
    this.queueFrames = limits.queueFrames ?? DEFAULT_SSE_QUEUE_FRAMES;
  }

  /**
   * Start pumping `out` into a new turn stream. With an `owner` the stream
   * is kept for reattach (replacing an older stream of the same id);
   * without one it lives only as long as its responses. `onSettled` runs
   * once, when the run has settled, whatever happened to the clients.
   */
  start(params: {
    readonly id: string;
    readonly owner?: TurnStreamOwner;
    readonly out: AgentRunOutput;
    /** Fields every `thread` / `finish` / `suspended` frame carries. */
    readonly meta: Record<string, unknown>;
    /** Open the stream with a `thread` frame carrying `meta`. */
    readonly announce: boolean;
    /** The turn's task, so a retry that attaches can be checked against it. */
    readonly task?: string;
    readonly onSettled?: () => Promise<void>;
  }): TurnStream {
    const { id, owner, out } = params;
    silenceOutput(out);
    const stream = new TurnStream({
      id,
      ...(owner && { owner }),
      maxBufferBytes: this.maxBufferBytes,
      cancel: () => out.cancel(),
      ...(params.task !== undefined && { task: params.task }),
    });
    if (owner) this.register(stream);
    const pumped = pumpAgentRun({ stream, out, meta: params.meta, announce: params.announce });
    stream.settled = Promise.allSettled([pumped, out.text]).then(async () => {
      stream.releaseRun();
      await params.onSettled?.().catch(() => {});
      if (owner) {
        this.scheduleEviction(stream);
        this.enforceBudget();
      }
    });
    return stream;
  }

  /** The kept stream `id`, when it belongs to `owner`. */
  find(params: { readonly id: string; readonly owner: TurnStreamOwner }): TurnStream | undefined {
    const stream = this.streams.get(params.id);
    return stream && sameOwner(stream.owner, params.owner) ? stream : undefined;
  }

  /** The kept streams of `owner`'s thread whose turn is still running. */
  running(owner: TurnStreamOwner): TurnStream[] {
    return [...this.streams.values()].filter((s) => !s.done && sameOwner(s.owner, owner));
  }

  /** The most recently started kept stream of `owner`'s thread. */
  latest(owner: TurnStreamOwner): TurnStream | undefined {
    const all = [...this.streams.values()];
    for (let i = all.length - 1; i >= 0; i--) {
      if (sameOwner(all[i]!.owner, owner)) return all[i];
    }
    return undefined;
  }

  /**
   * Serve `stream` from frame `fromSeq` as an SSE response. `onDisconnect`
   * says what a client going away does: `detach` (the run goes on) or
   * `cancel` (the run is cancelled).
   */
  respond(params: {
    readonly stream: TurnStream;
    readonly fromSeq: number;
    readonly onDisconnect: "detach" | "cancel";
  }): Response {
    const { stream, onDisconnect } = params;
    const clock = this.clock;
    let cursor = Math.max(0, params.fromSeq);
    let sent = 0;
    let closed = false;
    let heartbeat: { clear(): void } | undefined;
    const finish = (controller: ReadableStreamDefaultController<Uint8Array>): void => {
      if (closed) return;
      closed = true;
      heartbeat?.clear();
      try {
        controller.close();
      } catch {
        // already closed
      }
    };
    const body = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          const timer = clock.setInterval(() => {
            // A client that isn't reading gets nothing queued, not even this.
            if (closed || (controller.desiredSize ?? 0) <= 0) return;
            try {
              controller.enqueue(encoder.encode(": heartbeat\n\n"));
            } catch {
              closed = true;
              timer.clear();
            }
          }, this.heartbeatMs);
          timer.unref?.();
          heartbeat = timer;
        },
        pull: async (controller) => {
          while (!closed) {
            if (cursor < stream.firstSeq) {
              // The frames this client needs next are gone: it fell behind
              // the buffer (or asked to resume from before it).
              const error = sent === 0 ? "replay_unavailable" : "slow_consumer";
              controller.enqueue(
                encoder.encode(`event: error\ndata: ${JSON.stringify({ error })}\n\n`),
              );
              finish(controller);
              // The server closed this client: the same as it going away.
              if (onDisconnect === "cancel") void stream.cancel().catch(() => {});
              return;
            }
            let wrote = false;
            for (let f = stream.frameAt(cursor); f; f = stream.frameAt(cursor)) {
              controller.enqueue(f.bytes);
              cursor = f.seq + 1;
              sent++;
              wrote = true;
              if ((controller.desiredSize ?? 0) <= 0) break;
            }
            if (wrote) return;
            if (stream.done) {
              finish(controller);
              return;
            }
            await stream.changed();
          }
        },
        cancel: async () => {
          closed = true;
          heartbeat?.clear();
          if (onDisconnect === "cancel") await stream.cancel().catch(() => {});
        },
      },
      { highWaterMark: this.queueFrames },
    );
    return new Response(body, {
      status: 200,
      headers: { ...SSE_HEADERS, "x-turn-id": stream.id },
    });
  }

  private register(stream: TurnStream): void {
    this.evictions.get(stream.id)?.clear();
    this.evictions.delete(stream.id);
    // Re-inserting moves the id to the newest position.
    this.streams.delete(stream.id);
    this.streams.set(stream.id, stream);
    this.enforceBudget();
  }

  /** Drop the oldest finished turns while over the turn count or the byte budget. */
  private enforceBudget(): void {
    let bytes = 0;
    for (const kept of this.streams.values()) bytes += kept.bufferedBytes;
    for (const [id, kept] of this.streams) {
      if (this.streams.size <= this.maxTurns && bytes <= this.maxRetainedBytes) break;
      if (!kept.done) continue;
      bytes -= kept.bufferedBytes;
      this.drop(id);
    }
  }

  private scheduleEviction(stream: TurnStream): void {
    if (this.streams.get(stream.id) !== stream) return;
    const timer = this.clock.setTimeout(() => {
      if (this.streams.get(stream.id) === stream) this.drop(stream.id);
    }, this.retentionMs);
    timer.unref?.();
    this.evictions.set(stream.id, timer);
  }

  private drop(id: string): void {
    this.streams.delete(id);
    this.evictions.get(id)?.clear();
    this.evictions.delete(id);
  }
}

/**
 * Attach a no-op rejection handler to every promise on a run output so
 * a failed run that nobody awaits (the client went away, or the stream
 * reported the failure through `fullStream`) never surfaces as an
 * unhandled rejection.
 */
export function silenceOutput(out: AgentRunOutput): void {
  for (const value of Object.values(out)) {
    if (value instanceof Promise) value.catch(() => {});
  }
}

/** An `event: error` payload: a request id, never the error's text. */
export function streamErrorPayload(err: unknown): { error: string; requestId: string } {
  const requestId = crypto.randomUUID();
  try {
    console.error(`[zorya] agent stream failed requestId=${requestId}`, err);
  } catch {
    // ignore
  }
  return { error: "stream_failed", requestId };
}

/**
 * Read a run's `fullStream` into `stream` as typed SSE frames: text
 * deltas, approval requests, and the run's outcome.
 *
 * Suspension semantics: when the workflow suspends on an approval gate
 * the underlying `out.text` promise rejects with `WorkflowSuspendedError`.
 * The `approval-requested` frame already went out, so the rejection maps
 * to a clean `event: suspended` and the client renders the approval
 * banner. A resumed turn replays its journal without re-emitting that
 * request, so it ends with `event: finish` (or the next `suspended`). Any
 * other error becomes `event: error`.
 */
async function pumpAgentRun(params: {
  readonly stream: TurnStream;
  readonly out: AgentRunOutput;
  readonly meta: Record<string, unknown>;
  readonly announce: boolean;
}): Promise<void> {
  const { stream, out, meta } = params;
  const send = (event: string | null, data: unknown): void => stream.append({ event, data });
  if (params.announce) send("thread", meta);
  // The last approval the run asked for — reported if the run suspends.
  let pendingApproval: { toolCallId: string; toolName: string } | null = null;
  try {
    for await (const event of out.fullStream) {
      switch (event.type) {
        case "text-delta":
          send(null, { delta: event.delta });
          break;
        case "approval-requested":
          pendingApproval = { toolCallId: event.toolCallId, toolName: event.toolName };
          send("approval-requested", { ...pendingApproval, input: event.input });
          break;
        case "error":
          send("error", { message: event.error.message });
          break;
        default:
          break;
      }
    }
    // An approval request alone doesn't mean the run suspended (an inline
    // `onApprovalRequired` hook answers it in-process): the outcome decides.
    const text = await out.text;
    const finishReason = await out.finishReason;
    const usage = await out.usage;
    send("finish", { ...meta, text, finishReason, usage });
  } catch (err) {
    if (err instanceof WorkflowSuspendedError) {
      send("suspended", { ...meta, ...pendingApproval });
    } else if (err instanceof ApprovalNotFoundError) {
      send("error", { error: "approval_not_found" });
    } else if (err instanceof ApprovalAlreadyDecidedError) {
      send("error", { error: "approval_already_decided" });
    } else if (err instanceof ApprovalExpiredError) {
      send("error", { error: "approval_expired" });
    } else if (err instanceof ApprovalNotReadyError) {
      send("error", { error: "approval_not_ready" });
    } else if (err instanceof TurnKeyConflictError) {
      send("error", { error: "idempotency_key_mismatch" });
    } else {
      send("error", streamErrorPayload(err));
    }
  } finally {
    stream.end();
  }
}
