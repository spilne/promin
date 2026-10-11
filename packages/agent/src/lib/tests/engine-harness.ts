// ---------------------------------------------------------------------------
// Test harness for the agent turn engine: scripted LLMs, a run helper with
// an event-collecting bus, and a worker-crash simulation (a run stalls
// inside an activity past its lock lease and a second worker takes it
// over, replaying the journal — the same shape as the workflow package's
// zombie-worker suite).
// ---------------------------------------------------------------------------

import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import type { LLMChatParams, LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { Message } from "../message.ts";
import { SessionEventBus, type SessionEvent } from "../session-logger.ts";

/** Lock lease of `InMemoryWorkflowStorage` runs, plus a margin. */
const PAST_LOCK_LEASE_MS = 121_000;

export function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/** Yield to the event loop until `condition` holds, bounded by iterations. */
export async function until(condition: () => boolean, maxTurns = 5_000): Promise<void> {
  for (let i = 0; i < maxTurns; i++) {
    if (condition()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("until: condition never held");
}

/** An LLM that replays `responses` in order and records every call's params. */
export function scriptedLLM(responses: ReadonlyArray<LLMResponse>): LLMProvider & {
  calls: LLMChatParams[];
} {
  const calls: LLMChatParams[] = [];
  return {
    calls,
    chat: async (params) => {
      calls.push(params);
      const r = responses[calls.length - 1];
      if (!r) throw new Error(`scripted LLM exhausted after ${responses.length} calls`);
      return r;
    },
  };
}

/** An LLM whose reply depends on the conversation so far. */
export function reactiveLLM(
  reply: (
    messages: ReadonlyArray<Message>,
    params: LLMChatParams,
  ) => LLMResponse | Promise<LLMResponse>,
): LLMProvider & { calls: LLMChatParams[] } {
  const calls: LLMChatParams[] = [];
  return {
    calls,
    chat: async (params) => {
      calls.push(params);
      return reply(params.messages, params);
    },
  };
}

export function lastMessage(messages: ReadonlyArray<Message>): Message | undefined {
  return messages[messages.length - 1];
}

/** A bus plus the events it saw. */
export function collectingBus(): { bus: SessionEventBus; events: SessionEvent[] } {
  const bus = new SessionEventBus();
  const events: SessionEvent[] = [];
  bus.subscribe((e) => events.push(e));
  return { bus, events };
}

export function freshRunner(): { runner: WorkflowRunner; storage: InMemoryWorkflowStorage } {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

/**
 * Simulate a worker crash inside an activity. Worker A starts the run;
 * once `stalled()` reports A is parked inside the activity (the test's
 * fake parks it on a gate), A's lock lease is expired and worker B runs
 * the same workflow id from the journal. B's result is returned; A is
 * released afterwards (its writes are fenced off) and settled before the
 * function returns.
 */
export async function crashAndRecover<I, O>(params: {
  /** Builds the workflow; called once per worker so each gets its own config. */
  build: (worker: "A" | "B") => Workflow<I, O>;
  input: I;
  workflowId: string;
  stalled: () => boolean;
  /** Unparks worker A after B finished. */
  release: () => void;
}): Promise<{ result: O; storage: InMemoryWorkflowStorage }> {
  const clock = FakeWallClock.create(Date.now());
  const storage = new InMemoryWorkflowStorage({ clock });
  const runA = createWorkflowRunner({ storage })
    .run({ workflow: params.build("A"), workflowId: params.workflowId, input: params.input })
    .then(
      () => undefined,
      () => undefined,
    );
  try {
    await until(params.stalled);
    clock.set(clock.currentTimeMs() + PAST_LOCK_LEASE_MS);
    const result = await createWorkflowRunner({ storage }).run({
      workflow: params.build("B"),
      workflowId: params.workflowId,
      input: params.input,
    });
    return { result, storage };
  } finally {
    params.release();
    await runA;
  }
}
