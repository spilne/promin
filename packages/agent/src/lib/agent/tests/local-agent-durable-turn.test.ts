// ---------------------------------------------------------------------------
// LocalAgent thread turns as one durable unit:
//   - a send retried with the same turn key is the same turn (one LLM run,
//     one copy of history)
//   - the user message is recorded at turn start, so a failed turn keeps it
//   - a crash after the transcript was written but before the step was
//     journaled, recovered on another worker, never duplicates history
//   - run output: steps, this turn's tool calls / results, a push-based
//     fullStream every consumer reads in full, and a cancel() that waits
//     for the run to stop
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { TurnKeyConflictError } from "../errors.ts";
import { LocalAgent, threadTurnWorkflowId } from "../local-agent.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import type { AppendMessagesOptions, StoredMessage, ThreadKey } from "../../memory/types.ts";
import type { Message } from "../../message.ts";
import { tool } from "../../tool.ts";
import { deferred, reactiveLLM, scriptedLLM, until } from "../../tests/engine-harness.ts";
import type { AgentEvent } from "../types.ts";

/** Lock lease of `InMemoryWorkflowStorage` runs, plus a margin. */
const PAST_LOCK_LEASE_MS = 121_000;
const KEY: ThreadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t-1" };

function makeAgent(params: {
  llm: ReturnType<typeof scriptedLLM> | ReturnType<typeof reactiveLLM>;
  memory?: InMemoryMemoryStore;
  storage?: InMemoryWorkflowStorage;
}) {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const memory = params.memory ?? new InMemoryMemoryStore();
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner: createWorkflowRunner({ storage }),
    memory,
    agent: { name: "durable", llm: params.llm, llmRetry: false },
  });
  return { agent, memory, storage };
}

async function transcript(memory: InMemoryMemoryStore): Promise<Array<[string, unknown]>> {
  return (await memory.getMessages(KEY)).map((m) => [m.role, m.content]);
}

describe("LocalAgent thread turn — deterministic turn id", () => {
  it("a send retried with the same turn key runs once and records history once", async () => {
    const llm = scriptedLLM([{ content: "pong", finishReason: "stop" }]);
    const { agent, memory, storage } = makeAgent({ llm });
    const thread = await agent.thread("t-1");

    const first = await thread.send({ task: "ping" }, { turnKey: "req-1" });
    const retry = await thread.send({ task: "ping" }, { turnKey: "req-1" });

    expect(await first.text).toBe("pong");
    expect(await retry.text).toBe("pong");
    expect(llm.calls).toHaveLength(1);
    expect(await transcript(memory)).toEqual([
      ["user", "ping"],
      ["assistant", "pong"],
    ]);
    const workflowId = threadTurnWorkflowId({
      namespaceId: "acme",
      threadId: "t-1",
      turnKey: "req-1",
    });
    expect((await storage.loadWorkflow(workflowId))?.status).toBe("completed");
  });

  it("an invoke retried with the same runId runs once", async () => {
    const llm = scriptedLLM([{ content: "pong", finishReason: "stop" }]);
    const { agent } = makeAgent({ llm });
    const first = await agent.invoke({ task: "ping" }, { runId: "delivery-1" });
    const retry = await agent.invoke({ task: "ping" }, { runId: "delivery-1" });
    expect(await first.text).toBe("pong");
    expect(await retry.text).toBe("pong");
    expect(llm.calls).toHaveLength(1);
  });

  it("a turn key or run id reused for a different task is refused", async () => {
    const llm = scriptedLLM([
      { content: "pong", finishReason: "stop" },
      { content: "pong", finishReason: "stop" },
    ]);
    const { agent, memory } = makeAgent({ llm });
    await agent.invoke({ task: "ping" }, { runId: "delivery-1" });
    await expect(
      agent.invoke({ task: "something else" }, { runId: "delivery-1" }),
    ).rejects.toBeInstanceOf(TurnKeyConflictError);

    const thread = await agent.thread("t-1");
    await thread.send({ task: "first message" }, { turnKey: "k-1" });
    await expect(
      thread.send({ task: "a different message" }, { turnKey: "k-1" }),
    ).rejects.toBeInstanceOf(TurnKeyConflictError);
    expect(llm.calls).toHaveLength(2);
    expect(await transcript(memory)).toEqual([
      ["user", "first message"],
      ["assistant", "pong"],
    ]);
  });

  it("turn ids differ per thread, namespace and key, with no prefix relation", () => {
    const ids = [
      threadTurnWorkflowId({ namespaceId: "acme", threadId: "t", turnKey: "k" }),
      threadTurnWorkflowId({ namespaceId: "acme", threadId: "t-1", turnKey: "k" }),
      threadTurnWorkflowId({ namespaceId: "globex", threadId: "t", turnKey: "k" }),
      threadTurnWorkflowId({ namespaceId: "acme", threadId: "t", turnKey: "k2" }),
    ];
    expect(new Set(ids).size).toBe(4);
    for (const a of ids) {
      for (const b of ids) if (a !== b) expect(b.startsWith(a)).toBe(false);
    }
  });
});

describe("LocalAgent thread turn — transcript", () => {
  it("a failed turn keeps its user message", async () => {
    const llm = reactiveLLM(() => {
      throw new Error("provider exploded");
    });
    const { agent, memory } = makeAgent({ llm });
    const thread = await agent.thread("t-1");
    await expect(thread.send({ task: "hello?" })).rejects.toThrow(/provider exploded/);
    expect(await transcript(memory)).toEqual([["user", "hello?"]]);
  });

  it("a crash after the output was written is recovered on another worker without duplicates", async () => {
    // Worker A writes the turn's output, then stalls before the step is
    // journaled; worker B takes the turn over after A's lock lapses and
    // runs `transcript-commit` again.
    const gate = deferred();
    let parked = false;
    class StallingStore extends InMemoryMemoryStore {
      stallCommit = true;
      override async appendMessages(
        key: ThreadKey,
        msgs: ReadonlyArray<Message>,
        opts?: AppendMessagesOptions,
      ): Promise<StoredMessage[]> {
        const stored = await super.appendMessages(key, msgs, opts);
        if (this.stallCommit && opts?.idempotencyKey?.endsWith(":commit")) {
          this.stallCommit = false;
          parked = true;
          await gate.promise;
        }
        return stored;
      }
    }
    const clock = FakeWallClock.create(Date.now());
    const storage = new InMemoryWorkflowStorage({ clock });
    const memory = new StallingStore();
    let llmCalls = 0;
    const llm = reactiveLLM(() => {
      llmCalls++;
      return { content: "answer", finishReason: "stop" };
    });
    const workerA = makeAgent({ llm, memory, storage }).agent;
    const workerB = makeAgent({ llm, memory, storage }).agent;

    const runA = (await workerA.thread("t-1"))
      .send({ task: "question" }, { turnKey: "req-7" })
      .then(
        () => undefined,
        () => undefined,
      );
    await until(() => parked);
    clock.set(clock.currentTimeMs() + PAST_LOCK_LEASE_MS);

    const recovered = await (
      await workerB.thread("t-1")
    ).send({ task: "question" }, { turnKey: "req-7" });
    expect(await recovered.text).toBe("answer");
    gate.resolve();
    await runA;

    expect(llmCalls).toBe(1);
    expect(await transcript(memory)).toEqual([
      ["user", "question"],
      ["assistant", "answer"],
    ]);
  });
});

describe("LocalAgent run output", () => {
  const echo = tool({
    name: "echo",
    description: "echo",
    parameters: z.object({ v: z.string() }),
    execute: async ({ v }) => `echo:${v}`,
  });

  function toolTurn(id: string, answer: string) {
    return [
      {
        content: null,
        finishReason: "tool_calls" as const,
        toolCalls: [{ id, name: "echo", input: { v: id } }],
      },
      { content: answer, finishReason: "stop" as const },
    ];
  }

  it("send reports steps and only this turn's tool calls and results", async () => {
    const llm = scriptedLLM([...toolTurn("c-1", "one"), ...toolTurn("c-2", "two")]);
    const storage = new InMemoryWorkflowStorage();
    const agent = new LocalAgent({
      namespaceId: "acme",
      resourceId: "alice",
      runner: createWorkflowRunner({ storage }),
      memory: new InMemoryMemoryStore(),
      agent: { name: "out", llm, tools: { echo } },
    });
    const thread = await agent.thread("t-1");
    await thread.send({ task: "first" });
    const out = await thread.send({ task: "second" });

    expect((await out.toolCalls).map((c) => c.id)).toEqual(["c-2"]);
    expect(await out.toolResults).toEqual([
      { toolCallId: "c-2", name: "echo", content: "echo:c-2", failed: false },
    ]);
    expect((await out.steps).map((s) => s.type)).toEqual(["llm", "tool", "llm"]);
  });

  it("fullStream is complete for every consumer, including one that starts late", async () => {
    const llm = scriptedLLM(toolTurn("c-1", "done"));
    const agent = new LocalAgent({
      namespaceId: "acme",
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      agent: { name: "stream", llm, tools: { echo } },
    });
    const out = (await agent.thread("t-1")).stream({ task: "go" });
    const read = async (): Promise<AgentEvent["type"][]> => {
      const types: AgentEvent["type"][] = [];
      for await (const e of out.fullStream) types.push(e.type);
      return types;
    };
    const early = await read();
    const late = await read();
    expect(early).toEqual([
      "step-end",
      "tool-call",
      "step-end",
      "text-delta",
      "step-end",
      "finish",
    ]);
    expect(late).toEqual(early);
  });

  it("cancel() aborts the run and resolves once it has stopped", async () => {
    const started = deferred();
    let settled = false;
    const llm = reactiveLLM(async (_messages, params) => {
      started.resolve();
      await new Promise<void>((resolve) => {
        params.signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      throw Object.assign(new Error("aborted"), { name: "AbortError" });
    });
    const agent = new LocalAgent({
      namespaceId: "acme",
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      agent: { name: "cancel", llm, llmRetry: false },
    });
    const out = (await agent.thread("t-1")).stream({ task: "long" });
    void out.finishReason.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await started.promise;
    await out.cancel();
    // Let the settle callbacks registered before cancel() run.
    await Promise.resolve();
    expect(settled).toBe(true);
    expect(await out.finishReason).toBe("cancelled");
  });
});
