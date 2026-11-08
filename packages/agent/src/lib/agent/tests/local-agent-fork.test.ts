// ---------------------------------------------------------------------------
// LocalAgent.forkThread — rejects cuts that would corrupt the child (inside a
// tool exchange, with an approval pending), renumbers copied episodes onto the
// child's own message seqs, and never leaves a usable half-copied thread.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryPendingApprovalStore } from "../../approvals/in-memory-pending-approval-store.ts";
import { ThreadAwaitingApprovalError } from "../../approvals/pending-approval-store.ts";
import { InMemoryAgentRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import type { LLMProvider } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import type { EpisodeInput, ThreadKey } from "../../memory/types.ts";
import { InvalidForkPointError, ThreadExistsError } from "../errors.ts";
import { FORK_IN_PROGRESS_METADATA_KEY } from "../types.ts";

const LLM: LLMProvider = { chat: async () => ({ content: "ok", finishReason: "stop" }) };
const BACKEND = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: null, tools: [] } },
};
const parentKey: ThreadKey = { namespaceId: "ns", resourceId: "alice", threadId: "parent" };
const childKey = (threadId: string): ThreadKey => ({ ...parentKey, threadId });

/** Parent thread: seq 1 user, 2 assistant(tool call), 3 tool result, 4 assistant. */
const TRANSCRIPT: Message[] = [
  { role: "user", content: "deploy" },
  { role: "assistant", content: null, toolCalls: [{ id: "c1", name: "deploy", input: {} }] },
  { role: "tool", toolCallId: "c1", content: "done" },
  { role: "assistant", content: "deployed" },
];

async function setup(memory = new InMemoryMemoryStore()) {
  const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
  const approvals = new InMemoryPendingApprovalStore();
  const registry = new InMemoryAgentRegistry();
  const recipe = await registry.register({ id: "writer", backend: BACKEND });
  const agent = (
    await resolveLocalAgent(recipe, { runner, memory, approvals, llm: () => LLM, tools: {} })
  ).withScope({ namespaceId: "ns", resourceId: "alice" });
  await agent.thread("parent");
  await memory.appendMessages(parentKey, TRANSCRIPT);
  return { agent, memory, approvals };
}

const contents = async (memory: InMemoryMemoryStore, threadId: string) =>
  (await memory.getMessages(childKey(threadId))).map((m) => m.content);

describe("LocalAgent.forkThread", () => {
  it("rejects a cut between a tool call and its result, but allows turn boundaries", async () => {
    const { agent, memory } = await setup();
    await expect(agent.forkThread!("parent", "bad", { atSeq: 2 })).rejects.toBeInstanceOf(
      InvalidForkPointError,
    );
    expect(await memory.getThread(childKey("bad"))).toBeNull();
    await agent.forkThread!("parent", "ok", { atSeq: 3 });
    expect(await contents(memory, "ok")).toEqual(["deploy", null, "done"]);
    await agent.forkThread!("parent", "start", { atSeq: 1 });
    expect(await contents(memory, "start")).toEqual(["deploy"]);
  });

  it("refuses a parent with an open approval", async () => {
    const { agent, memory, approvals } = await setup();
    await approvals.record({
      namespaceId: "ns",
      threadId: "parent",
      toolCallId: "c1",
      workflowId: "wf",
      toolName: "deploy",
      input: {},
    });
    await expect(agent.forkThread!("parent", "child")).rejects.toBeInstanceOf(
      ThreadAwaitingApprovalError,
    );
    expect(await memory.getThread(childKey("child"))).toBeNull();
  });

  it("throws ThreadExistsError for a taken id and leaves that thread untouched", async () => {
    const { agent, memory } = await setup();
    await agent.thread("taken");
    await expect(agent.forkThread!("parent", "taken")).rejects.toBeInstanceOf(ThreadExistsError);
    expect(await memory.getMessages(childKey("taken"))).toEqual([]);
  });

  it("remaps episode ranges onto the child's seqs when the parent head is compacted", async () => {
    // Parent seqs 3..4 remain (1..2 compacted away); the child renumbers from 1.
    class CompactedStore extends InMemoryMemoryStore {
      override async getMessages(
        key: ThreadKey,
        range?: Parameters<InMemoryMemoryStore["getMessages"]>[1],
      ) {
        const all = await super.getMessages(key, range);
        return key.threadId === "parent" ? all.filter((m) => m.seq > 2) : all;
      }
    }
    const { agent, memory } = await setup(new CompactedStore());
    const episode = (fromSeq: number, toSeq: number): EpisodeInput => ({
      summary: `${fromSeq}-${toSeq}`,
      sourceThreadId: "parent",
      sourceMessageRange: { fromSeq, toSeq },
    });
    for (const e of [episode(1, 4), episode(3, 3), episode(1, 2)]) {
      await memory.appendThreadEpisode(parentKey, e);
    }
    await agent.forkThread!("parent", "child");
    const ranges = Object.fromEntries(
      (await memory.listThreadEpisodes(childKey("child"))).map((e) => [
        e.summary,
        e.sourceMessageRange,
      ]),
    );
    expect(ranges["1-4"]).toEqual({ fromSeq: 1, toSeq: 2 });
    expect(ranges["3-3"]).toEqual({ fromSeq: 1, toSeq: 1 });
    expect(ranges["1-2"]).toBeNull();
  });

  it("deletes the child on a failed copy and hides an interrupted fork", async () => {
    class FailingStore extends InMemoryMemoryStore {
      failAppend = false;
      override async appendMessages(...args: Parameters<InMemoryMemoryStore["appendMessages"]>) {
        if (this.failAppend && args[0].threadId !== "parent") throw new Error("disk full");
        return super.appendMessages(...args);
      }
    }
    const memory = new FailingStore();
    const { agent } = await setup(memory);
    memory.failAppend = true;
    await expect(agent.forkThread!("parent", "child")).rejects.toThrow("disk full");
    expect(await memory.getThread(childKey("child"))).toBeNull();

    // A crash leaves the marker behind: the row is not a usable thread.
    await memory.createThread(childKey("crashed"), {
      metadata: { [FORK_IN_PROGRESS_METADATA_KEY]: true },
    });
    await expect(agent.thread("crashed", { createIfMissing: false })).rejects.toThrow(
      "Thread not found",
    );
    expect((await agent.listThreads()).map((t) => t.id)).toEqual(["parent"]);
  });

  it("clears the marker and records lineage on success", async () => {
    const { agent, memory } = await setup();
    await agent.forkThread!("parent", "child");
    const metadata = (await memory.getThread(childKey("child")))?.metadata;
    expect(metadata).toMatchObject({
      branchParentThreadId: "parent",
      branchAtSeq: 4,
      branchDepth: 1,
    });
    expect(metadata).not.toHaveProperty(FORK_IN_PROGRESS_METADATA_KEY);
  });
});
