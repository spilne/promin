// ---------------------------------------------------------------------------
// `LocalAgentConfig.memoryProfile` is the single source for memory settings:
// it expands into the agent's memory fields, refuses to sit next to the fields
// it replaces, and a profile with the consolidator off makes distilling fail.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { LocalAgent, type LocalAgentConfig } from "../local-agent.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { memoryProfile } from "../../memory/memory-profile.ts";
import { scriptedLLM } from "../../tests/engine-harness.ts";

function config(extra: Partial<LocalAgentConfig> = {}): LocalAgentConfig {
  return {
    namespaceId: "acme",
    resourceId: "alice",
    runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
    agent: {
      name: "a",
      llm: scriptedLLM([{ content: "ok", finishReason: "stop" }]),
      llmRetry: false,
    },
    ...extra,
  };
}

describe("LocalAgent memoryProfile", () => {
  it("persists thread state in the profile's store, and refuses fields it replaces", async () => {
    const store = new InMemoryMemoryStore();
    const profile = memoryProfile({ store }).build();
    const agent = new LocalAgent(config({ memoryProfile: profile }));
    const thread = await agent.thread("t-1");
    await (
      await thread.send({ task: "hi" })
    ).text;
    expect(
      await store.getMessages({ namespaceId: "acme", resourceId: "alice", threadId: "t-1" }),
    ).not.toHaveLength(0);

    expect(() => new LocalAgent(config({ memoryProfile: profile, memory: store }))).toThrow(
      "`memory`",
    );
    expect(
      () =>
        new LocalAgent(config({ memoryProfile: profile, contextBudget: { maxMessageTokens: 1 } })),
    ).toThrow("`contextBudget`");
    // A scoped view rebuilds from the resolved config without expanding twice.
    expect(() => agent.withScope({ namespaceId: "other" })).not.toThrow();
  });

  it("with the consolidator off, distilling throws and autoDistill is refused", async () => {
    const profile = memoryProfile({ store: new InMemoryMemoryStore() }).consolidate(false).build();
    const agent = new LocalAgent(config({ memoryProfile: profile }));
    await agent.thread("t-1");
    await expect(agent.distillThread("t-1")).rejects.toThrow("turned off");
    expect(
      () =>
        new LocalAgent(config({ memoryProfile: profile, autoDistill: { messageThreshold: 4 } })),
    ).toThrow("autoDistill");
  });
});
