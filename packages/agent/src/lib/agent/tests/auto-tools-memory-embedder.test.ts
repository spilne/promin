// ---------------------------------------------------------------------------
// The auto-attached `memory` tool takes the agent's `memoryEmbedder`, so
// `recall` finds a fact by meaning.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { withAutoTools } from "../auto-tools.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";

describe("withAutoTools memory tool", () => {
  it("recalls by meaning with a memoryEmbedder, and lexically without one", async () => {
    const memory = new InMemoryMemoryStore();
    await memory.appendResourceFact(
      { namespaceId: "acme", resourceId: "alice" },
      { text: "resides in Lisbon", source: "user" },
    );
    const vectors: Record<string, number[]> = {
      "where do I live": [1, 0],
      "resides in Lisbon": [0.9, 0.1],
    };
    const recall = async (memoryEmbedder?: { embed(text: string): Promise<number[]> }) => {
      const { memory: tool } = withAutoTools({
        sources: { agentId: "a1", memory, ...(memoryEmbedder && { memoryEmbedder }) },
        scope: { namespaceId: "acme", resourceId: "alice", threadId: "t1" },
        userTools: {},
      });
      return tool!.execute(tool!.parameters.parse({ command: "recall", query: "where do I live" }));
    };

    expect(await recall({ embed: async (t) => vectors[t] ?? [0, 0] })).toContain(
      "resides in Lisbon",
    );
    expect(await recall()).toContain("No memories matched");
  });
});
