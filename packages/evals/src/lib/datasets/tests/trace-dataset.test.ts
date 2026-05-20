import { describe, expect, it } from "bun:test";
import type { MemoryStore } from "@promin/agent";
import type { EvalCase, EvalDataset } from "../../types.ts";
import { traceDataset } from "../trace.ts";

async function collect(dataset: EvalDataset): Promise<EvalCase[]> {
  const cases: EvalCase[] = [];
  for await (const evalCase of dataset.cases()) cases.push(evalCase);
  return cases;
}

// A MemoryStore stub exposing only the two methods traceDataset reads.
const fakeMemory = {
  listThreads: async () => [
    { namespaceId: "ns", resourceId: null, threadId: "t1" },
    { namespaceId: "ns", resourceId: null, threadId: "t2" },
  ],
  getMessages: async (key: { threadId: string }) =>
    key.threadId === "t1"
      ? [
          { role: "user", content: "hello from t1" },
          { role: "assistant", content: "hi" },
        ]
      : [{ role: "assistant", content: "no user turn here" }],
} as unknown as MemoryStore;

describe("traceDataset", () => {
  it("turns threads with a user message into cases", async () => {
    const cases = await collect(traceDataset(fakeMemory, { namespaceId: "ns" }));
    expect(cases.length).toBe(1); // t2 has no user message → skipped
    expect(cases[0]?.id).toBe("t1");
    expect(cases[0]?.input).toBe("hello from t1");
    expect(cases[0]?.expected).toBeUndefined();
  });

  it("honours the limit", async () => {
    const cases = await collect(traceDataset(fakeMemory, { namespaceId: "ns", limit: 0 }));
    expect(cases.length).toBe(0);
  });
});
