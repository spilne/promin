import { describe, expect, it } from "bun:test";
import {
  InMemoryMemoryStore,
  type ListThreadsParams,
  type MessageRange,
  type StoredMessage,
  type ThreadKey,
  type ThreadSummary,
} from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import type { EvalCase, EvalDataset } from "../../types.ts";
import { traceDataset } from "../trace.ts";

async function collect(dataset: EvalDataset): Promise<EvalCase[]> {
  const cases: EvalCase[] = [];
  for await (const evalCase of dataset.cases()) cases.push(evalCase);
  return cases;
}

/** An in-memory store that records how traceDataset reads it. */
class RecordingMemoryStore extends InMemoryMemoryStore {
  readonly listCalls: ListThreadsParams[] = [];
  readonly messageReads: Array<MessageRange | undefined> = [];

  override async listThreads(params: ListThreadsParams): Promise<ThreadSummary[]> {
    this.listCalls.push(params);
    return super.listThreads(params);
  }

  override async getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]> {
    this.messageReads.push(range);
    return super.getMessages(key, range);
  }
}

async function seed(params: {
  store: InMemoryMemoryStore;
  clock: FakeWallClock;
  threads: number;
}): Promise<void> {
  for (let i = 0; i < params.threads; i += 1) {
    params.clock.set(params.clock.currentTimeMs() + 1);
    const key = { namespaceId: "ns", threadId: `t${String(i).padStart(3, "0")}` };
    await params.store.createThread(key);
    await params.store.appendMessages(key, [
      { role: "user", content: `question ${i}` },
      { role: "assistant", content: "answer" },
    ]);
  }
}

describe("traceDataset", () => {
  it("turns threads with a user message into cases", async () => {
    const store = new InMemoryMemoryStore();
    await store.createThread({ namespaceId: "ns", threadId: "t1" });
    await store.appendMessages({ namespaceId: "ns", threadId: "t1" }, [
      { role: "user", content: "hello from t1" },
      { role: "assistant", content: "hi" },
    ]);
    await store.createThread({ namespaceId: "ns", threadId: "t2" });
    await store.appendMessages({ namespaceId: "ns", threadId: "t2" }, [
      { role: "assistant", content: "no user turn here" },
    ]);
    const cases = await collect(traceDataset({ memory: store, namespaceId: "ns" }));
    expect(cases.map((c) => [c.id, c.input])).toEqual([["t1", "hello from t1"]]);
    expect(cases[0]?.expected).toBeUndefined();
  });

  it("reads threads a page at a time in creation order, and only each thread's head", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new RecordingMemoryStore({ clock });
    await seed({ store, clock, threads: 25 });
    const cases = await collect(traceDataset({ memory: store, namespaceId: "ns", pageSize: 10 }));
    expect(cases.length).toBe(25);
    expect(new Set(cases.map((c) => c.id)).size).toBe(25);
    expect(
      store.listCalls.map((c) => [c.limit, c.cursor !== undefined, c.order, c.archived]),
    ).toEqual([
      [10, false, "createdAsc", false],
      [10, true, "createdAsc", false],
      [10, true, "createdAsc", false],
    ]);
    expect(store.messageReads.every((range) => range?.limit !== undefined)).toBe(true);
  });

  it("stops reading pages once the limit is reached", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new RecordingMemoryStore({ clock });
    await seed({ store, clock, threads: 25 });
    const cases = await collect(
      traceDataset({ memory: store, namespaceId: "ns", pageSize: 10, limit: 12 }),
    );
    expect(cases.map((c) => c.id)).toEqual(
      Array.from({ length: 12 }, (_unused, i) => `t${String(i).padStart(3, "0")}`),
    );
    expect(store.listCalls.length).toBe(2);
    expect(await collect(traceDataset({ memory: store, namespaceId: "ns", limit: 0 }))).toEqual([]);
  });

  it("finds a first user message past the head slice", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "ns", threadId: "late" };
    await store.createThread(key);
    await store.appendMessages(
      key,
      Array.from({ length: 30 }, () => ({ role: "assistant" as const, content: "system chatter" })),
    );
    await store.appendMessages(key, [{ role: "user", content: "finally" }]);
    const cases = await collect(traceDataset({ memory: store, namespaceId: "ns" }));
    expect(cases.map((c) => c.input)).toEqual(["finally"]);
  });

  it("rejects a non-positive page size or a negative limit", () => {
    const memory = new InMemoryMemoryStore();
    expect(() => traceDataset({ memory, namespaceId: "ns", pageSize: 0 })).toThrow(RangeError);
    expect(() => traceDataset({ memory, namespaceId: "ns", limit: -1 })).toThrow(RangeError);
  });
});
