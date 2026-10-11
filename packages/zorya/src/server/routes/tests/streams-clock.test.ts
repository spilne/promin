// ---------------------------------------------------------------------------
// Streams SSE handler — the between-poll wait is a timer on the injected
// clock, so new chunks surface only when FakeWallClock.advance() passes the
// poll interval. The stream follows its run through the shared watcher pool
// and ends, with no timer left behind, once the run reaches any terminal
// status.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";
import { RunEventBus } from "../../run-event-bus.ts";
import { RunWatcherPool } from "../../run-poll-watcher.ts";
import { streamChunks } from "../streams.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

const POLL_MS = 500;
const WATCH_MS = 1_000;

async function setup(workflowId: string) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const watchers = new RunWatcherPool({
    storage,
    bus: new RunEventBus(),
    clock,
    intervalMs: WATCH_MS,
  });
  await storage.createWorkflow({ workflowId, workflowName: "process", input: {} });
  const append = (payload: unknown) =>
    storage.appendStreamChunk({
      workflowId,
      streamId: "progress",
      payload,
      appendedBy: "external",
    });
  return { clock, storage, watchers, append };
}

/** Open the SSE route and collect its body text until it closes. */
async function open(params: {
  storage: InMemoryWorkflowStorage;
  watchers: RunWatcherPool;
  clock: FakeWallClock;
  workflowId: string;
  signal?: AbortSignal;
}) {
  const { storage, watchers, clock, workflowId, signal } = params;
  const res = await streamChunks({ storage, watchers, clock, pollIntervalMs: POLL_MS })(
    new Request(`http://x/api/runs/${workflowId}/streams/progress`, signal ? { signal } : {}),
    { id: workflowId, streamId: "progress" },
  );
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  const out = { text: "", closed: false };
  const pump = (async () => {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      out.text += decoder.decode(next.value, { stream: true });
    }
    out.closed = true;
  })();
  return { res, out, pump };
}

describe("streamChunks — poll cadence on an injected clock", () => {
  it("re-reads storage only after pollIntervalMs of clock time", async () => {
    const workflowId = "wf-stream-clock";
    const { clock, storage, watchers, append } = await setup(workflowId);
    await append({ percent: 10 });

    const abort = new AbortController();
    const { out, pump } = await open({
      storage,
      watchers,
      clock,
      workflowId,
      signal: abort.signal,
    });

    // Initial read emits chunk 0, then the handler waits on the clock
    // (one poll timer, plus the run watcher's).
    await waitFor(() => out.text.includes('"percent":10') && clock.pendingCount() === 2);

    await append({ percent: 50 });
    clock.advance(POLL_MS - 1);
    await new Promise<void>((r) => setImmediate(r));
    expect(out.text.includes('"percent":50')).toBe(false);

    clock.advance(1);
    await waitFor(() => out.text.includes('"percent":50') && clock.pendingCount() === 2);

    // Abort closes the stream at once and clears its wait and the watcher.
    abort.abort();
    await pump;
    await waitFor(() => clock.pendingCount() === 0);
    expect(watchers.activeWatchers()).toBe(0);
    expect(out.text.includes("event: end")).toBe(false);
  });
});

describe("streamChunks — ends with its run", () => {
  const endings: Array<{
    status: string;
    end: (s: InMemoryWorkflowStorage, workflowId: string) => Promise<void>;
  }> = [
    {
      status: "completed",
      end: (s, workflowId) => s.completeWorkflow({ workflowId, result: 1 }),
    },
    {
      status: "failed",
      end: (s, workflowId) => s.failWorkflow({ workflowId, error: "boom" }),
    },
    { status: "cancelled", end: (s, workflowId) => s.cancelWorkflow({ workflowId }) },
    {
      status: "tripwire",
      end: (s, workflowId) => s.tripwireWorkflow({ workflowId, reason: "stop" }),
    },
  ];

  for (const { status, end } of endings) {
    it(`drains, sends end and closes when the run ends ${status}`, async () => {
      const workflowId = `wf-stream-end-${status}`;
      const { clock, storage, watchers, append } = await setup(workflowId);
      await append({ n: 1 });
      const { out, pump } = await open({ storage, watchers, clock, workflowId });
      await waitFor(() => out.text.includes('"n":1') && clock.pendingCount() === 2);

      // The last chunk lands just before the run ends; it is still delivered.
      await append({ n: 2 });
      await end(storage, workflowId);
      clock.advance(WATCH_MS);
      await pump;

      expect(out.closed).toBe(true);
      expect(out.text.includes('"n":2')).toBe(true);
      expect(out.text.endsWith("event: end\ndata: {}\n\n")).toBe(true);
      expect(out.text.indexOf('"n":2')).toBeLessThan(out.text.indexOf("event: end"));
      await waitFor(() => clock.pendingCount() === 0);
      expect(watchers.activeWatchers()).toBe(0);
    });
  }

  it("replays and ends at once for a run that already ended", async () => {
    const workflowId = "wf-stream-ended";
    const { clock, storage, watchers, append } = await setup(workflowId);
    await append({ n: 1 });
    await append({ n: 2 });
    await storage.completeWorkflow({ workflowId, result: null });

    const { out, pump } = await open({ storage, watchers, clock, workflowId });
    await pump;
    expect(out.text.includes('"n":1')).toBe(true);
    expect(out.text.includes('"n":2')).toBe(true);
    expect(out.text.endsWith("event: end\ndata: {}\n\n")).toBe(true);
    expect(clock.pendingCount()).toBe(0);
    expect(watchers.activeWatchers()).toBe(0);
  });

  it("answers 404 for an unknown run", async () => {
    const { clock, storage, watchers } = await setup("wf-stream-known");
    const res = await streamChunks({ storage, watchers, clock })(
      new Request("http://x/api/runs/nope/streams/progress"),
      { id: "nope", streamId: "progress" },
    );
    expect(res.status).toBe(404);
    expect(clock.pendingCount()).toBe(0);
  });
});
