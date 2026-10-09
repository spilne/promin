// ---------------------------------------------------------------------------
// Streams SSE handler — the between-poll wait is a timer on the injected
// clock, so new chunks surface only when FakeWallClock.advance() passes the
// poll interval.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";
import { streamChunks } from "../streams.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

describe("streamChunks — poll cadence on an injected clock", () => {
  it("re-reads storage only after pollIntervalMs of clock time", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const workflowId = "wf-stream-clock";
    await storage.createWorkflow({ workflowId, workflowName: "process", input: {} });
    await storage.appendStreamChunk({
      workflowId,
      streamId: "progress",
      payload: { percent: 10 },
      appendedBy: "external",
    });

    const abort = new AbortController();
    const res = await streamChunks({ storage, clock, pollIntervalMs: 500 })(
      new Request(`http://x/api/runs/${workflowId}/streams/progress`, { signal: abort.signal }),
      { id: workflowId, streamId: "progress" },
    );
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let text = "";
    const pump = (async () => {
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        text += decoder.decode(next.value, { stream: true });
      }
    })();

    // Initial read emits chunk 0, then the handler waits on the clock.
    await waitFor(() => text.includes('"percent":10') && clock.pendingCount() === 1);

    await storage.appendStreamChunk({
      workflowId,
      streamId: "progress",
      payload: { percent: 50 },
      appendedBy: "external",
    });
    clock.advance(499);
    await new Promise<void>((r) => setImmediate(r));
    expect(text.includes('"percent":50')).toBe(false);

    clock.advance(1);
    await waitFor(() => text.includes('"percent":50') && clock.pendingCount() === 1);

    // Abort closes the stream; the in-flight wait finishes on the next tick.
    abort.abort();
    clock.advance(500);
    await pump;
    await waitFor(() => clock.pendingCount() === 0);
  });
});
