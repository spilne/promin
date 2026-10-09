// ---------------------------------------------------------------------------
// SSE /api/runs/:id/events — the keep-alive heartbeat and the run watcher's
// poll are timers on the injected clock; cancelling the stream clears both.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";
import { RunEventBus } from "../../run-event-bus.ts";
import { streamRunEvents } from "../sse.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

describe("streamRunEvents — heartbeat on an injected clock", () => {
  it("emits a heartbeat comment every 15s of clock time and clears its timers on cancel", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "order", input: {} });

    const handler = streamRunEvents({
      storage,
      bus: new RunEventBus(),
      clock,
      pollIntervalMs: 60_000,
    });
    const res = await handler(new Request("http://x/api/runs/wf-1/events"), { id: "wf-1" });
    expect(res.status).toBe(200);

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let text = "";
    let reading = true;
    const pump = (async () => {
      while (reading) {
        const next = await reader.read();
        if (next.done) break;
        text += decoder.decode(next.value, { stream: true });
      }
    })();

    // Snapshot arrives; heartbeat interval + watcher poll are both on the clock.
    await waitFor(() => text.includes('"type":"snapshot"') && clock.pendingCount() === 2);

    clock.advance(14_999);
    await new Promise<void>((r) => setImmediate(r));
    expect(text.includes(": heartbeat")).toBe(false);

    clock.advance(1);
    await waitFor(() => text.includes(": heartbeat"));

    reading = false;
    await reader.cancel();
    await pump;
    expect(clock.pendingCount()).toBe(0);
  });
});
