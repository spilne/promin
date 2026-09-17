import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";
import type { RunEvent } from "../../server/api-types.ts";
import { LocalWorkflows } from "../../index.ts";

function makeWorkflows(storage: InMemoryWorkflowStorage) {
  return new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
}

async function readSseLines(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  decoder: TextDecoder,
  max: number,
  timeoutMs: number,
): Promise<string[]> {
  const lines: string[] = [];
  const deadline = Date.now() + timeoutMs;
  let buffer = "";
  while (lines.length < max && Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const read = reader.read();
    const timeout = new Promise<{ value?: Uint8Array; done: boolean }>((resolve) =>
      setTimeout(() => resolve({ done: true }), remaining),
    );
    const next = await Promise.race([read, timeout]);
    if (next.done) break;
    buffer += decoder.decode(next.value, { stream: true });
    const parts = buffer.split("\n\n");
    buffer = parts.pop() ?? "";
    for (const p of parts) {
      if (p.startsWith("data: ")) lines.push(p.slice(6));
    }
  }
  return lines;
}

describe("SSE /api/runs/:id/events", () => {
  it("emits a snapshot on subscribe", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "order", input: { a: 1 } });
    const server = new ZoryaServer({ workflows: makeWorkflows(storage), sseIntervalMs: 50 });

    const res = await server.handle(new Request("http://x/api/runs/wf-1/events"));
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    const events = await readSseLines(reader, decoder, 1, 2000);
    await reader.cancel();

    expect(events.length).toBeGreaterThanOrEqual(1);
    const first = JSON.parse(events[0]!) as RunEvent;
    expect(first.type).toBe("snapshot");
    if (first.type === "snapshot") {
      expect(first.run.workflowId).toBe("wf-1");
    }
  });

  it("returns 404 for missing workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const server = new ZoryaServer({ workflows: makeWorkflows(storage) });
    const res = await server.handle(new Request("http://x/api/runs/missing/events"));
    expect(res.status).toBe(404);
  });
});
