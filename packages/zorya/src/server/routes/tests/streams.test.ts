// ---------------------------------------------------------------------------
// Streams routes — append + snapshot read. SSE handler smoke-tested
// separately; here we hit the JSON snapshot path that mirrors what an
// SSE client would consume on reconnect.
// ---------------------------------------------------------------------------

import { beforeEach, describe, expect, it } from "bun:test";
import { defineInputStream, defineStream, InMemoryWorkflowStorage } from "@promin/workflow";
import { getStreamChunks, sendStreamChunk, type StreamChunkDto } from "../streams.ts";

const jsonReq = (body: unknown) =>
  new Request("http://x", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

describe("streams routes", () => {
  let storage: InMemoryWorkflowStorage;
  const workflowId = "wf-streams";

  beforeEach(async () => {
    storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({
      workflowId,
      workflowName: "process",
      input: {},
    });
  });

  it("appends chunks and reads them back via the snapshot endpoint", async () => {
    const send = sendStreamChunk({ storage });
    const res1 = await send(jsonReq({ payload: { percent: 25 } }), {
      id: workflowId,
      streamId: "progress",
    });
    expect(res1.status).toBe(201);
    const body1 = (await res1.json()) as { chunkIndex: number };
    expect(body1.chunkIndex).toBe(0);

    const res2 = await send(jsonReq({ payload: { percent: 50 } }), {
      id: workflowId,
      streamId: "progress",
    });
    const body2 = (await res2.json()) as { chunkIndex: number };
    expect(body2.chunkIndex).toBe(1);

    const get = getStreamChunks({ storage });
    const snapshot = await get(
      new Request(`http://x/api/runs/${workflowId}/streams/progress/chunks`),
      { id: workflowId, streamId: "progress" },
    );
    const snap = (await snapshot.json()) as { chunks: StreamChunkDto[] };
    expect(snap.chunks).toHaveLength(2);
    expect(snap.chunks[0]!.chunkIndex).toBe(0);
    expect(snap.chunks[0]!.appendedBy).toBe("external");
    expect(snap.chunks[0]!.payload).toEqual({ percent: 25 });
  });

  it("supports `since` to replay from an offset (SSE reconnect path)", async () => {
    const send = sendStreamChunk({ storage });
    for (let i = 0; i < 5; i++) {
      await send(jsonReq({ payload: { i } }), {
        id: workflowId,
        streamId: "progress",
      });
    }

    const get = getStreamChunks({ storage });
    const res = await get(
      new Request(`http://x/api/runs/${workflowId}/streams/progress/chunks?since=2`),
      { id: workflowId, streamId: "progress" },
    );
    const body = (await res.json()) as { chunks: StreamChunkDto[] };
    expect(body.chunks.map((c) => c.chunkIndex)).toEqual([3, 4]);
  });

  it("rejects POST without payload (400)", async () => {
    const send = sendStreamChunk({ storage });
    const res = await send(jsonReq({}), { id: workflowId, streamId: "progress" });
    expect(res.status).toBe(400);
  });
});

describe("defineStream / defineInputStream descriptors", () => {
  it("defineStream produces an output descriptor with the right kind", () => {
    const s = defineStream<{ progress: number }>({ id: "progress" });
    expect(s.id).toBe("progress");
    expect(s.kind).toBe("output");
  });

  it("defineInputStream produces an input descriptor", () => {
    const s = defineInputStream<{ reason: string }>({ id: "cancel" });
    expect(s.kind).toBe("input");
  });
});

describe("storage append/read end-to-end", () => {
  it("MAX+1 indexing assigns monotonic indices across appends", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wfId = "wf-monotonic";
    await storage.createWorkflow({ workflowId: wfId, workflowName: "p", input: {} });
    for (let i = 0; i < 10; i++) {
      const r = await storage.appendStreamChunk({
        workflowId: wfId,
        streamId: "s",
        payload: { i },
        appendedBy: "workflow",
      });
      expect(r.chunkIndex).toBe(i);
    }
    const all = await storage.readStreamChunks({ workflowId: wfId, streamId: "s" });
    expect(all).toHaveLength(10);
    expect(all.every((c, i) => c.chunkIndex === i)).toBe(true);
  });

  it("appendedBy round-trips correctly", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wfId = "wf-by";
    await storage.createWorkflow({ workflowId: wfId, workflowName: "p", input: {} });
    await storage.appendStreamChunk({
      workflowId: wfId,
      streamId: "s",
      payload: 1,
      appendedBy: "workflow",
    });
    await storage.appendStreamChunk({
      workflowId: wfId,
      streamId: "s",
      payload: 2,
      appendedBy: "external",
    });
    const all = await storage.readStreamChunks({ workflowId: wfId, streamId: "s" });
    expect(all[0]!.appendedBy).toBe("workflow");
    expect(all[1]!.appendedBy).toBe("external");
  });

  it("streams are scoped by streamId — same workflow, different streams don't mix", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wfId = "wf-scoped";
    await storage.createWorkflow({ workflowId: wfId, workflowName: "p", input: {} });
    await storage.appendStreamChunk({
      workflowId: wfId,
      streamId: "a",
      payload: "alpha",
      appendedBy: "workflow",
    });
    await storage.appendStreamChunk({
      workflowId: wfId,
      streamId: "b",
      payload: "bravo",
      appendedBy: "workflow",
    });
    const a = await storage.readStreamChunks({ workflowId: wfId, streamId: "a" });
    const b = await storage.readStreamChunks({ workflowId: wfId, streamId: "b" });
    expect(a).toHaveLength(1);
    expect(b).toHaveLength(1);
    expect(a[0]!.payload).toBe("alpha");
    expect(b[0]!.payload).toBe("bravo");
  });
});
