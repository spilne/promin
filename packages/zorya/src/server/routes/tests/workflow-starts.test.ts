import { describe, it, expect } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";
import {
  claimWorkflowStarts,
  completeWorkflowStart,
  heartbeatWorkflowStart,
  releaseWorkflowStart,
} from "../workflow-starts.ts";

function post(body?: unknown): Request {
  return new Request("http://x/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    ...(body !== undefined && { body: JSON.stringify(body) }),
  });
}

async function claimOne(queue: InMemoryWorkflowStartQueue) {
  await queue.enqueue({ workflowId: "wf-1", workflowName: "wf" });
  const [rec] = await queue.claim({ workflowSpecs: [{ name: "wf", versions: [] }], limit: 1 });
  return rec!;
}

describe("worker-protocol start routes", () => {
  it("heartbeat-start answers ok for the current claim and not for a stale token", async () => {
    const queue = new InMemoryWorkflowStartQueue();
    const rec = await claimOne(queue);
    const handler = heartbeatWorkflowStart(queue);

    const ok = await handler(post({ claimToken: rec.claimToken }), { id: rec.id });
    expect(ok.status).toBe(200);
    expect(await ok.json()).toEqual({ ok: true });

    const stale = await handler(post({ claimToken: "stale" }), { id: rec.id });
    expect(await stale.json()).toEqual({ ok: false });
  });

  it("complete-start only deletes for the current claim token", async () => {
    const queue = new InMemoryWorkflowStartQueue();
    const rec = await claimOne(queue);
    const handler = completeWorkflowStart(queue);

    const stale = await handler(post({ claimToken: "stale" }), { id: rec.id });
    expect(await stale.json()).toEqual({ ok: false });
    expect(await queue.list()).toHaveLength(1);

    const ok = await handler(post({ claimToken: rec.claimToken }), { id: rec.id });
    expect(await ok.json()).toEqual({ ok: true });
    expect(await queue.list()).toHaveLength(0);
  });

  it("a reclaimed start rejects the first claimant's complete", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const queue = new InMemoryWorkflowStartQueue({ clock, reclaimAfterMs: 1_000 });
    const first = await claimOne(queue);
    clock.advance(1_001);
    const [second] = await queue.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      limit: 1,
    });
    const handler = completeWorkflowStart(queue);

    const res = await handler(post({ claimToken: first.claimToken }), { id: first.id });
    expect(await res.json()).toEqual({ ok: false });
    const remaining = await queue.list();
    expect(remaining).toHaveLength(1);
    expect(remaining[0]?.claimToken).toBe(second!.claimToken);
  });

  it("release-start gives the start back after the delay, fenced by the claim token", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const queue = new InMemoryWorkflowStartQueue({ clock });
    const rec = await claimOne(queue);
    const handler = releaseWorkflowStart(queue);

    const stale = await handler(post({ claimToken: "stale", delayMs: 1_000 }), { id: rec.id });
    expect(await stale.json()).toEqual({ ok: false });
    const ok = await handler(post({ claimToken: rec.claimToken, delayMs: 1_000 }), {
      id: rec.id,
    });
    expect(await ok.json()).toEqual({ ok: true });

    const specs = [{ name: "wf", versions: [] }];
    expect(await queue.claim({ workflowSpecs: specs, limit: 1 })).toHaveLength(0);
    clock.advance(1_000);
    expect(await queue.claim({ workflowSpecs: specs, limit: 1 })).toHaveLength(1);
  });

  it("claim-starts validates its body and caps the batch", async () => {
    const queue = new InMemoryWorkflowStartQueue();
    const claim = claimWorkflowStarts(queue);
    await expect(claim(post({ workflowSpecs: "nope" }))).rejects.toMatchObject({ status: 400 });
    await expect(claim(post({ workflowSpecs: [], limit: 1_000 }))).rejects.toMatchObject({
      status: 400,
    });
    await queue.enqueue({ workflowId: "a", workflowName: "wf", version: "2" });
    const res = await claim(
      post({ workflowSpecs: [{ name: "wf", versions: ["1"] }], workerId: "w1", limit: 5 }),
    );
    expect(((await res.json()) as { starts: unknown[] }).starts).toEqual([]);
    const res2 = await claim(
      post({ workflowSpecs: [{ name: "wf", versions: ["2"] }], workerId: "w1", limit: 5 }),
    );
    const { starts } = (await res2.json()) as { starts: Array<{ claimedBy?: string }> };
    expect(starts.map((s) => s.claimedBy)).toEqual(["w1"]);
  });

  it("rejects a request without a claim token", async () => {
    const queue = new InMemoryWorkflowStartQueue();
    const rec = await claimOne(queue);
    await expect(completeWorkflowStart(queue)(post({}), { id: rec.id })).rejects.toMatchObject({
      status: 400,
      code: "invalid_request",
    });
    expect(await queue.list()).toHaveLength(1);
  });
});
