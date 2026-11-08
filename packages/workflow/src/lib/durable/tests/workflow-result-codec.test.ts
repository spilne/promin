// The workflow's final result must be stored in the codec's encoded form,
// like step results, so Date / Map / BigInt survive every read of a completed
// run through a JSON-serialising backend: a second runner's replay, an
// idempotent re-run, and `handle.result()`.

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import type {
  CompleteWorkflowParams,
  SaveStepResultParams,
  StepCheckpoint,
} from "../workflow-storage.ts";
import type { WorkflowStatusSnapshot } from "../workflow-state.ts";

const roundTrip = (v: unknown) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));

/** Every persisted result goes through JSON, as on Postgres / SQLite / Redis. */
class JsonRoundTripStorage extends InMemoryWorkflowStorage {
  override async saveStepResult(params: SaveStepResultParams): Promise<void> {
    await super.saveStepResult({ ...params, result: roundTrip(params.result) });
  }
  override async checkpointStep(c: StepCheckpoint): Promise<WorkflowStatusSnapshot | null> {
    if (c.outcome.kind !== "completed") return super.checkpointStep(c);
    return super.checkpointStep({
      ...c,
      outcome: { ...c.outcome, result: roundTrip(c.outcome.result) },
    });
  }
  override async completeWorkflow(params: CompleteWorkflowParams): Promise<void> {
    await super.completeWorkflow({ ...params, result: roundTrip(params.result) });
  }
}

const when = new Date("2026-04-16T12:00:00Z");
const value = () => ({ at: when, big: 12345678901234567890n, m: new Map([["k", when]]) });

function expectValue(v: unknown) {
  const r = v as ReturnType<typeof value>;
  expect(r.at).toBeInstanceOf(Date);
  expect(r.at.getTime()).toBe(when.getTime());
  expect(r.big).toBe(12345678901234567890n);
  expect(r.m).toBeInstanceOf(Map);
  expect((r.m.get("k") as Date).getTime()).toBe(when.getTime());
}

const build = (idempotent = false) =>
  workflow({ name: "wf-result-codec" })
    .step("a", async () => 1)
    .step("produce", async () => value())
    .build(idempotent ? { idempotency: { ttl: 60_000 } } : undefined);

describe("workflow result codec", () => {
  it("fresh run, second runner's replay and handle.result() all return real values", async () => {
    const storage = new JsonRoundTripStorage();
    const wf = build();
    const first = await createWorkflowRunner({ storage }).run({
      workflow: wf,
      workflowId: "r1",
      input: {},
    });
    expectValue(first);
    const other = createWorkflowRunner({ storage });
    expectValue(await other.run({ workflow: wf, workflowId: "r1", input: {} }));
    expectValue(await other.handle("r1").result());
  });

  it("idempotent re-run inside the TTL returns real values", async () => {
    const storage = new JsonRoundTripStorage();
    const wf = build(true);
    await createWorkflowRunner({ storage }).run({ workflow: wf, workflowId: "r2", input: {} });
    expectValue(
      await createWorkflowRunner({ storage }).run({ workflow: wf, workflowId: "r2", input: {} }),
    );
  });

  it("undefined result round-trips", async () => {
    const storage = new JsonRoundTripStorage();
    const wf = workflow({ name: "wf-undef" })
      .step("u", async () => undefined)
      .build({ idempotency: { ttl: 60_000 } });
    const r = createWorkflowRunner({ storage });
    expect(await r.run({ workflow: wf, workflowId: "u", input: {} })).toBeUndefined();
    expect(await r.run({ workflow: wf, workflowId: "u", input: {} })).toBeUndefined();
    expect(await r.handle("u").result()).toBeUndefined();
  });

  it("a legacy plain-JSON stored result still reads", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow({ name: "wf-legacy" })
      .step("p", async () => ({ n: 1, s: "x" }))
      .build();
    const r = createWorkflowRunner({ storage });
    await r.run({ workflow: wf, workflowId: "l", input: {} });
    expect(await r.handle("l").result()).toEqual({ n: 1, s: "x" });
  });
});
