// ---------------------------------------------------------------------------
// SchedulerLoop delivery on a FakeWallClock: a tick is committed only after
// its dispatch started the run; a failed or timed-out dispatch is reported
// and redelivered; the lease is renewed across a slow poll; a redelivered
// tick never runs a workflow body twice.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { sync } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { InMemorySchedulerStorage, type ScheduleTick } from "@promin/workflow/scheduler";
import {
  ScheduleDispatchError,
  ScheduleDispatchTimeoutError,
  SchedulerLoop,
  type SchedulerLoopErrorEvent,
} from "../scheduler-loop.ts";
import { LocalWorkflows } from "../workflows/index.ts";
import { emitScheduleNow } from "../../routes/schedules.ts";

const T0 = Date.parse("2026-01-01T00:00:00Z");

/** Yield to the event loop until `predicate` holds, bounded by iterations. */
async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

/** A promise plus its resolver, to hold a dispatch open. */
function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((r) => (open = r));
  return { promise, open };
}

async function setup() {
  const clock = FakeWallClock.create(T0);
  const storage = new InMemorySchedulerStorage({ clock });
  const errors: SchedulerLoopErrorEvent[] = [];
  return { clock, storage, errors, onError: (e: SchedulerLoopErrorEvent) => void errors.push(e) };
}

async function due(params: {
  storage: InMemorySchedulerStorage;
  id: string;
  intervalMs?: number;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  await params.storage.upsertSchedule({
    id: params.id,
    intervalMs: params.intervalMs ?? 60_000,
    metadata: params.metadata ?? { workflowName: "wf" },
  });
}

const tickCount = async (storage: InMemorySchedulerStorage, id: string) =>
  (await storage.loadScheduleState(id))?.tickCount;

describe("SchedulerLoop — commit only dispatched ticks", () => {
  it("a failed dispatch is reported, not committed, and redelivered with the same tickNumber", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "nightly" });
    const runId = `nightly.${(await storage.loadScheduleState("nightly"))!.incarnation}.0`;
    const calls: Array<string | undefined> = [];
    let failing = true;
    const loop = new SchedulerLoop({
      storage,
      clock,
      onError,
      trigger: async (_name, _input, opts) => {
        calls.push(opts?.workflowId);
        if (failing) throw new Error("storage down");
        return { workflowId: opts!.workflowId! };
      },
    });

    expect(await loop.tickOnce()).toEqual([]);
    expect(errors.map((e) => [e.phase, e.scheduleId, e.tickNumber])).toEqual([
      ["dispatch", "nightly", 0],
    ]);
    expect(await tickCount(storage, "nightly")).toBe(0);
    // The failure is recorded on the schedule, where the API shows it.
    expect((await storage.loadScheduleState("nightly"))?.lastError).toEqual({
      message: "storage down",
      at: clock.now(),
    });

    // Backing off: the next poll leaves it alone.
    expect(await loop.tickOnce()).toEqual([]);
    expect(calls).toEqual([runId]);

    failing = false;
    clock.advance(2_000); // 2 × pollIntervalMs after the first failure
    const fired = await loop.tickOnce();
    expect(fired.map((t) => [t.scheduleId, t.tickNumber])).toEqual([["nightly", 0]]);
    expect(calls).toEqual([runId, runId]);
    // The fire that went through cleared it.
    expect((await storage.loadScheduleState("nightly"))?.lastError).toBeNull();
    expect(await tickCount(storage, "nightly")).toBe(1);
    expect(errors).toHaveLength(1);
  });

  it("one schedule's failure doesn't hold back the others in the poll", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "bad", metadata: { workflowName: "missing" } });
    await due({ storage, id: "good" });
    const loop = new SchedulerLoop({
      storage,
      clock,
      onError,
      trigger: async (name, _input, opts) => {
        if (name === "missing") throw new Error(`Unknown workflow "${name}"`);
        return { workflowId: opts!.workflowId! };
      },
    });

    expect((await loop.tickOnce()).map((t) => t.scheduleId)).toEqual(["good"]);
    expect(await tickCount(storage, "good")).toBe(1);
    expect(await tickCount(storage, "bad")).toBe(0);
    expect(errors.map((e) => [e.phase, e.scheduleId])).toEqual([["dispatch", "bad"]]);
  });

  it("a dispatch past its deadline is reported and its schedule stays due; the rest commits", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "hung" });
    await due({ storage, id: "ok" });
    const hung = gate();
    const started: string[] = [];
    const loop = new SchedulerLoop({
      storage,
      clock,
      onError,
      dispatchTimeoutMs: 5_000,
      leaderLockTtlMs: 60_000,
      fire: async (tick) => {
        started.push(tick.scheduleId);
        if (tick.scheduleId === "hung") await hung.promise;
      },
    });

    const poll = loop.tickOnce();
    await until(() => started.length === 2);
    clock.advance(5_000);
    const fired = await poll;

    expect(fired.map((t) => t.scheduleId)).toEqual(["ok"]);
    expect(errors.map((e) => [e.phase, e.scheduleId])).toEqual([["dispatch", "hung"]]);
    expect(errors[0]!.error).toBeInstanceOf(ScheduleDispatchTimeoutError);
    expect(await tickCount(storage, "ok")).toBe(1);
    expect(await tickCount(storage, "hung")).toBe(0);
    expect(clock.pendingCount()).toBe(0);
    hung.open();
  });

  it("fireOnce reports a failed dispatch to its caller and the emit route answers 502", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "m" });
    const loop = new SchedulerLoop({
      storage,
      clock,
      onError,
      trigger: async () => {
        throw new Error("no worker advertises wf");
      },
    });

    await expect(loop.fireOnce("m")).rejects.toBeInstanceOf(ScheduleDispatchError);
    expect(errors.map((e) => [e.phase, e.scheduleId, e.tickNumber])).toEqual([
      ["dispatch", "m", 0],
    ]);

    const res = await emitScheduleNow(storage, (id) => loop.fireOnce(id))(
      new Request("http://x/", { method: "POST" }),
      { id: "m" },
    );
    expect(res.status).toBe(502);
    expect(((await res.json()) as { error: string }).error).toBe("dispatch_failed");
  });
});

describe("SchedulerLoop — leader lease across a slow poll", () => {
  function contender(params: {
    storage: InMemorySchedulerStorage;
    clock: FakeWallClock;
    instanceId: string;
    fires: string[];
    hold?: Promise<void>;
    onError?: (e: SchedulerLoopErrorEvent) => void;
  }) {
    return new SchedulerLoop({
      storage: params.storage,
      clock: params.clock,
      instanceId: params.instanceId,
      pollIntervalMs: 1_000, // lease TTL 3 s, renewed every second
      dispatchTimeoutMs: 60_000,
      dispatchConcurrency: 1,
      onError: params.onError,
      fire: async (tick: ScheduleTick) => {
        params.fires.push(`${params.instanceId}:${tick.scheduleId}.${tick.tickNumber}`);
        if (params.hold) await params.hold;
      },
    });
  }

  it("the lease is renewed while dispatch runs, so another instance can't fire the same tick", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "digest", intervalMs: 3_600_000 });
    let renewals = 0;
    const acquire = storage.tryAcquireLeader.bind(storage);
    storage.tryAcquireLeader = (p) => {
      if (p.instanceId === "A") renewals++;
      return acquire(p);
    };
    const fires: string[] = [];
    const slow = gate();
    const a = contender({ storage, clock, instanceId: "A", fires, hold: slow.promise, onError });
    const b = contender({ storage, clock, instanceId: "B", fires, onError });

    const aPoll = a.tickOnce();
    await until(() => fires.length === 1);
    // Ten seconds of dispatch: more than three lease TTLs.
    for (let s = 0; s < 10; s++) {
      const before = renewals;
      clock.advance(1_000);
      await until(() => renewals > before);
    }
    expect(await b.tickOnce()).toEqual([]);

    slow.open();
    expect((await aPoll).map((t) => t.tickNumber)).toEqual([0]);
    expect(fires).toEqual(["A:digest.0"]);
    expect(await tickCount(storage, "digest")).toBe(1);
    expect(errors).toEqual([]);
  });

  it("a poll that loses its lease stops dispatching; undelivered schedules stay due", async () => {
    const { clock, storage, onError } = await setup();
    await due({ storage, id: "x" });
    await due({ storage, id: "y" });
    let leading = true;
    const acquire = storage.tryAcquireLeader.bind(storage);
    storage.tryAcquireLeader = async (p) => (leading ? acquire(p) : null);
    const fires: string[] = [];
    const slow = gate();
    const a = contender({ storage, clock, instanceId: "A", fires, hold: slow.promise, onError });

    const poll = a.tickOnce();
    await until(() => fires.length === 1);
    leading = false;
    clock.advance(1_000); // the renewal finds the lease gone
    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
    slow.open();
    const fired = await poll;

    expect(fired).toHaveLength(1);
    expect(fires).toHaveLength(1);
    const [delivered] = fires[0]!.slice(2).split(".");
    const undelivered = delivered === "x" ? "y" : "x";
    expect(await tickCount(storage, delivered!)).toBe(1);
    expect(await tickCount(storage, undelivered)).toBe(0);
  });
});

describe("SchedulerLoop — redelivered ticks", () => {
  it("a tick redelivered after a failed commit doesn't run the workflow body again", async () => {
    const { clock, storage, errors, onError } = await setup();
    await due({ storage, id: "charge-nightly", metadata: { workflowName: "charge" } });
    let executions = 0;
    const charge = workflow<unknown>({ name: "charge" })
      .step("charge-card", () => sync(() => ++executions))
      .build() as Workflow<unknown, unknown>;
    const wfStorage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage: wfStorage,
      runner: createWorkflowRunner({ storage: wfStorage }),
      definitions: { charge },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const commit = storage.commitPoll.bind(storage);
    let failCommit = true;
    storage.commitPoll = async (params) => {
      if (failCommit) {
        failCommit = false;
        throw new Error("commit blip");
      }
      return commit(params);
    };
    const loop = new SchedulerLoop({
      storage,
      clock,
      onError,
      trigger: (name, input, opts) => workflows.trigger(name, input, opts),
    });

    const runId = `charge-nightly.${(await storage.loadScheduleState("charge-nightly"))!.incarnation}.0`;
    await loop.tickOnce();
    await until(async () => (await wfStorage.loadWorkflow(runId))?.status === "completed");
    await loop.tickOnce(); // same tick, delivered again
    for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));

    expect(errors.map((e) => e.phase)).toEqual(["commit"]);
    expect(executions).toBe(1);
    const run = await wfStorage.loadWorkflow(runId);
    expect([run?.run, run?.status, run?.runSource]).toEqual([1, "completed", "schedule"]);
    expect(await tickCount(storage, "charge-nightly")).toBe(1);
  });
});
