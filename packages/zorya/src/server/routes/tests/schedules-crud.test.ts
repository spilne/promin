// ---------------------------------------------------------------------------
// Schedule CRUD through `ZoryaServer.handle`: create-only POST (201 / 409),
// namespaces the scheduler polls, a PATCH that changes every mutable field
// and recomputes the next run, DELETE 204 / 404, and an emit that answers
// with the tick it fired.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { sync } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { InMemorySchedulerStorage } from "@promin/workflow/scheduler";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import { ZoryaScheduler, type ZoryaSchedulerConfig } from "../../services/scheduler/index.ts";
import type { ApiError } from "../../api-types.ts";
import type { ScheduleDto, ScheduleEmitResponse } from "../schedules.ts";

const KEY = "operator-key-0123456789";

function setup(
  params: {
    scheduler?: Pick<ZoryaSchedulerConfig, "namespace" | "namespaces">;
    apiKeys?: string[];
  } = {},
) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runs = { executions: 0 };
  const hello = workflow<unknown>({ name: "hello" })
    .step("body", () => sync(() => ++runs.executions))
    .build() as Workflow<unknown, unknown>;
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: { hello },
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const schedStorage = new InMemorySchedulerStorage({ clock });
  const scheduler = new ZoryaScheduler({
    storage: schedStorage,
    workflows,
    clock,
    ...params.scheduler,
    onError: () => {},
  });
  const server = new ZoryaServer({
    workflows,
    scheduler,
    clock,
    ...(params.apiKeys ? { apiKeys: params.apiKeys } : { insecure: true }),
  });
  const auth: Record<string, string> = params.apiKeys
    ? { authorization: `Bearer ${params.apiKeys[0]}` }
    : {};
  const send = (method: string, path: string, body?: unknown) =>
    server.handle(
      new Request(`http://x${path}`, {
        method,
        headers: { ...auth, "content-type": "application/json" },
        ...(body !== undefined && { body: JSON.stringify(body) }),
      }),
    );
  return { clock, storage, schedStorage, server, send, runs };
}

async function dto(res: Response): Promise<ScheduleDto> {
  return (await res.json()) as ScheduleDto;
}

describe("POST /api/schedules — create-only", () => {
  it("creates with 201 and refuses an existing id with 409, leaving it unchanged", async () => {
    const { send, schedStorage } = setup();
    const created = await send("POST", "/api/schedules", {
      id: "s1",
      intervalMs: 60_000,
      workflowName: "hello",
    });
    expect(created.status).toBe(201);
    expect((await dto(created)).intervalMs).toBe(60_000);

    const again = await send("POST", "/api/schedules", { id: "s1", cron: "0 * * * *" });
    expect(again.status).toBe(409);
    expect(((await again.json()) as ApiError).error).toBe("schedule_exists");
    const stored = await schedStorage.loadSchedule("s1");
    expect([stored?.intervalMs, stored?.cron]).toEqual([60_000, undefined]);
  });

  it("refuses a namespace no scheduler here polls", async () => {
    const { send, schedStorage } = setup();
    const res = await send("POST", "/api/schedules", {
      id: "s1",
      intervalMs: 60_000,
      namespace: "tenant-a",
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as ApiError).error).toBe("namespace_not_served");
    expect(await schedStorage.loadSchedule("s1")).toBeNull();
  });

  it("accepts the namespaces the scheduler polls and defaults to its own", async () => {
    const multi = setup({ scheduler: { namespaces: [undefined, "tenant-a"] } });
    for (const namespace of [undefined, "tenant-a"]) {
      const res = await multi.send("POST", "/api/schedules", {
        id: `s-${namespace ?? "global"}`,
        intervalMs: 60_000,
        ...(namespace !== undefined && { namespace }),
      });
      expect(res.status).toBe(201);
    }
    expect(
      (await multi.send("POST", "/api/schedules", { id: "x", intervalMs: 1, namespace: "b" }))
        .status,
    ).toBe(400);

    const single = setup({ scheduler: { namespace: "tenant-b" } });
    const res = await single.send("POST", "/api/schedules", { id: "s", intervalMs: 60_000 });
    expect(res.status).toBe(201);
    expect((await dto(res)).namespace).toBe("tenant-b");
    expect(
      (await single.send("POST", "/api/schedules", { id: "g", intervalMs: 1, namespace: "x" }))
        .status,
    ).toBe(400);
  });

  it("validates trigger, timezone and unknown fields", async () => {
    const { send, schedStorage } = setup();
    for (const body of [
      { id: "s1" },
      { id: "s1", cron: "0 * * * *", intervalMs: 1000 },
      { id: "s1", cron: "not a cron" },
      { id: "s1", cron: "0 * * * *", timezone: "Not/AZone" },
      { id: "s1", intervalMs: 1000, surprise: true },
    ]) {
      expect((await send("POST", "/api/schedules", body)).status).toBe(400);
    }
    expect(await schedStorage.loadSchedule("s1")).toBeNull();
  });
});

describe("PATCH /api/schedules/:id", () => {
  it("changes the trigger and recomputes the next run", async () => {
    const { send, clock } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000 });
    clock.advance(10_000);

    const res = await send("PATCH", "/api/schedules/s1", {
      cron: "0 6 * * *",
      timezone: "Europe/Kyiv",
      name: "morning",
    });
    expect(res.status).toBe(200);
    const patched = await dto(res);
    expect(patched.cron).toBe("0 6 * * *");
    expect(patched.intervalMs).toBeUndefined();
    expect(patched.timezone).toBe("Europe/Kyiv");
    expect(patched.name).toBe("morning");
    // 06:00 Kyiv is 04:00 UTC in winter.
    expect(patched.nextRunAt).toBe("2026-01-01T04:00:00.000Z");
  });

  it("clears optional fields with null and merges metadata", async () => {
    const { send } = setup();
    await send("POST", "/api/schedules", {
      id: "s1",
      cron: "0 * * * *",
      timezone: "UTC",
      jitterMs: 5,
      name: "n",
      workflowName: "hello",
      input: { a: 1 },
      metadata: { team: "core", keep: true },
    });
    const res = await send("PATCH", "/api/schedules/s1", {
      timezone: null,
      jitterMs: null,
      name: null,
      input: { a: 2 },
      metadata: { team: null, owner: "ops" },
    });
    expect(res.status).toBe(200);
    const patched = await dto(res);
    expect(patched.timezone).toBeUndefined();
    expect(patched.jitterMs).toBeUndefined();
    expect(patched.name).toBeUndefined();
    expect(patched.metadata).toEqual({
      keep: true,
      owner: "ops",
      workflowName: "hello",
      input: { a: 2 },
    });
  });

  it("pauses and resumes", async () => {
    const { send, schedStorage } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000 });
    const paused = await dto(await send("PATCH", "/api/schedules/s1", { enabled: false }));
    expect(paused.enabled).toBe(false);
    expect(paused.nextRunAt).toBeUndefined();
    expect((await schedStorage.loadSchedule("s1"))?.enabled).toBe(false);

    const resumed = await dto(await send("PATCH", "/api/schedules/s1", { enabled: true }));
    expect(resumed.enabled).toBe(true);
    expect((await schedStorage.loadSchedule("s1"))?.enabled).toBe(true);
  });

  it("rejects an invalid result and leaves the schedule unchanged", async () => {
    const { send, schedStorage } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000 });
    for (const body of [
      { cron: "garbage" },
      { cron: "0 * * * *", intervalMs: 1000 },
      { timezone: "Mars/Olympus" },
      { startAt: "2027-01-01T00:00:00Z", endAt: "2026-01-01T00:00:00Z" },
      { id: "other" },
      { namespace: "x" },
      { intervalMs: -1 },
    ]) {
      const res = await send("PATCH", "/api/schedules/s1", body);
      expect(res.status).toBe(400);
    }
    const stored = await schedStorage.loadSchedule("s1");
    expect([stored?.intervalMs, stored?.cron, stored?.timezone]).toEqual([
      60_000,
      undefined,
      undefined,
    ]);
  });

  it("is a 404 for an unknown schedule", async () => {
    const { send } = setup();
    expect((await send("PATCH", "/api/schedules/nope", { enabled: false })).status).toBe(404);
  });
});

describe("DELETE /api/schedules/:id", () => {
  it("answers 204, then 404", async () => {
    const { send, schedStorage } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000 });
    const res = await send("DELETE", "/api/schedules/s1");
    expect(res.status).toBe(204);
    expect(await res.text()).toBe("");
    expect(await schedStorage.loadSchedule("s1")).toBeNull();
    expect((await send("DELETE", "/api/schedules/s1")).status).toBe(404);
  });
});

describe("POST /api/schedules/:id/emit", () => {
  it("answers 202 with the tick and the run it started", async () => {
    const { send, storage } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000, workflowName: "hello" });
    const res = await send("POST", "/api/schedules/s1/emit");
    expect(res.status).toBe(202);
    const body = (await res.json()) as ScheduleEmitResponse;
    expect(body.scheduleId).toBe("s1");
    expect(body.tickNumber).toBe(0);
    expect(res.headers.get("location")).toBe(`/api/runs/${encodeURIComponent(body.workflowId)}`);
    expect((await storage.loadWorkflow(body.workflowId))?.workflowName).toBe("hello");

    const next = (await (await send("POST", "/api/schedules/s1/emit")).json()) as {
      tickNumber: number;
    };
    expect(next.tickNumber).toBe(1);
  });

  it("is a 409 for a paused schedule and a 502 when the run can't start", async () => {
    const { send } = setup();
    await send("POST", "/api/schedules", {
      id: "paused",
      intervalMs: 60_000,
      workflowName: "hello",
      enabled: false,
    });
    const paused = await send("POST", "/api/schedules/paused/emit");
    expect(paused.status).toBe(409);
    expect(((await paused.json()) as ApiError).error).toBe("schedule_paused");

    await send("POST", "/api/schedules", { id: "ghost", intervalMs: 60_000, workflowName: "nope" });
    const failed = await send("POST", "/api/schedules/ghost/emit");
    expect(failed.status).toBe(502);
    expect(((await failed.json()) as ApiError).error).toBe("dispatch_failed");
  });
});

describe("schedules whose workflow can't be dispatched", () => {
  it("GET /api/schedules shows the dispatch error until a fire goes through", async () => {
    const { send, server, storage, clock } = setup();
    await send("POST", "/api/schedules", { id: "ghost", intervalMs: 60_000, workflowName: "nope" });

    expect(await server.scheduler!.tickOnce()).toEqual([]);
    const listed = (await (await send("GET", "/api/schedules")).json()) as {
      schedules: ScheduleDto[];
    };
    const ghost = listed.schedules.find((s) => s.id === "ghost")!;
    expect(ghost.lastError?.message).toContain('Unknown workflow "nope"');
    expect(ghost.lastError?.at).toBe(clock.now().toISOString());
    expect(ghost.tickCount).toBe(0);
    expect((await dto(await send("GET", "/api/schedules/ghost"))).lastError).toEqual(
      ghost.lastError,
    );

    // Pointing it at a real workflow clears the error; the next fire runs.
    const fixed = await dto(await send("PATCH", "/api/schedules/ghost", { workflowName: "hello" }));
    expect(fixed.lastError).toBeUndefined();
    clock.advance(60_000);
    const fired = await server.scheduler!.tickOnce();
    expect(fired.map((t) => t.tickNumber)).toEqual([0]);
    const after = await dto(await send("GET", "/api/schedules/ghost"));
    expect(after.lastError).toBeUndefined();
    expect(after.tickCount).toBe(1);
    const runId = `ghost.${after.incarnation}.0`;
    expect((await storage.loadWorkflow(runId))?.workflowName).toBe("hello");
  });

  it("a failed emit is recorded too", async () => {
    const { send } = setup();
    await send("POST", "/api/schedules", { id: "ghost", intervalMs: 60_000, workflowName: "nope" });
    expect((await send("POST", "/api/schedules/ghost/emit")).status).toBe(502);
    const ghost = await dto(await send("GET", "/api/schedules/ghost"));
    expect(ghost.lastError?.message).toContain("nope");
  });
});

describe("schedules deleted and created again", () => {
  it("the new schedule's ticks start new runs instead of answering the old ones", async () => {
    const { send, storage, runs } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000, workflowName: "hello" });
    const first = (await (
      await send("POST", "/api/schedules/s1/emit")
    ).json()) as ScheduleEmitResponse;
    const oldIncarnation = (await dto(await send("GET", "/api/schedules/s1"))).incarnation;

    expect((await send("DELETE", "/api/schedules/s1")).status).toBe(204);
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000, workflowName: "hello" });
    const recreated = await dto(await send("GET", "/api/schedules/s1"));
    expect(recreated.incarnation).not.toBe(oldIncarnation);
    expect(recreated.tickCount).toBe(0);

    const second = (await (
      await send("POST", "/api/schedules/s1/emit")
    ).json()) as ScheduleEmitResponse;
    expect(second.tickNumber).toBe(0);
    expect(second.workflowId).not.toBe(first.workflowId);
    expect(second.workflowId).toBe(`s1.${recreated.incarnation}.0`);
    for (let i = 0; i < 200 && runs.executions < 2; i++) await new Promise((r) => setTimeout(r, 0));
    expect(runs.executions).toBe(2);
    expect((await storage.loadWorkflow(second.workflowId))?.runSourceId).toBe("s1");
  });
});

describe("schedule routes validate their query strings", () => {
  it("history and upcoming reject out-of-range paging", async () => {
    const { send } = setup();
    await send("POST", "/api/schedules", { id: "s1", intervalMs: 60_000 });
    for (const path of [
      "/api/schedules/s1/history?limit=0",
      "/api/schedules/s1/history?offset=-1",
      "/api/schedules/s1/upcoming?count=101",
      "/api/schedules?enabled=maybe",
      "/api/schedules?metadata=nope",
    ]) {
      expect((await send("GET", path)).status).toBe(400);
    }
    expect((await send("GET", "/api/schedules/s1/upcoming?count=100")).status).toBe(200);
  });
});

describe("schedules through auth", () => {
  it("every schedule route needs the operator key", async () => {
    const { server } = setup({ apiKeys: [KEY] });
    for (const [method, path] of [
      ["POST", "/api/schedules"],
      ["PATCH", "/api/schedules/s1"],
      ["DELETE", "/api/schedules/s1"],
      ["POST", "/api/schedules/s1/emit"],
      ["GET", "/api/schedules/s1/history"],
    ] as const) {
      expect((await server.handle(new Request(`http://x${path}`, { method }))).status).toBe(401);
    }
    const { send } = setup({ apiKeys: [KEY] });
    expect((await send("POST", "/api/schedules", { id: "s1", intervalMs: 1000 })).status).toBe(201);
  });
});
