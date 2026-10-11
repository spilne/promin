// ---------------------------------------------------------------------------
// Untrusted numbers and dates are validated before they are stored, and the
// read routes survive a row that was stored invalid before validation
// existed. Exercised through `ZoryaServer.handle` with in-memory storage.
// ---------------------------------------------------------------------------

import { beforeEach, describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { InMemorySchedulerStorage } from "@promin/workflow/scheduler";
import { LocalWorkflows } from "../../services/workflows/local-workflows.ts";
import { ZoryaScheduler } from "../../services/scheduler/index.ts";
import { ZoryaServer } from "../../server.ts";
import type { ScheduleDto, SchedulesResponse } from "../schedules.ts";
import { DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS, type SignalTokenDto } from "../signal-tokens.ts";

const WF = "wf-1";

function post(path: string, body: string): Request {
  return new Request(`http://x${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
}

describe("signal-token mint bounds", () => {
  let storage: InMemoryWorkflowStorage;
  let server: ZoryaServer;
  let clock: FakeWallClock;

  beforeEach(async () => {
    clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: WF, workflowName: "approve", input: {} });
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    server = new ZoryaServer({ insecure: true, workflows, clock, signalTokenMaxTtlMs: 60_000 });
  });

  const mint = (body: string) => server.handle(post(`/api/runs/${WF}/signals/go/token`, body));

  for (const [label, raw] of [
    ["1e400 (Infinity)", "1e400"],
    ["-1e400", "-1e400"],
    ["zero", "0"],
    ["a negative number", "-5"],
    ["a fraction", "1.5"],
    ["a string", '"60000"'],
    ["null", "null"],
    ["over the configured cap", "60001"],
  ] as const) {
    it(`rejects expiresInMs = ${label} with 400 and stores nothing`, async () => {
      const res = await mint(`{"expiresInMs":${raw}}`);
      expect(res.status).toBe(400);
      expect(((await res.json()) as { error: string }).error).toBe("invalid_expires_in_ms");
      expect(await storage.listSignalTokensForWorkflow(WF)).toHaveLength(0);
    });
  }

  it("accepts the cap itself", async () => {
    const res = await mint(`{"expiresInMs":60000}`);
    expect(res.status).toBe(201);
    expect(((await res.json()) as { expiresAt: string }).expiresAt).toBe(
      "2026-01-01T00:01:00.000Z",
    );
  });

  it("rejects malformed tags and idempotency keys", async () => {
    expect((await mint(`{"expiresInMs":1000,"tags":"x"}`)).status).toBe(400);
    expect((await mint(`{"expiresInMs":1000,"tags":[1]}`)).status).toBe(400);
    expect((await mint(`{"expiresInMs":1000,"idempotencyKey":5}`)).status).toBe(400);
  });

  it("caps at 90 days by default", () => {
    expect(DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS).toBe(90 * 24 * 60 * 60 * 1000);
  });

  it("lists, describes and refuses a token stored with an invalid expiry", async () => {
    // A row persisted before mint validation existed.
    await storage.createSignalToken({
      tokenId: "bad",
      workflowId: WF,
      signalName: "go",
      bearer: "b".repeat(43),
      tags: [],
      idempotencyKey: null,
      expiresAt: new Date(Number.NaN),
    });

    const list = await server.handle(new Request(`http://x/api/runs/${WF}/signal-tokens`));
    expect(list.status).toBe(200);
    const { tokens } = (await list.json()) as { tokens: SignalTokenDto[] };
    expect(tokens).toHaveLength(1);
    expect(tokens[0]!.expiresAt).toBeNull();

    const auth = { authorization: `Bearer ${"b".repeat(43)}` };
    const described = await server.handle(
      new Request("http://x/api/signal-tokens/bad/describe", { headers: auth }),
    );
    expect(described.status).toBe(200);
    expect(((await described.json()) as { expiresAt: unknown }).expiresAt).toBeNull();

    // An unreadable expiry is expired, never valid forever.
    const complete = await server.handle(
      new Request("http://x/api/signal-tokens/bad/complete", {
        method: "POST",
        headers: { ...auth, "content-type": "application/json" },
        body: JSON.stringify({ value: true }),
      }),
    );
    expect(complete.status).toBe(408);
    expect(await storage.loadSignals(WF)).toHaveLength(0);
  });
});

describe("schedule create validation", () => {
  let schedStorage: InMemorySchedulerStorage;
  let server: ZoryaServer;

  beforeEach(() => {
    const storage = new InMemoryWorkflowStorage();
    schedStorage = new InMemorySchedulerStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const scheduler = new ZoryaScheduler({ storage: schedStorage, workflows });
    server = new ZoryaServer({ insecure: true, workflows, scheduler });
  });

  const create = (body: unknown) => server.handle(post("/api/schedules", JSON.stringify(body)));

  for (const [label, body] of [
    ["a garbage startAt", { startAt: "garbage" }],
    ["a garbage endAt", { endAt: "not-a-date" }],
    ["a numeric startAt", { startAt: 5 }],
    ["startAt after endAt", { startAt: "2027-01-01T00:00:00Z", endAt: "2026-01-01T00:00:00Z" }],
    ["an unknown timezone", { timezone: "Mars/Olympus" }],
    ["a string jitterMs", { jitterMs: "10" }],
    ["a negative jitterMs", { jitterMs: -1 }],
    ["a fractional maxCatchUp", { maxCatchUp: 1.5 }],
    ["a non-object metadata", { metadata: "x" }],
  ] as const) {
    it(`rejects ${label} with 400 and stores nothing`, async () => {
      const res = await create({ id: "s1", intervalMs: 60_000, ...body });
      expect(res.status).toBe(400);
      expect(((await res.json()) as { error: string }).error).toBe("invalid_schedule");
      expect(await schedStorage.loadSchedule("s1")).toBeNull();
    });
  }

  it("rejects a non-finite or non-positive intervalMs", async () => {
    for (const intervalMs of [-1, 0, "60000"]) {
      const res = await create({ id: "s1", intervalMs });
      expect(res.status).toBe(400);
    }
    expect(await schedStorage.loadSchedule("s1")).toBeNull();
  });

  it("accepts a valid schedule and treats null / empty optionals as unset", async () => {
    const res = await create({
      id: "s1",
      cron: "0 * * * *",
      timezone: "Europe/Kyiv",
      startAt: "2026-01-01T00:00:00Z",
      name: "",
      endAt: null,
    });
    expect(res.status).toBe(200);
    const dto = (await res.json()) as ScheduleDto;
    expect(dto.startAt).toBe("2026-01-01T00:00:00.000Z");
    expect(dto.endAt).toBeUndefined();
    expect(dto.name).toBeUndefined();
  });

  it("lists and reads a schedule stored with invalid dates", async () => {
    // A row persisted before route validation existed.
    await schedStorage.upsertSchedule({
      id: "bad",
      intervalMs: 60_000,
      startAt: new Date("garbage"),
      endAt: new Date(Number.NaN),
    });

    const list = await server.handle(new Request("http://x/api/schedules"));
    expect(list.status).toBe(200);
    const body = (await list.json()) as SchedulesResponse;
    expect(body.schedules.map((s) => s.id)).toEqual(["bad"]);
    expect(body.schedules[0]!.startAt).toBeUndefined();

    const one = await server.handle(new Request("http://x/api/schedules/bad"));
    expect(one.status).toBe(200);

    const upcoming = await server.handle(new Request("http://x/api/schedules/bad/upcoming"));
    expect(upcoming.status).toBe(200);
  });

  it("clamps list limits instead of passing garbage to storage", async () => {
    const res = await server.handle(new Request("http://x/api/schedules?limit=-5&offset=1e400"));
    expect(res.status).toBe(200);
  });
});
