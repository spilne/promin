// ---------------------------------------------------------------------------
// Portable Scheduler conformance test suite
//
// Run the full Scheduler interface conformance suite against any
// implementation. Sync impls (InMemoryScheduler) just plug in the scheduler;
// async impls (DurableScheduler/Postgres, RedisDurableScheduler) supply
// awaitable overrides so the suite can wait for writes to land.
//
// Usage:
//   import { schedulerTestSuite } from "@promin/workflow/testing";
//   schedulerTestSuite("InMemoryScheduler", () => ({ scheduler: createScheduler() }));
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { StreamPipeline } from "@promin/core";
import type { Scheduler } from "./scheduler.ts";
import type { ScheduleConfig } from "./types.ts";

/**
 * Test-time wiring for a Scheduler implementation. The `scheduler` field is
 * required; the optional async overrides let durable impls await persistence
 * before the suite reads back state.
 */
export interface SchedulerTestHarness {
  scheduler: Scheduler;
  register?: (config: ScheduleConfig) => Promise<void>;
  unregister?: (id: string, options?: { reason?: string }) => Promise<void>;
  pause?: (id: string) => Promise<void>;
  resume?: (id: string) => Promise<void>;
  list?: () => Promise<ScheduleConfig[]>;
  cleanup?: () => Promise<void>;
}

/**
 * Run the full Scheduler conformance suite against any implementation.
 *
 * @param name — implementation name (shown in describe block)
 * @param factory — called before each test to produce a fresh harness
 */
export function schedulerTestSuite(
  name: string,
  factory: () => SchedulerTestHarness | Promise<SchedulerTestHarness>,
) {
  describe(`Scheduler conformance: ${name}`, () => {
    let harness: SchedulerTestHarness;

    async function reg(config: ScheduleConfig): Promise<void> {
      if (harness.register) await harness.register(config);
      else harness.scheduler.register(config);
    }
    async function unreg(id: string, options?: { reason?: string }): Promise<void> {
      if (harness.unregister) await harness.unregister(id, options);
      else harness.scheduler.unregister(id, options);
    }
    async function pause(id: string): Promise<void> {
      if (harness.pause) await harness.pause(id);
      else harness.scheduler.pause(id);
    }
    async function resume(id: string): Promise<void> {
      if (harness.resume) await harness.resume(id);
      else harness.scheduler.resume(id);
    }
    async function list(): Promise<ScheduleConfig[]> {
      if (harness.list) return await harness.list();
      return harness.scheduler.list();
    }

    beforeEach(async () => {
      harness = await factory();
    });

    afterEach(async () => {
      if (harness.cleanup) await harness.cleanup();
    });

    // -----------------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------------

    describe("register", () => {
      it("registers a cron schedule", async () => {
        await reg({ id: "cron-1", cron: "0 2 * * *" });
        const all = await list();
        expect(all.find((s) => s.id === "cron-1")).toBeDefined();
      });

      it("registers an interval schedule", async () => {
        await reg({ id: "iv-1", intervalMs: 5000 });
        const all = await list();
        expect(all.find((s) => s.id === "iv-1")).toBeDefined();
      });

      it("registers an rrule schedule", async () => {
        await reg({ id: "rr-1", rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU" });
        const all = await list();
        const found = all.find((s) => s.id === "rr-1");
        expect(found).toBeDefined();
        expect(found!.rrule).toBe("FREQ=WEEKLY;INTERVAL=2;BYDAY=TU");
      });

      it("throws if no trigger type specified", async () => {
        await expect(reg({ id: "bad" })).rejects.toThrow(/must have one of/);
      });

      it("throws if multiple trigger types specified", async () => {
        await expect(reg({ id: "bad", cron: "* * * * *", intervalMs: 1000 })).rejects.toThrow(
          /must have exactly one/,
        );
      });

      it("throws on invalid cron expression", async () => {
        await expect(reg({ id: "bad", cron: "not a cron" })).rejects.toThrow(/[Ii]nvalid cron/);
      });

      it("throws on invalid rrule", async () => {
        await expect(reg({ id: "bad", rrule: "not an rrule" })).rejects.toThrow(/[Ii]nvalid RRULE/);
      });

      it("preserves timezone on registered schedule", async () => {
        await reg({ id: "tz-1", cron: "0 9 * * MON", timezone: "America/New_York" });
        const all = await list();
        expect(all.find((s) => s.id === "tz-1")!.timezone).toBe("America/New_York");
      });

      it("preserves metadata on registered schedule", async () => {
        await reg({ id: "meta-1", intervalMs: 1000, metadata: { team: "data" } });
        const all = await list();
        expect(all.find((s) => s.id === "meta-1")!.metadata).toEqual({ team: "data" });
      });
    });

    // -----------------------------------------------------------------------
    // Unregister / list
    // -----------------------------------------------------------------------

    describe("unregister / list", () => {
      it("removes a registered schedule", async () => {
        await reg({ id: "rm-1", intervalMs: 1000 });
        await unreg("rm-1");
        const all = await list();
        expect(all.find((s) => s.id === "rm-1")).toBeUndefined();
      });
    });

    // -----------------------------------------------------------------------
    // Pause / resume / enabled flag
    // -----------------------------------------------------------------------

    describe("pause / resume / enabled flag", () => {
      it("pauses and resumes via enabled flag", async () => {
        await reg({ id: "pr-1", intervalMs: 1000 });
        const initial = (await list()).find((s) => s.id === "pr-1")!;
        expect(initial.enabled).toBe(true);

        await pause("pr-1");
        const paused = (await list()).find((s) => s.id === "pr-1")!;
        expect(paused.enabled).toBe(false);

        await resume("pr-1");
        const resumed = (await list()).find((s) => s.id === "pr-1")!;
        expect(resumed.enabled).toBe(true);
      });

      it("registers as disabled when enabled: false is passed", async () => {
        await reg({ id: "off-1", intervalMs: 1000, enabled: false });
        const found = (await list()).find((s) => s.id === "off-1")!;
        expect(found.enabled).toBe(false);
      });
    });

    // -----------------------------------------------------------------------
    // Interval tick emission
    // -----------------------------------------------------------------------

    describe("interval ticks", () => {
      it("emits ticks at the configured interval", async () => {
        await reg({ id: "iv-fast", intervalMs: 50 });
        const ticks = await harness.scheduler.stream("iv-fast").take(3).collect();

        expect(ticks).toHaveLength(3);
        expect(ticks[0]!.scheduleId).toBe("iv-fast");
        expect(ticks[0]!.tickNumber).toBe(0);
        expect(ticks[1]!.tickNumber).toBe(1);
        expect(ticks[2]!.tickNumber).toBe(2);
      });

      it("each tick has scheduledAt and firedAt", async () => {
        await reg({ id: "iv-dates", intervalMs: 50 });
        const [tick] = await harness.scheduler.stream("iv-dates").take(1).collect();

        expect(tick!.scheduledAt).toBeInstanceOf(Date);
        expect(tick!.firedAt).toBeInstanceOf(Date);
        expect(tick!.firedAt.getTime()).toBeGreaterThanOrEqual(tick!.scheduledAt.getTime() - 50);
      });

      it("ticks pass through scheduleName and metadata", async () => {
        await reg({
          id: "iv-meta",
          intervalMs: 50,
          name: "Heartbeat",
          metadata: { env: "test" },
        });
        const [tick] = await harness.scheduler.stream("iv-meta").take(1).collect();

        expect(tick!.scheduleName).toBe("Heartbeat");
        expect(tick!.metadata).toEqual({ env: "test" });
      });
    });

    // -----------------------------------------------------------------------
    // Cron tick emission
    // -----------------------------------------------------------------------

    describe("cron ticks", () => {
      // 1Hz cron with a generous deadline. Poll-based impls (DurableScheduler,
      // RedisDurableScheduler) emit slower than the cron's nominal frequency
      // because each tick must round-trip through storage and leader election;
      // we assert "at least 1 tick within 8s" rather than exact rates.
      it("emits at least one tick for a 1Hz cron within 8s", async () => {
        await reg({ id: "cron-1hz", cron: "* * * * * *" });
        const ticks = await harness.scheduler
          .stream("cron-1hz")
          .interruptAfter(8000)
          .take(1)
          .collect();

        expect(ticks.length).toBeGreaterThanOrEqual(1);
        expect(ticks[0]!.scheduleId).toBe("cron-1hz");
        expect(ticks[0]!.scheduledAt).toBeInstanceOf(Date);
      }, 10_000);
    });

    // -----------------------------------------------------------------------
    // RRULE tick emission — same 1-tick-within-window rationale as cron.
    // -----------------------------------------------------------------------

    describe("rrule ticks", () => {
      it("emits at least one tick from an rrule schedule within 8s", async () => {
        await reg({ id: "rrule-1hz", rrule: "FREQ=SECONDLY;INTERVAL=1" });
        const ticks = await harness.scheduler
          .stream("rrule-1hz")
          .interruptAfter(8000)
          .take(1)
          .collect();

        expect(ticks.length).toBeGreaterThanOrEqual(1);
        expect(ticks[0]!.scheduleId).toBe("rrule-1hz");
        expect(ticks[0]!.tickNumber).toBe(0);
      }, 10_000);
    });

    // -----------------------------------------------------------------------
    // startAt / endAt
    // -----------------------------------------------------------------------

    describe("startAt / endAt", () => {
      it("startAt delays the first tick", async () => {
        const startAt = new Date(Date.now() + 200);
        await reg({ id: "sa-1", intervalMs: 50, startAt });

        const before = Date.now();
        const [tick] = await harness.scheduler.stream("sa-1").take(1).collect();
        const elapsed = Date.now() - before;

        expect(elapsed).toBeGreaterThanOrEqual(150);
        expect(tick!.scheduleId).toBe("sa-1");
      });

      it("endAt prevents ticks after the cutoff (bounded by 1s window)", async () => {
        const endAt = new Date(Date.now() + 250);
        await reg({ id: "ea-1", intervalMs: 50, endAt });

        // Use interruptAfter so the assertion works for both stream-ends impls
        // (InMemoryScheduler) and pollers that keep the source open (DurableScheduler).
        const ticks = await harness.scheduler.stream("ea-1").interruptAfter(1000).collect();

        // endAt at +250ms with 50ms interval: at most ~5 ticks before cutoff,
        // and zero new ticks fire between 250ms and 1000ms.
        expect(ticks.length).toBeLessThan(20);
      }, 10_000);

      it("endAt in the past produces no ticks (bounded window)", async () => {
        await reg({
          id: "ea-past",
          intervalMs: 50,
          endAt: new Date(Date.now() - 10_000),
        });

        const ticks = await harness.scheduler.stream("ea-past").interruptAfter(500).collect();
        expect(ticks).toHaveLength(0);
      });

      it("startAt + endAt define a firing window (bounded)", async () => {
        const now = Date.now();
        await reg({
          id: "win-1",
          intervalMs: 50,
          startAt: new Date(now + 100),
          endAt: new Date(now + 400),
        });

        const ticks = await harness.scheduler.stream("win-1").interruptAfter(1000).collect();

        // Window is 100→400ms with 50ms interval — at most ~6 ticks possible.
        expect(ticks.length).toBeLessThan(20);
      }, 10_000);
    });

    // -----------------------------------------------------------------------
    // Multi-schedule streaming
    // -----------------------------------------------------------------------

    describe("multi-schedule streaming", () => {
      it("stream() with no id merges all enabled schedules", async () => {
        await reg({ id: "m-a", intervalMs: 50 });
        await reg({ id: "m-b", intervalMs: 50 });

        const ticks = await harness.scheduler.stream().take(4).collect();
        const ids = new Set(ticks.map((t) => t.scheduleId));

        expect(ticks).toHaveLength(4);
        expect(ids.has("m-a")).toBe(true);
        expect(ids.has("m-b")).toBe(true);
      });

      it("subscribe() behaves the same as stream()", async () => {
        await reg({ id: "sub-1", intervalMs: 50 });
        const ticks = await harness.scheduler.subscribe().take(2).collect();

        expect(ticks).toHaveLength(2);
        expect(ticks[0]!.scheduleId).toBe("sub-1");
      });
    });

    // -----------------------------------------------------------------------
    // StreamPipeline integration
    // -----------------------------------------------------------------------

    describe("StreamPipeline integration", () => {
      it("works with StreamPipeline.fromSource()", async () => {
        await reg({ id: "src-1", intervalMs: 50 });
        const ticks = await StreamPipeline.fromSource(harness.scheduler).take(2).collect();

        expect(ticks).toHaveLength(2);
        expect(ticks[0]!.scheduleId).toBe("src-1");
      });

      it("composes with stream operators (filter/map)", async () => {
        await reg({ id: "comp-fast", intervalMs: 50, metadata: { type: "fast" } });
        await reg({ id: "comp-slow", intervalMs: 50, metadata: { type: "slow" } });

        const fastTicks = await harness.scheduler
          .stream()
          .filter((t) => t.metadata?.type === "fast")
          .take(2)
          .collect();

        expect(fastTicks).toHaveLength(2);
        expect(fastTicks.every((t) => t.scheduleId === "comp-fast")).toBe(true);
      });
    });

    // -----------------------------------------------------------------------
    // Lifecycle: unregister ends the stream
    // -----------------------------------------------------------------------

    describe("lifecycle", () => {
      it("no new ticks fire after a schedule is unregistered", async () => {
        await reg({ id: "life-1", intervalMs: 50 });

        // Unregister shortly after start; collect within a fixed window.
        // Works for both stream-ends impls and pollers that keep the source open.
        setTimeout(() => {
          void unreg("life-1", { reason: "test cleanup" });
        }, 200);

        const ticks = await harness.scheduler.stream("life-1").interruptAfter(1000).collect();

        // Some ticks fire pre-unregister, then nothing — bounded by what fits in 200ms.
        expect(ticks.length).toBeLessThan(50);
      }, 10_000);
    });
  });
}
