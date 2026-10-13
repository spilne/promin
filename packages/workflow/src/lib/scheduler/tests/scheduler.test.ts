import { describe, it, expect } from "bun:test";
import { createScheduler } from "../in-memory-scheduler.ts";
import { scheduleTickRunId } from "../types.ts";

// ---------------------------------------------------------------------------
// scheduleTickRunId — shared id format
// ---------------------------------------------------------------------------

describe("scheduleTickRunId", () => {
  it("formats `${scheduleId}.${tickNumber}` so a duplicate fire collides on the same run row", () => {
    expect(scheduleTickRunId("daily-summary", 0)).toBe("daily-summary.0");
    expect(scheduleTickRunId("daily-summary", 42)).toBe("daily-summary.42");
    // Same inputs → same output: this is the property both the
    // workflow-trigger and agent-dispatch paths rely on for idempotency.
    expect(scheduleTickRunId("x", 7)).toBe(scheduleTickRunId("x", 7));
  });
});

// ---------------------------------------------------------------------------
// InMemoryScheduler — construction & config
// ---------------------------------------------------------------------------

describe("InMemoryScheduler", () => {
  describe("register", () => {
    it("registers a cron schedule", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "daily", cron: "0 2 * * *" });
      expect(await scheduler.list()).toHaveLength(1);
      expect((await scheduler.list())[0]!.id).toBe("daily");
    });

    it("registers an interval schedule", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "heartbeat", intervalMs: 5000 });
      expect(await scheduler.list()).toHaveLength(1);
    });

    it("throws if no trigger type specified", async () => {
      const scheduler = createScheduler();
      await expect(scheduler.register({ id: "bad" })).rejects.toThrow("must have one of");
    });

    it("throws if multiple trigger types specified", async () => {
      const scheduler = createScheduler();
      await expect(
        scheduler.register({ id: "bad", cron: "* * * * *", intervalMs: 1000 }),
      ).rejects.toThrow("must have exactly one");
    });

    it("throws on invalid cron expression", async () => {
      const scheduler = createScheduler();
      await expect(scheduler.register({ id: "bad", cron: "not a cron" })).rejects.toThrow(
        "Invalid cron",
      );
    });

    it("registers with rrule", async () => {
      const scheduler = createScheduler();
      await scheduler.register({
        id: "biweekly",
        rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
      });
      const list = await scheduler.list();
      expect(list[0]!.rrule).toBe("FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10");
    });

    it("throws on invalid rrule", async () => {
      const scheduler = createScheduler();
      await expect(scheduler.register({ id: "bad", rrule: "not a rrule" })).rejects.toThrow(
        "Invalid RRULE",
      );
    });

    it("throws if rrule + cron both specified", async () => {
      const scheduler = createScheduler();
      await expect(
        scheduler.register({ id: "bad", cron: "* * * * *", rrule: "FREQ=DAILY" }),
      ).rejects.toThrow("must have exactly one");
    });

    it("registers with timezone", async () => {
      const scheduler = createScheduler();
      await scheduler.register({
        id: "est",
        cron: "0 9 * * MON",
        timezone: "America/New_York",
      });
      expect((await scheduler.list())[0]!.timezone).toBe("America/New_York");
    });

    it("registers with metadata", async () => {
      const scheduler = createScheduler();
      await scheduler.register({
        id: "meta",
        intervalMs: 1000,
        metadata: { team: "data" },
      });
      expect((await scheduler.list())[0]!.metadata).toEqual({ team: "data" });
    });
  });

  describe("unregister", () => {
    it("removes a schedule", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "x", intervalMs: 1000 });
      await scheduler.unregister("x");
      expect(await scheduler.list()).toHaveLength(0);
    });
  });

  describe("pause / resume", () => {
    it("pauses and resumes", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "p", intervalMs: 1000 });
      expect((await scheduler.list())[0]!.enabled).toBe(true);

      await scheduler.pause("p");
      expect((await scheduler.list())[0]!.enabled).toBe(false);

      await scheduler.resume("p");
      expect((await scheduler.list())[0]!.enabled).toBe(true);
    });

    it("registers as disabled", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "off", intervalMs: 1000, enabled: false });
      expect((await scheduler.list())[0]!.enabled).toBe(false);
    });
  });

  // ---------------------------------------------------------------------------
  // Stream emission — interval-based
  // ---------------------------------------------------------------------------

  describe("interval stream", () => {
    it("emits ticks at interval", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "fast", intervalMs: 50 });

      const ticks = await scheduler.stream("fast").take(3).toArray().run();

      expect(ticks).toHaveLength(3);
      expect(ticks[0]!.scheduleId).toBe("fast");
      expect(ticks[0]!.tickNumber).toBe(0);
      expect(ticks[1]!.tickNumber).toBe(1);
      expect(ticks[2]!.tickNumber).toBe(2);
    });

    it("tick includes scheduledAt and firedAt", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "dates", intervalMs: 50 });

      const [tick] = await scheduler.stream("dates").take(1).toArray().run();

      expect(tick!.scheduledAt).toBeInstanceOf(Date);
      expect(tick!.firedAt).toBeInstanceOf(Date);
      expect(tick!.firedAt.getTime()).toBeGreaterThanOrEqual(tick!.scheduledAt.getTime() - 10);
    });

    it("tick includes metadata from config", async () => {
      const scheduler = createScheduler();
      await scheduler.register({
        id: "meta",
        intervalMs: 50,
        name: "Heartbeat",
        metadata: { env: "test" },
      });

      const [tick] = await scheduler.stream("meta").take(1).toArray().run();

      expect(tick!.scheduleName).toBe("Heartbeat");
      expect(tick!.metadata).toEqual({ env: "test" });
    });

    it("respects timing between ticks", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "timed", intervalMs: 100 });

      const start = Date.now();
      await scheduler.stream("timed").take(3).toArray().run();
      const elapsed = Date.now() - start;

      expect(elapsed).toBeGreaterThanOrEqual(250); // 3 ticks * ~100ms
    });
  });

  // ---------------------------------------------------------------------------
  // Stream emission — cron-based
  // ---------------------------------------------------------------------------

  describe("cron stream", () => {
    it("emits on cron match (every second)", async () => {
      const scheduler = createScheduler();
      // Every second — should fire quickly
      await scheduler.register({ id: "every-sec", cron: "* * * * * *" }); // 6-field with seconds

      const ticks = await scheduler.stream("every-sec").take(2).toArray().run();

      expect(ticks).toHaveLength(2);
      expect(ticks[0]!.scheduleId).toBe("every-sec");
      expect(ticks[0]!.scheduledAt).toBeInstanceOf(Date);
    }, 10_000);
  });

  // ---------------------------------------------------------------------------
  // Multiple schedules
  // ---------------------------------------------------------------------------

  describe("multiple schedules", () => {
    it("stream() without id merges all schedules", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "a", intervalMs: 50 });
      await scheduler.register({ id: "b", intervalMs: 50 });

      const ticks = await scheduler.stream().take(4).toArray().run();

      expect(ticks).toHaveLength(4);
      const ids = new Set(ticks.map((t) => t.scheduleId));
      expect(ids.has("a")).toBe(true);
      expect(ids.has("b")).toBe(true);
    });

    it("subscribe() is same as stream()", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "sub", intervalMs: 50 });

      const ticks = await scheduler.subscribe().take(2).toArray().run();

      expect(ticks).toHaveLength(2);
      expect(ticks[0]!.scheduleId).toBe("sub");
    });
  });

  // ---------------------------------------------------------------------------
  // Stream integration
  // ---------------------------------------------------------------------------

  describe("Stream integration", () => {
    it("subscribe() builds a fresh stream on every call", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "src", intervalMs: 50 });

      const first = await scheduler.subscribe().take(2).toArray().run();
      const second = await scheduler.subscribe().take(2).toArray().run();

      expect(first.map((t) => t.tickNumber)).toEqual([0, 1]);
      expect(second.map((t) => t.tickNumber)).toEqual([0, 1]);
    });

    it("picks up schedules registered after subscribe() started", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "early", intervalMs: 50 });
      setTimeout(() => void scheduler.register({ id: "late", intervalMs: 20 }), 30);

      const ticks = await scheduler
        .subscribe()
        .filter((t) => t.scheduleId === "late")
        .take(1)
        .toArray()
        .run();

      expect(ticks[0]!.scheduleId).toBe("late");
    });

    it("composes with stream operators", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "a", intervalMs: 50, metadata: { type: "fast" } });
      await scheduler.register({ id: "b", intervalMs: 50, metadata: { type: "slow" } });

      const fastTicks = await scheduler
        .stream()
        .filter((t) => t.metadata?.type === "fast")
        .take(2)
        .toArray()
        .run();

      expect(fastTicks).toHaveLength(2);
      expect(fastTicks.every((t) => t.scheduleId === "a")).toBe(true);
    });

    it("maps ticks to workflow input", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "etl", intervalMs: 50 });

      const inputs = await scheduler
        .stream("etl")
        .take(2)
        .map((tick) => ({
          date: tick.scheduledAt.toISOString().split("T")[0],
          tickNumber: tick.tickNumber,
        }))
        .toArray()
        .run();

      expect(inputs).toHaveLength(2);
      expect(inputs[0]!.tickNumber).toBe(0);
      expect(inputs[0]!.date).toBeTruthy();
    });
  });

  // ---------------------------------------------------------------------------
  // RRULE stream
  // ---------------------------------------------------------------------------

  describe("rrule stream", () => {
    it("emits ticks from rrule schedule", async () => {
      const scheduler = createScheduler();
      // Every second (for test speed)
      await scheduler.register({
        id: "rrule-fast",
        rrule: "FREQ=SECONDLY;INTERVAL=1",
      });

      const ticks = await scheduler.stream("rrule-fast").take(2).toArray().run();

      expect(ticks).toHaveLength(2);
      expect(ticks[0]!.scheduleId).toBe("rrule-fast");
      expect(ticks[0]!.tickNumber).toBe(0);
      expect(ticks[1]!.tickNumber).toBe(1);
    }, 10_000);
  });

  // ---------------------------------------------------------------------------
  // startAt / endAt
  // ---------------------------------------------------------------------------

  describe("startAt / endAt", () => {
    it("startAt delays first tick until the specified time", async () => {
      const scheduler = createScheduler();
      const startAt = new Date(Date.now() + 200);
      await scheduler.register({ id: "delayed", intervalMs: 50, startAt });

      const before = Date.now();
      const [tick] = await scheduler.stream("delayed").take(1).toArray().run();
      const elapsed = Date.now() - before;

      expect(elapsed).toBeGreaterThanOrEqual(150);
      expect(tick!.scheduleId).toBe("delayed");
    });

    it("endAt stops the stream after the specified time", async () => {
      const scheduler = createScheduler();
      const endAt = new Date(Date.now() + 200);
      await scheduler.register({ id: "expiring", intervalMs: 50, endAt });

      const ticks = await scheduler.stream("expiring").toArray().run();

      expect(ticks.length).toBeGreaterThanOrEqual(1);
      expect(ticks.length).toBeLessThan(10);
    });

    it("endAt in the past produces no ticks", async () => {
      const scheduler = createScheduler();
      await scheduler.register({
        id: "expired",
        intervalMs: 50,
        endAt: new Date(Date.now() - 1000),
      });

      const ticks = await scheduler.stream("expired").toArray().run();
      expect(ticks).toHaveLength(0);
    });

    it("startAt + endAt together define a window", async () => {
      const scheduler = createScheduler();
      const now = Date.now();
      await scheduler.register({
        id: "windowed",
        intervalMs: 50,
        startAt: new Date(now + 100),
        endAt: new Date(now + 400),
      });

      const before = Date.now();
      const ticks = await scheduler.stream("windowed").toArray().run();
      const elapsed = Date.now() - before;

      expect(elapsed).toBeGreaterThanOrEqual(100);
      expect(ticks.length).toBeGreaterThanOrEqual(1);
      expect(ticks.length).toBeLessThan(10);
    });
  });

  // ---------------------------------------------------------------------------
  // Unregister ends stream
  // ---------------------------------------------------------------------------

  describe("unregister ends stream", () => {
    it("stream ends when schedule is unregistered", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "temp", intervalMs: 50 });

      // Unregister after a short delay
      setTimeout(() => void scheduler.unregister("temp"), 200);

      const ticks = await scheduler.stream("temp").toArray().run();

      // Should have collected some ticks before ending
      expect(ticks.length).toBeGreaterThanOrEqual(1);
      expect(ticks.length).toBeLessThan(100);
    });
  });

  // ---------------------------------------------------------------------------
  // Cleanup when the consumer stops
  // ---------------------------------------------------------------------------

  describe("cleanup on consumer stop", () => {
    // The register-callback set is private; bracket access keeps the check
    // honest without widening the public surface.
    const registerCallbacks = (s: ReturnType<typeof createScheduler>) => s["_registerCallbacks"];

    it("take(n) removes the register callback", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "a", intervalMs: 20 });

      let sizeWhileRunning = -1;
      const ticks = await scheduler
        .subscribe()
        .tap(() => {
          sizeWhileRunning = registerCallbacks(scheduler).size;
        })
        .take(2)
        .toArray()
        .run();

      expect(ticks).toHaveLength(2);
      expect(sizeWhileRunning).toBe(1);
      expect(registerCallbacks(scheduler).size).toBe(0);
    });

    it("breaking out of for-await removes the register callback", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "a", intervalMs: 20 });

      for await (const tick of scheduler.subscribe().toAsyncIterable()) {
        expect(tick.scheduleId).toBe("a");
        expect(registerCallbacks(scheduler).size).toBe(1);
        break;
      }

      expect(registerCallbacks(scheduler).size).toBe(0);
    });

    it("interrupts a pending sleep instead of waiting for the next fire time", async () => {
      const scheduler = createScheduler();
      await scheduler.register({ id: "slow", intervalMs: 60_000 });

      const start = Date.now();
      const ticks = await scheduler.subscribe().interruptAfter(50).toArray().run();

      expect(ticks).toHaveLength(0);
      expect(Date.now() - start).toBeLessThan(5_000);
      expect(registerCallbacks(scheduler).size).toBe(0);
    });
  });
});
