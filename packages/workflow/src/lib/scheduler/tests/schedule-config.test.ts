import { describe, expect, it } from "bun:test";
import { shortestFireGapMs } from "../schedule-config.ts";

const FROM = new Date("2026-03-02T00:00:00Z");

describe("shortestFireGapMs", () => {
  it("is the interval itself for interval schedules", () => {
    expect(shortestFireGapMs({ config: { intervalMs: 5_000 }, from: FROM })).toBe(5_000);
  });

  it("measures cron cadence, including a seconds field", () => {
    expect(shortestFireGapMs({ config: { cron: "0 * * * *" }, from: FROM })).toBe(3_600_000);
    expect(shortestFireGapMs({ config: { cron: "*/5 * * * * *" }, from: FROM })).toBe(5_000);
  });

  it("finds a burst inside an otherwise sparse cron", () => {
    // Two fires a minute apart, once a day.
    expect(shortestFireGapMs({ config: { cron: "0,1 9 * * *" }, from: FROM })).toBe(60_000);
  });

  it("measures RRULE cadence", () => {
    expect(
      shortestFireGapMs({
        config: { rrule: "DTSTART:20260301T000000Z\nRRULE:FREQ=SECONDLY;INTERVAL=10" },
        from: FROM,
      }),
    ).toBe(10_000);
    expect(
      shortestFireGapMs({
        config: { rrule: "DTSTART:20260301T090000Z\nRRULE:FREQ=DAILY" },
        from: FROM,
      }),
    ).toBe(86_400_000);
  });

  it("is null when the trigger fires fewer than twice", () => {
    expect(
      shortestFireGapMs({
        config: { rrule: "DTSTART:20260301T000000Z\nRRULE:FREQ=DAILY;COUNT=1" },
        from: FROM,
      }),
    ).toBeNull();
  });
});
