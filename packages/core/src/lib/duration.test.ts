import { describe, it, expect } from "bun:test";
import { Duration, resolveMs } from "./duration.ts";

describe("Duration", () => {
  it("factory methods", () => {
    expect(Duration.millis(500).ms).toBe(500);
    expect(Duration.seconds(10).ms).toBe(10_000);
    expect(Duration.minutes(5).ms).toBe(300_000);
    expect(Duration.hours(2).ms).toBe(7_200_000);
    expect(Duration.days(1).ms).toBe(86_400_000);
    expect(Duration.weeks(1).ms).toBe(604_800_000);
  });

  it("parse string", () => {
    expect(Duration.parse("500ms").ms).toBe(500);
    expect(Duration.parse("10s").ms).toBe(10_000);
    expect(Duration.parse("5m").ms).toBe(300_000);
    expect(Duration.parse("2h").ms).toBe(7_200_000);
    expect(Duration.parse("1d").ms).toBe(86_400_000);
    expect(Duration.parse("1w").ms).toBe(604_800_000);
  });

  it("parse throws on invalid", () => {
    expect(() => Duration.parse("abc")).toThrow();
    expect(() => Duration.parse("5x")).toThrow();
  });

  it("from accepts number, string, Duration", () => {
    expect(Duration.from(1000).ms).toBe(1000);
    expect(Duration.from("5s").ms).toBe(5000);
    expect(Duration.from(Duration.hours(1)).ms).toBe(3_600_000);
  });

  it("conversions", () => {
    const d = Duration.hours(1).plus(Duration.minutes(30));
    expect(d.toMinutes()).toBe(90);
    expect(d.toSeconds()).toBe(5400);
    expect(d.toHours()).toBe(1.5);
  });

  it("arithmetic", () => {
    expect(Duration.hours(1).plus(Duration.minutes(30)).ms).toBe(5_400_000);
    expect(Duration.hours(2).minus(Duration.hours(1)).ms).toBe(3_600_000);
    expect(Duration.seconds(10).times(3).ms).toBe(30_000);
  });

  it("comparison", () => {
    expect(Duration.hours(1).gt(Duration.minutes(30))).toBe(true);
    expect(Duration.hours(1).lt(Duration.hours(2))).toBe(true);
    expect(Duration.seconds(60).eq(Duration.minutes(1))).toBe(true);
  });

  it("toString", () => {
    expect(Duration.millis(500).toString()).toBe("500ms");
    expect(Duration.seconds(30).toString()).toBe("30s");
    expect(Duration.minutes(5).toString()).toBe("5m");
    expect(Duration.hours(2).toString()).toBe("2h");
    expect(Duration.days(3).toString()).toBe("3d");
  });

  it("resolveMs", () => {
    expect(resolveMs(1000)).toBe(1000);
    expect(resolveMs("5s")).toBe(5000);
    expect(resolveMs(Duration.hours(1))).toBe(3_600_000);
  });
});
