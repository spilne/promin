import { describe, expect, it } from "bun:test";
import { frameTask } from "../frame-task.ts";

describe("frameTask", () => {
  it("returns the bare task when source is omitted", () => {
    expect(frameTask({ task: "Summarise this." })).toBe("Summarise this.");
  });

  it("returns the bare task for a user source (live conversation)", () => {
    expect(frameTask({ task: "Hi there", source: { kind: "user" } })).toBe("Hi there");
  });

  it("prepends a scheduled-trigger header with ISO timestamp", () => {
    const firedAt = new Date("2026-04-28T14:00:00Z");
    expect(frameTask({ task: "Check Twitter", source: { kind: "scheduled", firedAt } })).toBe(
      "[Scheduled trigger at 2026-04-28T14:00:00.000Z] Check Twitter",
    );
  });

  it("includes the schedule id when present", () => {
    const firedAt = new Date("2026-04-28T14:00:00Z");
    expect(
      frameTask({
        task: "Run weekly report",
        source: { kind: "scheduled", firedAt, scheduleId: "weekly-1" },
      }),
    ).toBe("[Scheduled trigger weekly-1 at 2026-04-28T14:00:00.000Z] Run weekly report");
  });

  it("prepends a webhook header with origin", () => {
    const receivedAt = new Date("2026-04-28T14:00:00Z");
    expect(
      frameTask({
        task: "Process this PR",
        source: { kind: "webhook", receivedAt, origin: "github" },
      }),
    ).toBe("[Webhook event from github at 2026-04-28T14:00:00.000Z] Process this PR");
  });

  it("prepends a webhook header without origin", () => {
    const receivedAt = new Date("2026-04-28T14:00:00Z");
    expect(frameTask({ task: "X", source: { kind: "webhook", receivedAt } })).toBe(
      "[Webhook event at 2026-04-28T14:00:00.000Z] X",
    );
  });

  it("prepends a delegation header for peer-agent calls", () => {
    expect(
      frameTask({
        task: "Look up the docs",
        source: { kind: "agent", callerAgentId: "researcher" },
      }),
    ).toBe("[Delegated by agent researcher] Look up the docs");
  });
});
