// ---------------------------------------------------------------------------
// createSchedulerTools — verifies the background subscription delivers ticks
// for schedules the LLM registers after the tools were created.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryScheduler, type ScheduleTick } from "@promin/workflow/scheduler";
import { FakeWallClock } from "@promin/workflow";
import { createSchedulerTools } from "../scheduler-tools.ts";

describe("createSchedulerTools", () => {
  it("delivers ticks for schedules registered after the tools were created", async () => {
    const scheduler = new InMemoryScheduler();
    const fired: Array<{ task: string; tick: ScheduleTick }> = [];
    let resolveFirst!: () => void;
    const firstTick = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });

    const tools = createSchedulerTools({
      scheduler,
      onTick: (task, tick) => {
        fired.push({ task, tick });
        resolveFirst();
      },
    });

    const reply = await tools.scheduleTask.execute({ id: "ping", task: "say hi", intervalMs: 20 });
    expect(reply).toBe('Scheduled "ping": every 20ms');

    await firstTick;
    await scheduler.unregister({ scheduleId: "ping" });

    expect(fired[0]!.task).toBe("say hi");
    expect(fired[0]!.tick.scheduleId).toBe("ping");
  });

  it("ignores ticks without a string task in metadata", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const scheduler = new InMemoryScheduler({ clock });
    const tasks: string[] = [];
    createSchedulerTools({ scheduler, onTick: (task) => tasks.push(task) });

    // "raw" carries no task; "tagged" is the control that proves ticks flow.
    await scheduler.register({ id: "raw", intervalMs: 10, metadata: { other: 1 } });
    await scheduler.register({ id: "tagged", intervalMs: 10, metadata: { task: "t" } });
    for (let i = 0; i < 200 && tasks.length < 3; i++) {
      for (let j = 0; j < 20; j++) await new Promise<void>((r) => setImmediate(r));
      clock.advance(10);
    }
    await scheduler.unregister({ scheduleId: "raw" });
    await scheduler.unregister({ scheduleId: "tagged" });

    // Both schedules fired every 10ms; only the tagged one reached onTick.
    expect(tasks.length).toBeGreaterThanOrEqual(3);
    expect(new Set(tasks)).toEqual(new Set(["t"]));
  });
});
