// ---------------------------------------------------------------------------
// createSchedulerTools — verifies the background subscription delivers ticks
// for schedules the LLM registers after the tools were created.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryScheduler, type ScheduleTick } from "@promin/workflow/scheduler";
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
    const scheduler = new InMemoryScheduler();
    const tasks: string[] = [];
    createSchedulerTools({ scheduler, onTick: (task) => tasks.push(task) });

    await scheduler.register({ id: "raw", intervalMs: 10, metadata: { other: 1 } });
    await new Promise((resolve) => setTimeout(resolve, 60));
    await scheduler.unregister({ scheduleId: "raw" });

    expect(tasks).toEqual([]);
  });
});
