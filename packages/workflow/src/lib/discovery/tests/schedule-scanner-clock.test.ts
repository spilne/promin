// ---------------------------------------------------------------------------
// applyDiscoveredSchedules({ kickstart }) seeds nextRun from the injected
// clock, so it lines up with a scheduler driven by the same clock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { applyDiscoveredSchedules } from "../schedule-scanner.ts";
import { InMemorySchedulerStorage } from "../../scheduler/in-memory-scheduler-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Records every setNextRun write so the test can read the seeded time. */
class RecordingSchedulerStorage extends InMemorySchedulerStorage {
  readonly nextRunWrites: { id: string; nextRun: Date | null }[] = [];
  override async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    this.nextRunWrites.push({ id, nextRun });
    await super.setNextRun(id, nextRun);
  }
}

describe("applyDiscoveredSchedules — kickstart on an injected clock", () => {
  it("seeds nextRun for newly added schedules at the clock's now", async () => {
    const clock = FakeWallClock.create("2026-03-01T12:00:00Z");
    const storage = new RecordingSchedulerStorage({ clock });

    const result = await applyDiscoveredSchedules(
      storage,
      [
        { id: "nightly", cron: "0 0 * * *" },
        { id: "paused", intervalMs: 60_000, enabled: false },
      ],
      { kickstart: true, clock },
    );

    expect(result.added).toEqual(["nightly", "paused"]);
    // Disabled schedules aren't kickstarted.
    expect(storage.nextRunWrites).toEqual([
      { id: "nightly", nextRun: new Date("2026-03-01T12:00:00Z") },
    ]);
    expect(await storage.findDue({ now: clock.now(), limit: 10 })).toEqual(["nightly"]);
  });
});
