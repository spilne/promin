// ---------------------------------------------------------------------------
// Schedule routes read "now" from the injected clock — the create-time
// nextRun seed, the DTO's nextRunAt projection and the upcoming-ticks list.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemorySchedulerStorage } from "@promin/workflow";
import {
  createSchedule,
  getSchedule,
  getScheduleUpcoming,
  type ScheduleDto,
  type ScheduleUpcomingResponse,
} from "../schedules.ts";

/** Records every setNextRun write so the test can read the seeded time. */
class RecordingSchedulerStorage extends InMemorySchedulerStorage {
  readonly nextRunWrites: { id: string; nextRun: Date | null }[] = [];
  override async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    this.nextRunWrites.push({ id, nextRun });
    await super.setNextRun(id, nextRun);
  }
}

async function create(
  deps: { storage: InMemorySchedulerStorage; clock: FakeWallClock },
  body: unknown,
): Promise<ScheduleDto> {
  const res = await createSchedule(deps)(
    new Request("http://x/api/schedules", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
  expect(res.status).toBe(200);
  return (await res.json()) as ScheduleDto;
}

describe("schedule routes — time on an injected clock", () => {
  it("createSchedule seeds nextRun at the clock's now and projects nextRunAt from it", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:10Z");
    const storage = new RecordingSchedulerStorage({ clock });

    const dto = await create({ storage, clock }, { id: "every-minute", intervalMs: 60_000 });

    expect(storage.nextRunWrites).toEqual([
      { id: "every-minute", nextRun: new Date("2026-01-01T00:00:10Z") },
    ]);
    expect(dto.nextRunAt).toBe("2026-01-01T00:01:10.000Z");

    clock.advance(30_000);
    const res = await getSchedule({ storage, clock })(new Request("http://x"), {
      id: "every-minute",
    });
    expect(((await res.json()) as ScheduleDto).nextRunAt).toBe("2026-01-01T00:01:40.000Z");
  });

  it("getScheduleUpcoming steps forward from the clock's now", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemorySchedulerStorage({ clock });
    await create({ storage, clock }, { id: "hourly", cron: "0 * * * *", timezone: "UTC" });

    clock.advance(90 * 60_000); // 01:30
    const res = await getScheduleUpcoming({ storage, clock })(
      new Request("http://x/api/schedules/hourly/upcoming?count=3"),
      { id: "hourly" },
    );
    const body = (await res.json()) as ScheduleUpcomingResponse;
    expect(body.upcoming.map((u) => u.scheduledAt)).toEqual([
      "2026-01-01T02:00:00.000Z",
      "2026-01-01T03:00:00.000Z",
      "2026-01-01T04:00:00.000Z",
    ]);
    expect(clock.pendingCount()).toBe(0);
  });
});
