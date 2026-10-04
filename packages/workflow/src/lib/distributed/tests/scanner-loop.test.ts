import { describe, it, expect } from "bun:test";
import { DefaultSleepScanner } from "../sleep-scanner.ts";
import { DefaultSignalScanner } from "../signal-scanner.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import type { WorkflowRunner } from "../../durable/workflow-runner.ts";
import type { WorkflowStorage } from "../../durable/workflow-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

const noRunner = { run: async () => undefined } as unknown as WorkflowRunner;

function countingStorage(clock: FakeWallClock): { storage: WorkflowStorage; scans: () => number } {
  const storage = new InMemoryWorkflowStorage({ clock });
  let scans = 0;
  // Each scan issues one scanner query (an empty store fits on one page).
  const dueTimers = storage.listDueTimers.bind(storage);
  storage.listDueTimers = (params) => {
    scans++;
    return dueTimers(params);
  };
  const signalWakeups = storage.listSignalWakeups.bind(storage);
  storage.listSignalWakeups = (params) => {
    scans++;
    return signalWakeups(params);
  };
  return { storage, scans: () => scans };
}

describe("scanner loops on the shared PollLoop", () => {
  for (const [name, Scanner] of [
    ["sleep scanner", DefaultSleepScanner],
    ["signal scanner", DefaultSignalScanner],
  ] as const) {
    it(`${name}: stop() cancels the wait at once and leaves no timer behind`, async () => {
      const clock = FakeWallClock.create(0);
      const { storage, scans } = countingStorage(clock);
      const scanner = new Scanner({
        storage,
        runner: noRunner,
        scanIntervalMs: 60_000,
        resolveWorkflow: () => undefined,
        clock,
      });

      const running = scanner.start();
      await waitFor(() => scans() === 1 && clock.pendingCount() === 1);
      clock.advance(60_000);
      await waitFor(() => scans() === 2 && clock.pendingCount() === 1);

      await scanner.stop();
      await running;
      expect(clock.pendingCount()).toBe(0);
      expect(scans()).toBe(2);
    });
  }
});
