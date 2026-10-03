// ---------------------------------------------------------------------------
// Builder step kinds read time from the runner's clock — `.sleep` wake time,
// `.waitForSignal` timeout and `.dowhile` iteration timing all follow a
// FakeWallClock handed to `createWorkflowRunner`, with no real waits.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00.000Z";

function setup() {
  const clock = FakeWallClock.create(T0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

describe(".sleep() — wake time follows the runner clock", () => {
  it("stamps wakeAt from the clock and stays asleep until the clock passes it", async () => {
    const { clock, storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "clock-sleep" })
        .sleep("nap", 60_000)
        .step("after", () => succeed("woke"))
        .build();

    const first = await runner.runSafe({ workflow: build(), workflowId: "cs-1", input: 1 });
    expect(first.error?._tag).toBe("WorkflowSuspendedError");
    const state = await storage.loadWorkflow("cs-1");
    expect(state?.steps["nap"]?.wakeAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    // One ms short of the wake time — still sleeping.
    clock.advance(59_999);
    const early = await runner.runSafe({ workflow: build(), workflowId: "cs-1", input: 1 });
    expect(early.error?._tag).toBe("WorkflowSuspendedError");

    clock.advance(1);
    const done = await runner.runSafe({ workflow: build(), workflowId: "cs-1", input: 1 });
    expect(done.error).toBeNull();
    expect(done.data).toBe("woke");
  });
});

describe(".waitForSignal() — timeout follows the runner clock", () => {
  it("stamps signalTimeoutAt from the clock and times out once the clock reaches it", async () => {
    const { clock, storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "clock-signal" })
        .waitForSignal<string>("approval", { signalName: "approve", timeoutMs: 5_000 })
        .build();

    const first = await runner.runSafe({ workflow: build(), workflowId: "csig-1", input: 1 });
    expect(first.error?._tag).toBe("WorkflowSuspendedError");
    const state = await storage.loadWorkflow("csig-1");
    expect(state?.steps["approval"]?.signalTimeoutAt?.toISOString()).toBe(
      "2026-01-01T00:00:05.000Z",
    );

    // No re-entry before the deadline here: a pre-deadline re-entry re-arms
    // the timeout from the re-entry time, so jump straight to the deadline.
    clock.advance(5_000);
    const late = await runner.runSafe({ workflow: build(), workflowId: "csig-1", input: 1 });
    expect(late.error?._tag).toBe("WorkflowTimeoutError");
  });
});

describe(".dowhile() — iteration rows are timed on the runner clock", () => {
  it("records each iteration's startedAt and durationMs from the clock", async () => {
    const { clock, storage, runner } = setup();
    const wf = workflow<number>({ name: "clock-loop" })
      .dowhile(
        "spin",
        (_ctx, iter) => {
          // Each iteration "takes" 250ms of fake time.
          clock.advance(250);
          return iter + 1;
        },
        (n) => n < 2,
      )
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "cl-1", input: 0 });
    expect(result).toBe(2);

    const state = await storage.loadWorkflow("cl-1");
    const iter0 = state?.steps["spin.iter.0"];
    const iter1 = state?.steps["spin.iter.1"];
    expect(iter0?.durationMs).toBe(250);
    expect(iter1?.durationMs).toBe(250);
    expect(iter0?.startedAt?.toISOString()).toBe("2026-01-01T00:00:00.000Z");
    expect(iter1?.startedAt?.toISOString()).toBe("2026-01-01T00:00:00.250Z");
  });
});
