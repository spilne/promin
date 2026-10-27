// ---------------------------------------------------------------------------
// Zombie-worker test suite — runner-level fencing across two workers.
//
// Worker A runs a workflow and stalls inside a step past its lock lease.
// Worker B, on a second storage instance over the same backend, takes the
// run over and finishes it. Then A wakes up and carries on: every write it
// makes from then on must be rejected with `FenceTokenMismatchError`, and
// B's run must be what storage holds.
//
// Usage:
//   import { zombieWorkerTestSuite } from "@promin/workflow/testing";
//   zombieWorkerTestSuite({ createStorage, createPeer, expireLock });
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "./workflow-builder.ts";
import type { Workflow } from "./workflow-types.ts";
import { createWorkflowRunner } from "./workflow-runner.ts";
import { hasCapability } from "./storage/capabilities.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

export interface ZombieWorkerTestSuiteOptions {
  /** A fresh storage for one test. */
  readonly createStorage: () => WorkflowStorage | Promise<WorkflowStorage>;
  /**
   * A second instance over the same backend as `storage` — the worker that
   * takes the run over. Single-instance backends return `storage` itself.
   */
  readonly createPeer: (storage: WorkflowStorage) => WorkflowStorage | Promise<WorkflowStorage>;
  /**
   * Make the run lock on `workflowId` expire now, as if its holder stalled
   * past its lease without heartbeating (move the backend's clock past the
   * lease, or set the lock's expiry into the past).
   */
  readonly expireLock: (params: {
    readonly storage: WorkflowStorage;
    readonly workflowId: string;
  }) => Promise<void>;
}

/** Every storage write a run makes. Fenced ones must reject a stale holder. */
const WRITE_METHODS: ReadonlySet<string> = new Set([
  "saveStepResult",
  "checkpointStep",
  "batchSaveStepResults",
  "saveStepFailure",
  "saveTaskResult",
  "saveTaskFailure",
  "completeWorkflow",
  "failWorkflow",
  "tripwireWorkflow",
  "cancelWorkflow",
  "suspendWorkflow",
  "setWorkflowMetadata",
  "startFreshRun",
  "resetSteps",
  "deliverSignal",
  "appendStreamChunk",
  "createWorkflow",
  "saveStepAttempt",
  "appendEntry",
  "appendPendingEntry",
  "completePendingEntry",
  "discardJournalEntries",
]);

/**
 * Two full runs on a real backend, plus the wait for the stalled one: well
 * past bun's 5s default on a loaded machine. A timed-out test is abandoned
 * mid-flight, so the limit must sit above any realistic run.
 */
const TEST_TIMEOUT_MS = 30_000;

interface RecordedWrite {
  readonly method: string;
  /** `"ok"` when the write landed, else the error's `_tag` (or message). */
  readonly outcome: string;
}

/**
 * Wrap `storage` so every write made after `arm()` is recorded with its
 * outcome. Reads and lock calls pass through untouched.
 */
function recordWrites(storage: WorkflowStorage): {
  readonly storage: WorkflowStorage;
  readonly writes: RecordedWrite[];
  arm(): void;
} {
  const writes: RecordedWrite[] = [];
  let armed = false;
  const proxy = new Proxy(storage, {
    get(target, prop) {
      const value: unknown = Reflect.get(target, prop, target);
      if (typeof value !== "function") return value;
      const fn = value as (...args: unknown[]) => unknown;
      if (typeof prop !== "string" || !WRITE_METHODS.has(prop)) return fn.bind(target);
      return async (...args: unknown[]) => {
        if (!armed) return fn.apply(target, args);
        try {
          const result = await fn.apply(target, args);
          writes.push({ method: prop, outcome: "ok" });
          return result;
        } catch (err) {
          const tag = (err as { _tag?: unknown } | null)?._tag;
          writes.push({ method: prop, outcome: typeof tag === "string" ? tag : String(err) });
          throw err;
        }
      };
    },
  });
  return {
    storage: proxy,
    writes,
    arm: () => {
      armed = true;
    },
  };
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

async function waitFor(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!condition() && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 5));
  }
  if (!condition()) throw new Error("waitFor: condition never held");
}

/** What either worker can write for `workflowId`. */
async function snapshot(storage: WorkflowStorage, workflowId: string): Promise<unknown> {
  return {
    workflow: await storage.loadWorkflow(workflowId),
    journal: hasCapability(storage, "journal")
      ? await storage.loadJournal({ workflowId, stepName: "body" })
      : [],
    runs: await storage.loadRunHistory({ workflowId }),
  };
}

/** Run the zombie-worker scenarios against one backend. */
export function zombieWorkerTestSuite(options: ZombieWorkerTestSuiteOptions): void {
  describe("zombie worker (runner)", () => {
    /**
     * A runs `wf` until its first `work()` call blocks, the lock expires,
     * B runs the same workflow id to completion, then A is released.
     */
    async function stallThenTakeOver(params: {
      readonly workflowId: string;
      readonly build: (work: () => Promise<string>) => Workflow<unknown, unknown>;
    }): Promise<{
      readonly storage: WorkflowStorage;
      readonly zombie: { readonly ok: boolean; readonly error?: unknown };
      readonly writes: readonly RecordedWrite[];
      readonly takeoverResult: unknown;
      readonly takeoverState: unknown;
    }> {
      const storage = await options.createStorage();
      const peer = await options.createPeer(storage);
      const gate = deferred();
      let calls = 0;
      const wf = params.build(async () => {
        calls++;
        if (calls === 1) {
          await gate.promise;
          return "zombie";
        }
        return "fresh";
      });

      const a = recordWrites(storage);
      const runA = createWorkflowRunner({ storage: a.storage })
        .run({ workflow: wf, workflowId: params.workflowId, input: {} })
        .then(
          () => ({ ok: true }),
          (error: unknown) => ({ ok: false, error }),
        );
      // Whatever happens below, A is released and both runs have settled
      // before the test ends: a run left parked on the gate (or a takeover
      // still writing) would otherwise outlive the test and race the next
      // test's setup on the shared backend.
      try {
        await waitFor(() => calls === 1);

        // A stalls past its lease; B takes the run over and finishes it.
        await options.expireLock({ storage, workflowId: params.workflowId });
        a.arm();
        const takeoverResult = await createWorkflowRunner({ storage: peer }).run({
          workflow: wf,
          workflowId: params.workflowId,
          input: {},
        });
        const takeoverState = await snapshot(storage, params.workflowId);

        // A wakes up and carries on.
        gate.resolve();
        const zombie = await runA;
        return { storage, zombie, writes: a.writes, takeoverResult, takeoverState };
      } finally {
        gate.resolve();
        await runA;
      }
    }

    function buildJournaled(work: () => Promise<string>) {
      return workflow<Record<string, never>>({ name: "zombie-journaled" })
        .journaled("body", function* (ctx) {
          // Idempotent, so the new holder re-runs an activity the stalled
          // holder left pending instead of halting as ambiguous.
          return yield* ctx.activity("work", work, { idempotent: true });
        })
        .build() as Workflow<unknown, unknown>;
    }

    function buildStep(work: () => Promise<string>) {
      return workflow<Record<string, never>>({ name: "zombie-step" })
        .stepAsync("body", () => work())
        .build() as Workflow<unknown, unknown>;
    }

    it(
      "a stalled journaled activity cannot record its result once another worker took the run",
      async () => {
        const workflowId = "zombie-journaled-1";
        const out = await stallThenTakeOver({ workflowId, build: buildJournaled });

        expect(out.takeoverResult).toBe("fresh");
        expect(out.zombie.ok).toBe(false);
        // A tried to write, and every write it made was fenced off.
        expect(out.writes.length).toBeGreaterThan(0);
        expect(out.writes.map((w) => w.outcome)).toEqual(
          out.writes.map(() => "FenceTokenMismatchError"),
        );
        expect(await snapshot(out.storage, workflowId)).toEqual(out.takeoverState);
        const state = (await out.storage.loadWorkflow(workflowId))!;
        expect(state.status).toBe("completed");
        expect(state.result).toBe("fresh");
        if (hasCapability(out.storage, "journal")) {
          const journal = await out.storage.loadJournal({ workflowId, stepName: "body" });
          expect(journal.map((e) => e.exit)).toEqual([{ tag: "Success", value: "fresh" }]);
        }
      },
      TEST_TIMEOUT_MS,
    );

    it(
      "a stalled step cannot save its result or end the run once another worker took the run",
      async () => {
        const workflowId = "zombie-step-1";
        const out = await stallThenTakeOver({ workflowId, build: buildStep });

        expect(out.takeoverResult).toBe("fresh");
        expect(out.zombie.ok).toBe(false);
        expect(out.writes.length).toBeGreaterThan(0);
        expect(out.writes.map((w) => w.outcome)).toEqual(
          out.writes.map(() => "FenceTokenMismatchError"),
        );
        expect(await snapshot(out.storage, workflowId)).toEqual(out.takeoverState);
        const state = (await out.storage.loadWorkflow(workflowId))!;
        expect(state.status).toBe("completed");
        expect(state.result).toBe("fresh");
        expect(state.steps["body"]?.result).toBe("fresh");
      },
      TEST_TIMEOUT_MS,
    );
  });
}
