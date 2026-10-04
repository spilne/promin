import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import type { WorkflowStorage } from "../../workflow-storage.ts";
import { hasCapability, STORAGE_CAPABILITIES, storageCapabilities } from "../capabilities.ts";

/** An in-memory storage with the named methods removed. */
function without(names: readonly string[]): WorkflowStorage {
  const storage = new InMemoryWorkflowStorage();
  const hidden = new Set(names);
  return new Proxy(storage, {
    get(target, prop, receiver) {
      if (typeof prop === "string" && hidden.has(prop)) return undefined;
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

describe("storage capabilities", () => {
  it("the in-memory storage has every capability", () => {
    const caps = storageCapabilities(new InMemoryWorkflowStorage());
    for (const capability of STORAGE_CAPABILITIES) {
      // The in-memory storage has no bulk stale-run cancel.
      expect(caps[capability]).toBe(capability !== "cancelStale");
    }
  });

  it("a capability needs every one of its methods", () => {
    const storage = without(["findPendingSignal"]);
    expect(hasCapability(storage, "journal")).toBe(false);
    expect(hasCapability(storage, "journalDiscard")).toBe(true);
    expect(storageCapabilities(storage).journal).toBe(false);
  });

  it("each optional method maps to its capability", () => {
    const cases: ReadonlyArray<readonly [string, keyof ReturnType<typeof storageCapabilities>]> = [
      ["tripwireWorkflow", "tripwire"],
      ["resetSteps", "resetSteps"],
      ["subscribeToWorkflow", "runEvents"],
      ["notifyStepStarted", "stepStartedEvents"],
      ["listWorkflowSummaries", "summaries"],
      ["countWorkflows", "countWorkflows"],
      ["listDueTimers", "dueTimers"],
      ["listSignalWakeups", "signalWakeups"],
      ["listOrphanedRuns", "orphanedRuns"],
      ["checkpointStep", "stepCheckpoint"],
      ["saveStepAttempt", "stepAttempts"],
      ["beginCompensation", "compensationLedger"],
      ["discardJournalEntries", "journalDiscard"],
    ];
    for (const [method, capability] of cases) {
      const caps = storageCapabilities(without([method]));
      expect({ method, has: caps[capability] }).toEqual({ method, has: false });
      for (const other of STORAGE_CAPABILITIES) {
        if (other === capability || other === "cancelStale") continue;
        expect({ method, other, has: caps[other] }).toEqual({ method, other, has: true });
      }
    }
  });
});
