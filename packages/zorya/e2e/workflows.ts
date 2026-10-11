// ---------------------------------------------------------------------------
// Workflows the worker-driven end-to-end tests run on real worker processes.
// Every step result carries the pid of the process that ran it, so a test
// can tell which worker drove which part of a run.
// ---------------------------------------------------------------------------

import { workflow, type Workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

const pid = () => process.pid;
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Two steps; `second` takes `input.ms` milliseconds. */
export const slow = workflow<{ ms: number }>({ name: "e2e-slow" })
  .step("first", () => succeed({ pid: pid() }))
  .stepAsync("second", async ({ input }) => {
    await delay(input.ms);
    return { pid: pid() };
  })
  .build();

/** A durable sleep between two steps. */
export const sleeper = workflow<{ ms: number }>({ name: "e2e-sleeper" })
  .step("before", () => succeed({ pid: pid() }))
  .sleep("nap", 500)
  .step("after", () => succeed({ pid: pid(), woke: true }))
  .build();

/** Waits for the `go` signal, then echoes its payload. */
export const waiter = workflow<{ n: number }>({ name: "e2e-waiter" })
  .waitForSignal<{ add: number }, "go">("go", { signalName: "go" })
  .step("sum", ({ input, prev }) => succeed(input.n + prev.add))
  .build();

/** A child workflow that sleeps, so its parent parks on it. */
export const child = workflow<{ n: number }>({ name: "e2e-child" })
  .sleep("child-nap", 500)
  .step("double", ({ input }) => succeed(input.n * 2))
  .build();

/** Runs `e2e-child` as a step; the child's id derives from the parent's input. */
export const parent = workflow<{ n: number; id: string }>({ name: "e2e-parent" })
  .subworkflow("kid", child, {
    input: (prev) => ({ n: prev.n }),
    workflowId: (prev) => `${prev.id}-kid`,
  })
  .step("finish", ({ prev }) => succeed(prev + 1))
  .build();

/** One version per deploy: `version` names the code that ran. */
export function versioned(version: string): Workflow<unknown, unknown> {
  return workflow<unknown>({ name: "e2e-versioned", version })
    .step("which", () => succeed({ version, pid: pid() }))
    .build() as Workflow<unknown, unknown>;
}

export const ALL = [slow, sleeper, waiter, parent] as unknown as Workflow<unknown, unknown>[];
