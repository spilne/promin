// ---------------------------------------------------------------------------
// `ZoryaRunner` — execute a workflow locally, persist state to Zorya.
//
// The simplest pattern when you have a Workflow and want to run it as part
// of normal application code (a request handler, a cron tick, a CLI):
//
//   const client = new ZoryaClient({ url: "http://localhost:4100" });
//   const runner = new ZoryaRunner({ client });
//
//   const result = await runner.run(verifyOrder, { orderId: 42 });
//   if (result.risky) holdForReview() else approve();
//
// What this does versus `ZoryaWorker`:
//
//   - The orchestration runs IN YOUR PROCESS (same as `worker.run(...)`).
//   - Storage / activity-journal writes go over the wire to Zorya, so the
//     run shows up in the dashboard with its full step DAG.
//   - There's NO queue polling and NO advertisement, so the user's process
//     can't accidentally pick up unrelated runs that other processes
//     scheduled. You only run what your code explicitly asks for.
//
// Use a `ZoryaWorker` instead when:
//   - The workflow uses `ctx.sleep` and must resume after the user's
//     process exits (sleep-driven resume needs a SleepScanner with access
//     to the workflow definition; the runner-only pattern is meant for
//     synchronous "kick off + await" use).
//   - You want the dashboard's "Trigger" button to start runs in your
//     process (that requires the workflow-start poll loop).
// ---------------------------------------------------------------------------

import {
  createWorkflowRunner,
  type Workflow,
  type WorkflowHandle,
  type WorkflowRunner,
} from "@promin/workflow";
import type { ZoryaClient } from "./zorya-client.ts";

export interface ZoryaRunnerConfig {
  readonly client: ZoryaClient;
}

export interface ZoryaRunOptions {
  /**
   * Override the auto-generated workflowId. Useful for idempotency:
   * passing the same id twice for an in-flight or completed run resumes
   * / no-ops per the workflow's `idempotency` config.
   */
  readonly workflowId?: string;
  readonly namespace?: string;
  readonly metadata?: Record<string, unknown>;
}

export class ZoryaRunner {
  private readonly runner: WorkflowRunner;

  constructor(config: ZoryaRunnerConfig) {
    // The client's `storage` is already a RemoteWorkflowStorage that proxies
    // to Zorya's `/rpc/storage`, so the locally-driven runner persists every
    // state transition + activity journal entry on the server side.
    this.runner = createWorkflowRunner({ storage: config.client.storage });
  }

  /**
   * Execute a workflow locally and resolve to its `Output`. Throws on
   * workflow failure (use `runSafe` for `{ data, error }` semantics).
   *
   * Generates a fresh `workflowId` per call when one isn't supplied, so a
   * naive `runner.run(wf, input)` is safe to call from concurrent
   * request handlers without manual id juggling.
   */
  async run<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    opts: ZoryaRunOptions = {},
  ): Promise<Output> {
    const workflowId = opts.workflowId ?? buildWorkflowId(workflow.name);
    if (opts.namespace || opts.metadata) {
      // Pre-create the row when caller wants tenant scoping or metadata.
      // The runner's internal createWorkflow is idempotent on workflowId,
      // so the duplicate isn't a problem.
      await this.runner.storage.createWorkflow({
        workflowId,
        workflowName: workflow.name,
        input,
        namespace: opts.namespace,
        metadata: opts.metadata,
        version: workflow.version,
      });
    }
    return (await this.runner.run({ workflow, workflowId, input })) as Output;
  }

  /**
   * Like `run` but returns `{ data, error }` instead of throwing.
   * Mirrors `WorkflowRunner.runSafe` — useful when the caller wants to
   * branch on success/failure without a try/catch.
   */
  async runSafe<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    opts: ZoryaRunOptions = {},
  ): Promise<{ data: Output; error: null } | { data: null; error: unknown }> {
    const workflowId = opts.workflowId ?? buildWorkflowId(workflow.name);
    if (opts.namespace || opts.metadata) {
      await this.runner.storage.createWorkflow({
        workflowId,
        workflowName: workflow.name,
        input,
        namespace: opts.namespace,
        metadata: opts.metadata,
        version: workflow.version,
      });
    }
    const out = await this.runner.runSafe({ workflow, workflowId, input });
    if (out.error) return { data: null, error: out.error };
    return { data: out.data as Output, error: null };
  }

  /**
   * Fire-and-forget. Returns a `WorkflowHandle` whose `.result()` resolves
   * to the workflow's Output without you blocking on the run loop here.
   * Useful when the caller wants to dispatch the workflow and observe it
   * from a different async context (e.g. a streaming HTTP response).
   */
  async start<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    opts: ZoryaRunOptions = {},
  ): Promise<WorkflowHandle<Output>> {
    const workflowId = opts.workflowId ?? buildWorkflowId(workflow.name);
    return this.runner.start({ workflow, workflowId, input });
  }
}

function buildWorkflowId(name: string): string {
  // Same shape ZoryaClient.start() uses when the caller doesn't supply
  // one — keeps log lines and dashboard ids visually consistent.
  return `${name}-${crypto.randomUUID()}`;
}
