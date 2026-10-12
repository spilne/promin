// ---------------------------------------------------------------------------
// The eval-run workflow — one durable run of an eval suite against one
// agent recipe version.
//
//   plan            journaled: the suite's cases and run knobs, read once (a
//                   replay sees the cases the run started with) and the run
//                   recorded as `running` in the run store
//   case-<i>-begin  journaled: an attempt token minted in the process that
//                   starts the case
//   case-<i>        journaled, `concurrency` cases at a time: every sample of
//                   the case run against the agent and scored, recorded in
//                   the run store as it finishes
//   finish          journaled: the totals, and the run marked `completed`
//
// A run whose process died is resumed by name after a restart (orphan
// recovery): the cases it finished replay from the journal. A case that was
// in flight carries an attempt token the new process did not mint, so the
// steps record it as interrupted instead of running its target (and its
// tools' side effects) a second time — unless the suite declares its cases
// idempotent. A run that fails or is cancelled leaves the run store
// `running`; the evals service reconciles it from the workflow's status.
// ---------------------------------------------------------------------------

import { workflow, type ActivityYield, type Workflow } from "@promin/workflow";
import type { EvalCase, EvalCaseResult, EvalRunTotals } from "@promin/evals";

/** Name the eval-run workflow is defined under. */
export const EVAL_RUN_WORKFLOW_NAME = "eval-run";

/** What a run is started with: a suite version and an agent version, by reference. */
export interface EvalRunWorkflowInput {
  readonly suiteId: string;
  readonly suiteVersion: string;
  readonly agentId: string;
  readonly agentVersion: string;
  /** Tenant scope every case's agent invocation runs in. */
  readonly scope: { readonly namespaceId: string; readonly resourceId: string };
}

export interface EvalRunWorkflowOutput {
  readonly runId: string;
  readonly totals: EvalRunTotals;
}

/** The journaled plan of a run. */
export interface EvalRunPlan {
  readonly cases: ReadonlyArray<EvalCase>;
  readonly concurrency: number;
}

/** The side effects the workflow journals; the evals service implements them. */
export interface EvalRunSteps {
  plan(params: { runId: string; input: EvalRunWorkflowInput }): Promise<EvalRunPlan>;
  /** Mint an attempt token for a case about to run in this process. */
  beginCase(): string;
  runCase(params: {
    runId: string;
    input: EvalRunWorkflowInput;
    caseIndex: number;
    evalCase: EvalCase;
    /** The case's journaled attempt token. */
    attempt: string;
  }): Promise<EvalCaseResult>;
  finish(params: {
    runId: string;
    caseResults: ReadonlyArray<EvalCaseResult>;
  }): Promise<EvalRunTotals>;
  /** In-memory cleanup when the body stops running here (any way). */
  release(params: { runId: string }): void;
}

/** Build the eval-run workflow. Define it once at boot, so recovery finds it by name. */
export function createEvalRunWorkflow(params: {
  readonly steps: EvalRunSteps;
  readonly name?: string;
}): Workflow<EvalRunWorkflowInput, EvalRunWorkflowOutput> {
  const { steps } = params;
  return workflow<EvalRunWorkflowInput>({
    name: params.name ?? EVAL_RUN_WORKFLOW_NAME,
    type: "eval-run",
  })
    .journaled("eval-run", function* (ctx, input) {
      const runId = ctx.workflowId;
      try {
        const plan = yield* ctx.activity("plan", () => steps.plan({ runId, input }), {
          idempotent: true,
        });

        function* evalCase(params: {
          caseIndex: number;
          evalCase: EvalCase;
        }): Generator<ActivityYield, EvalCaseResult, never> {
          const { caseIndex } = params;
          const attempt = yield* ctx.activity(`case-${caseIndex}-begin`, () => steps.beginCase(), {
            idempotent: true,
          });
          return yield* ctx.activity(
            `case-${caseIndex}`,
            () => steps.runCase({ runId, input, caseIndex, evalCase: params.evalCase, attempt }),
            { idempotent: true },
          );
        }

        const caseResults: EvalCaseResult[] = [];
        for (let start = 0; start < plan.cases.length; start += plan.concurrency) {
          const batch = plan.cases.slice(start, start + plan.concurrency);
          const results = yield* ctx.parallel(
            batch.map((c, offset) => evalCase({ caseIndex: start + offset, evalCase: c })),
          );
          caseResults.push(...results);
        }
        const totals = yield* ctx.activity("finish", () => steps.finish({ runId, caseResults }), {
          idempotent: true,
        });
        return { runId, totals };
      } finally {
        steps.release({ runId });
      }
    })
    .build();
}
