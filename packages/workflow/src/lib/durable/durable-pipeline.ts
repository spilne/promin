// ---------------------------------------------------------------------------
// Re-export barrel for the workflow builder modules, so existing imports of
// `durable-pipeline.ts` keep working. New code imports from the modules:
//
//   workflow-types.ts      Workflow, its internals, handle/status/hook and
//                          workflow-level config types
//   step-definition.ts     StepDefinition, ExecuteParams / StepRuntime, step
//                          contexts and options, StepEff, asStepEff
//   step-cache.ts          step-level result cache
//   steps/*.ts             one pure factory per step kind
//   builder-state.ts       the builder's immutable state and step list
//   workflow-builder.ts    WorkflowBuilder, workflow(), flow()
//   workflow-dag-viz.ts    WorkflowDAG, dagToMermaid, dagToDot
// ---------------------------------------------------------------------------

export type {
  CompensateConfig,
  DispatchConfig,
  IdempotencyConfig,
  Workflow,
  WorkflowConfig,
  WorkflowErrorOf,
  WorkflowDefinitionInternals,
  WorkflowHandle,
  WorkflowHooks,
  WorkflowParams,
  WorkflowQueueConfig,
  WorkflowStatusInfo,
} from "./workflow-types.ts";
export {
  asStepEff,
  readPrev,
  type DagStepContext,
  type ExecuteParams,
  type JournaledStepOptions,
  type MapElementOptions,
  type MapOverOptions,
  type MapStepContext,
  type ParallelStepsOptions,
  type RunChildWorkflow,
  type StepCacheOption,
  type StepContext,
  type StepDefinition,
  type StepEff,
  type StepFailureStrategy,
  type StepKind,
  type StepOptions,
  type StepQueueContext,
  type StepQueueOption,
  type StepRuntime,
  type SubworkflowOptions,
  type TripwireOptions,
} from "./step-definition.ts";
export { stepCacheKey } from "./step-cache.ts";
export type { LoopOptions } from "./steps/loop-step.ts";
export { MatchError, type MatchParams } from "./steps/match-step.ts";
export type { BranchError, BranchOutput } from "./steps/parallel-steps.ts";
export { WorkflowBuilder, flow, workflow } from "./workflow-builder.ts";
export { dagToDot, dagToMermaid, toWorkflowDag, type WorkflowDAG } from "./workflow-dag-viz.ts";
