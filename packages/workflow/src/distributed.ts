// ---------------------------------------------------------------------------
// @promin/workflow/distributed — run workflow steps on remote workers.
//
// The distributed runner drives a workflow and hands its steps to a
// `StepQueue`; workers claim them, run the handlers they registered and
// commit the outcome. Also here: the worker registry, leader election,
// sleep / signal scanners, worker middleware, the step-queue executor for
// mixed local + remote runs, workflow advertisements and the start queue.
// ---------------------------------------------------------------------------

export {
  type StepRegistry,
  type StepHandler,
  type WorkerStepContext,
  type StepRegistration,
  type RegisterStepParams,
  MapStepRegistry,
} from "./lib/distributed/step-registry.ts";
export {
  type StepQueue,
  type StepTask,
  type StepTaskStatus,
  type StepTaskRecord,
  type StepQueueEnqueueParams,
  type StepQueueCompleteParams,
  type StepQueueFailParams,
  type StepQueueClaimParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  DEFAULT_MAX_DELIVERIES,
} from "./lib/distributed/step-queue.ts";
export {
  InMemoryStepQueue,
  type InMemoryStepQueueConfig,
} from "./lib/distributed/in-memory-step-queue.ts";
export {
  type DistributedWorkflowRunner,
  createDistributedWorkflowRunner,
  type DistributedRunnerConfig,
  type DistributedRunnerErrorEvent,
  buildStubWorkflow,
} from "./lib/distributed/coordinator.ts";
export {
  type WorkflowWorker,
  type WorkerConfig,
  type WorkerHooks,
  type WorkerErrorEvent,
  type WorkerErrorPhase,
  createWorker,
  TaskLeaseLostError,
  WorkerStoppingError,
} from "./lib/distributed/worker.ts";
export {
  type WorkerInfo,
  type WorkerStatus,
  type WorkerRegistry,
  InMemoryWorkerRegistry,
  type InMemoryWorkerRegistryConfig,
} from "./lib/distributed/worker-registry.ts";
export {
  type LeaderElection,
  SingleLeader,
  coordinatorLeaderKey,
  scannerLeaderKey,
} from "./lib/distributed/leader-election.ts";
export {
  type SleepScanner,
  type SleepScannerConfig,
  createSleepScanner,
} from "./lib/distributed/sleep-scanner.ts";
export {
  type SignalScanner,
  type SignalScannerConfig,
  createSignalScanner,
} from "./lib/distributed/signal-scanner.ts";
export {
  type WorkerMiddleware,
  type NextFn,
  timeoutMiddleware,
  retryMiddleware,
  loggingMiddleware,
  metricsMiddleware,
} from "./lib/distributed/middleware.ts";
export {
  StepQueueExecutor,
  type StepQueueExecutorConfig,
  StepWaitTimeoutError,
  QueuedStepError,
  StepWaitAbandonedError,
  DEFAULT_STEP_WAIT_TIMEOUT_MS,
} from "./lib/distributed/step-queue-executor.ts";
export {
  type WorkflowAdvertisementRegistry,
  type AdvertisedWorkflow,
  type AdvertisementEntry,
  InMemoryWorkflowAdvertisementRegistry,
  type InMemoryWorkflowAdvertisementRegistryConfig,
} from "./lib/distributed/workflow-advertisements.ts";
export {
  type WorkflowStartQueue,
  type WorkflowStartRecord,
  type WorkerWorkflowSpec,
  type WorkflowStartClaimRef,
  InMemoryWorkflowStartQueue,
  type InMemoryWorkflowStartQueueConfig,
} from "./lib/distributed/workflow-start-queue.ts";
