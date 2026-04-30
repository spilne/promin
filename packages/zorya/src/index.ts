// Public API exports

export {
  ZoryaServer,
  type ZoryaServerConfig,
  type ListenOptions,
  type Logger,
} from "./server/server.ts";
export { Auth, type AuthConfig } from "./server/auth.ts";
export { RunEventBus } from "./server/run-event-bus.ts";
export { RunPollWatcher, isTerminalStatus } from "./server/run-poll-watcher.ts";
export { Router, json, jsonError, readJson, type Handler } from "./server/router.ts";
export { runToDto, runToSummaryDto, stepToDto } from "./server/serialize.ts";
export { StorageMetricsProvider, type MetricsProvider } from "./server/routes/metrics.ts";
export { emptyWorkersProvider, type WorkersProvider } from "./server/routes/workers.ts";
export type { RunTrigger } from "./server/routes/runs.ts";
export type { ScheduleDto, SchedulesResponse } from "./server/routes/schedules.ts";
export {
  scanWorkflowsFolder,
  type ScanOptions,
  type ScanResult,
} from "./server/workflow-registry.ts";
export {
  scanAgentsFolder,
  startAgentsScanLoop,
  type AgentScanOptions,
  type AgentScanFolderResult,
} from "./server/agent-registry.ts";
export {
  InMemoryWorkflowAdvertisementRegistry,
  type WorkflowAdvertisementRegistry,
  type AdvertisedWorkflow,
  type AdvertisementEntry,
} from "./server/workflow-advertisements.ts";
export {
  InMemoryWorkflowStartQueue,
  type WorkflowStartQueue,
  type WorkflowStartRecord,
  type WorkerWorkflowSpec,
} from "./server/workflow-starts.ts";
export {
  RunsService,
  TriggerService,
  mergePlannedSteps,
  type RunsServiceDeps,
  type TriggerServiceDeps,
} from "./server/services/index.ts";
export type {
  SignalDto,
  SignalHistoryResponse,
  AttemptDto,
  AttemptsResponse,
  RunHistoryEntryDto,
  RunHistoryResponse,
  ChildrenResponse,
} from "./server/routes/run-extras.ts";
export type { GridRunDto, GridResponse, SparklinesResponse } from "./server/routes/grid.ts";
export type {
  WorkflowStepDefDto,
  WorkflowDefDto,
  WorkflowDefsResponse,
} from "./server/routes/workflow-defs.ts";

// Wire format
export type {
  RunDto,
  RunSummaryDto,
  StepDto,
  RunListResponse,
  RunListQuery,
  RunEvent,
  TriggerRunRequest,
  TriggerRunResponse,
  SignalRequest,
  MetricsDto,
  WorkerDto,
  WorkersResponse,
  ApiError,
} from "./server/api-types.ts";
