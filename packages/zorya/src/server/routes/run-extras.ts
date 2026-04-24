// ---------------------------------------------------------------------------
// Run extras — secondary endpoints for signal history, attempt history,
// prior runs, and children. Each one degrades gracefully when the backing
// storage doesn't implement the optional interface.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  SignalState,
  StepAttemptRecord,
  WorkflowRunSummary,
} from "@promin/workflow";
import { isStepAttemptStorage } from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import { runToSummaryDto } from "../serialize.ts";
import type { RunSummaryDto } from "../api-types.ts";

export interface SignalDto {
  signalName: string;
  payload: unknown;
  /** ISO timestamp. */
  deliveredAt: string;
}

export interface SignalHistoryResponse {
  signals: SignalDto[];
}

export interface AttemptDto {
  stepName: string;
  attempt: number;
  type: "execution" | "compensation";
  status: "completed" | "failed";
  result?: unknown;
  error?: string;
  durationMs: number;
  /** ISO timestamp. */
  startedAt: string;
  /** ISO timestamp. */
  completedAt: string;
  workerId?: string;
}

export interface AttemptsResponse {
  /** Whether the storage backend records attempts. If false, `attempts` is []. */
  supported: boolean;
  attempts: AttemptDto[];
}

export interface RunHistoryEntryDto {
  run: number;
  version?: string;
  status: string;
  result?: unknown;
  error?: string;
  /** ISO timestamp. */
  createdAt: string;
  /** ISO timestamp. */
  startedAt?: string;
  /** ISO timestamp. */
  completedAt?: string;
}

export interface RunHistoryResponse {
  runs: RunHistoryEntryDto[];
}

export interface ChildrenResponse {
  children: RunSummaryDto[];
}

// ---------------------------------------------------------------------------

function signalToDto(s: SignalState): SignalDto {
  return {
    signalName: s.signalName,
    payload: s.payload,
    deliveredAt: s.deliveredAt.toISOString(),
  };
}

function attemptToDto(a: StepAttemptRecord): AttemptDto {
  return {
    stepName: a.stepName,
    attempt: a.attempt,
    type: a.type,
    status: a.status,
    result: a.result,
    error: a.error,
    durationMs: a.durationMs,
    startedAt: a.startedAt.toISOString(),
    completedAt: a.completedAt.toISOString(),
    workerId: a.workerId,
  };
}

function runSummaryToDto(s: WorkflowRunSummary): RunHistoryEntryDto {
  return {
    run: s.run,
    version: s.version,
    status: s.status,
    result: s.result,
    error: s.error,
    createdAt: s.createdAt.toISOString(),
    startedAt: s.startedAt?.toISOString(),
    completedAt: s.completedAt?.toISOString(),
  };
}

// ---------------------------------------------------------------------------

export function getRunSignals(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const signals = await storage.loadSignals(id);
    const response: SignalHistoryResponse = { signals: signals.map(signalToDto) };
    return json(200, response);
  };
}

export function getRunAttempts(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (!isStepAttemptStorage(storage)) {
      const response: AttemptsResponse = { supported: false, attempts: [] };
      return json(200, response);
    }
    const url = new URL(req.url);
    const stepName = url.searchParams.get("stepName") ?? undefined;
    const records = await storage.loadStepAttempts(id, stepName);
    const response: AttemptsResponse = {
      supported: true,
      attempts: records.map(attemptToDto),
    };
    return json(200, response);
  };
}

export function getRunHistory(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const rows = await storage.loadRunHistory(id);
    const response: RunHistoryResponse = { runs: rows.map(runSummaryToDto) };
    return json(200, response);
  };
}

export function getRunChildren(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const rows = await storage.listWorkflows({ parentId: id, limit: 200 });
    const response: ChildrenResponse = { children: rows.map(runToSummaryDto) };
    return json(200, response);
  };
}
