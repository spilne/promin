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
  JournalEntry,
} from "@promin/workflow";
import { isStepAttemptStorage, isActivityJournalStorage } from "@promin/workflow";
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

export interface JournalEntryDto {
  activityIndex: number;
  branchPath: string;
  activityName: string;
  stepType: "activity" | "sleep" | "signal" | "compensation" | "child";
  phase: "pending" | "completed";
  payloadHash?: string;
  /** Tagged exit payload — `Success.value` is whatever the activity returned, `Failure.error` is the message string. */
  exit?: { tag: "Success"; value: unknown } | { tag: "Failure"; error: string };
  /** ISO timestamp — present on `sleep` entries. */
  wakeAt?: string;
  /** ISO timestamp. */
  createdAt: string;
}

export interface StepJournalResponse {
  /** False when the storage backend doesn't implement ActivityJournalStorage. */
  supported: boolean;
  entries: JournalEntryDto[];
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

/**
 * Journal entries for one journaled step. The DAG view + workflow steps
 * collapse a `.journaled()` block into a single node, but the run's
 * actual execution had multiple `ctx.activity` / `ctx.sleep` checkpoints
 * — this endpoint surfaces them so the run-detail UI can list each one
 * with its result. Degrades gracefully when the storage backend doesn't
 * implement ActivityJournalStorage (returns supported=false).
 */
export function getRunStepJournal(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const stepName = params.stepName;
    if (!id) return jsonError(400, "missing_id");
    if (!stepName) return jsonError(400, "missing_step_name");
    if (!isActivityJournalStorage(storage)) {
      const response: StepJournalResponse = { supported: false, entries: [] };
      return json(200, response);
    }
    const entries = await storage.loadJournal(id, stepName);
    const response: StepJournalResponse = {
      supported: true,
      entries: entries.map(journalEntryToDto),
    };
    return json(200, response);
  };
}

function journalEntryToDto(e: JournalEntry): JournalEntryDto {
  return {
    activityIndex: e.activityIndex,
    branchPath: e.branchPath,
    activityName: e.activityName,
    stepType: e.stepType ?? "activity",
    phase: e.phase ?? "completed",
    payloadHash: e.payloadHash,
    exit: e.exit,
    wakeAt: e.wakeAt?.toISOString(),
    createdAt: e.createdAt.toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Admin actions — mark success / failed / rerun
// ---------------------------------------------------------------------------

export function markRunSuccess(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      await storage.completeWorkflow(id, null);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(
        400,
        "mark_success_failed",
        err instanceof Error ? err.message : String(err),
      );
    }
  };
}

export function markRunFailed(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    let reason = "marked failed";
    try {
      const body = (await req.json().catch(() => null)) as { reason?: string } | null;
      if (body?.reason) reason = body.reason;
    } catch {
      // body is optional
    }
    try {
      await storage.failWorkflow(id, reason);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "mark_failed_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * Reset a completed/failed run for a fresh execution. Requires a trigger
 * function because re-running a workflow definition needs the runner; the
 * server never owns workflow code itself.
 */
export function rerunRun(storage: WorkflowStorage, trigger?: (id: string) => Promise<void>) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      await storage.startFreshRun(id);
      if (trigger) await trigger(id);
      return json(200, { ok: true });
    } catch (err) {
      return jsonError(400, "rerun_failed", err instanceof Error ? err.message : String(err));
    }
  };
}
