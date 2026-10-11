// ---------------------------------------------------------------------------
// Run extras — secondary endpoints for signal history, attempt history,
// prior runs, children and journals, plus rerun and resume. Every route answers 404
// for an unknown run. The read routes degrade gracefully when the backing
// storage doesn't implement the optional interface.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  SignalState,
  StepAttemptRecord,
  WorkflowRunSummary,
  JournalEntry,
} from "@promin/workflow";
import {
  WorkflowLockError,
  hasCapability,
  isCancelledRun,
  isTerminalWorkflowStatus,
} from "@promin/workflow";
import { z } from "zod";
import { HttpError, accepted, json, jsonError } from "../router.ts";
import { runToSummaryDto } from "../serialize.ts";
import {
  IdSchema,
  NameSchema,
  OffsetQuery,
  limitQuery,
  ResumeRunRequestSchema,
  type RerunResponse,
  type ResumeRunResponse,
  type RunSummaryDto,
} from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import type { RunsService } from "../services/runs-service.ts";
import { RunCancelledError } from "../services/workflows/errors.ts";
import { RunParamsSchema, runLocation } from "./runs.ts";

export interface SignalDto {
  signalName: string;
  payload: unknown;
  /** ISO timestamp. */
  deliveredAt: string;
}

/**
 * Signal values delivered to the current run, at most one per name (the
 * latest: delivery is last-wins until the run consumes it).
 */
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
  executorId?: string;
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
  /** Children across all pages, when the storage can count. */
  total?: number;
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
  /** False when the storage backend doesn't implement JournalStore. */
  supported: boolean;
  entries: JournalEntryDto[];
}

/** Query of `GET /api/runs/:id/attempts`. */
export const AttemptsQuerySchema = z.object({
  stepName: z.preprocess((v) => (v === "" ? undefined : v), NameSchema.optional()),
});

/** Query of `GET /api/runs/:id/history`. */
export const RunHistoryQuerySchema = z.object({
  limit: limitQuery({ fallback: 100 }),
  offset: OffsetQuery,
});

/** Query of `GET /api/runs/:id/children`. */
export const ChildrenQuerySchema = z.object({
  limit: limitQuery({ fallback: 100 }),
  offset: OffsetQuery,
});

const JournalParamsSchema = z.object({ id: IdSchema, stepName: NameSchema });

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
    executorId: a.executorId,
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

/** Throw a 404 unless the run exists. */
export async function requireRun(params: {
  storage: WorkflowStorage;
  workflowId: string;
}): Promise<void> {
  const status = await params.storage.loadWorkflowStatus(params.workflowId);
  if (status === null || status === undefined) {
    throw new HttpError({ status: 404, code: "not_found", message: "Run not found" });
  }
}

// ---------------------------------------------------------------------------

export function getRunSignals(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    await requireRun({ storage, workflowId: id });
    const signals = await storage.loadSignals(id);
    const response: SignalHistoryResponse = { signals: signals.map(signalToDto) };
    return json(200, response);
  };
}

export function getRunAttempts(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const { stepName } = parseQuery({ req, schema: AttemptsQuerySchema });
    await requireRun({ storage, workflowId: id });
    if (!hasCapability(storage, "stepAttempts")) {
      const response: AttemptsResponse = { supported: false, attempts: [] };
      return json(200, response);
    }
    const records = await storage.loadStepAttempts({ workflowId: id, stepName });
    const response: AttemptsResponse = {
      supported: true,
      attempts: records.map(attemptToDto),
    };
    return json(200, response);
  };
}

export function getRunHistory(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const { limit, offset } = parseQuery({ req, schema: RunHistoryQuerySchema });
    await requireRun({ storage, workflowId: id });
    const rows = await storage.loadRunHistory({ workflowId: id, limit, offset });
    const response: RunHistoryResponse = { runs: rows.map(runSummaryToDto) };
    return json(200, response);
  };
}

export function getRunChildren(storage: WorkflowStorage) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: RunParamsSchema });
    const { limit, offset } = parseQuery({ req, schema: ChildrenQuerySchema });
    await requireRun({ storage, workflowId: id });
    const [rows, total] = await Promise.all([
      hasCapability(storage, "summaries")
        ? storage.listWorkflowSummaries({ parentId: id, limit, offset })
        : storage.listWorkflows({ parentId: id, limit, offset }),
      hasCapability(storage, "countWorkflows")
        ? storage.countWorkflows({ parentId: id })
        : undefined,
    ]);
    const response: ChildrenResponse = {
      children: rows.map(runToSummaryDto),
      ...(total !== undefined && { total }),
    };
    return json(200, response);
  };
}

/**
 * Journal entries for one journaled step. The DAG view + workflow steps
 * collapse a `.journaled()` block into a single node, but the run's
 * actual execution had multiple `ctx.activity` / `ctx.sleep` checkpoints
 * — this endpoint surfaces them so the run-detail UI can list each one
 * with its result. Degrades gracefully when the storage backend doesn't
 * implement JournalStore (returns supported=false).
 */
export function getRunStepJournal(storage: WorkflowStorage) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, stepName } = parseParams({ params, schema: JournalParamsSchema });
    await requireRun({ storage, workflowId: id });
    if (!hasCapability(storage, "journal")) {
      const response: StepJournalResponse = { supported: false, entries: [] };
      return json(200, response);
    }
    const entries = await storage.loadJournal({ workflowId: id, stepName });
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

/**
 * POST /api/runs/:id/rerun — start a fresh run of an ended run under the
 * same id: 202 + `Location`. Only ended runs re-run (409
 * `run_not_terminal` otherwise — resetting a live run would race its
 * driver). The workflow service owns the reset, so the run number goes up
 * exactly once.
 */
export function rerunRun(params: {
  storage: WorkflowStorage;
  rerun: (workflowId: string) => Promise<void>;
}) {
  const { storage, rerun } = params;
  return async (_req: Request, routeParams: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params: routeParams, schema: RunParamsSchema });
    const state = await storage.loadWorkflow(id);
    if (!state) return jsonError(404, "not_found");
    if (!isTerminalWorkflowStatus(state.status)) {
      return jsonError(
        409,
        "run_not_terminal",
        `Only an ended run can be re-run (status: ${state.status})`,
      );
    }
    try {
      await rerun(id);
    } catch (err) {
      if (err instanceof WorkflowLockError) {
        return jsonError(409, "run_in_flight", err.message);
      }
      throw err;
    }
    const response: RerunResponse = { workflowId: id };
    return accepted(response, runLocation(id));
  };
}

/**
 * Re-drives an ended run from `fromStep` with `WorkflowRunner.resume`
 * semantics: `fromStep` and every step downstream of it are reset and run
 * again, the steps upstream keep their results. Resolves once the run is
 * under way. Throws `WorkflowLockError` when another driver holds the run.
 */
export type RunResumer = (params: { workflowId: string; fromStep: string }) => Promise<void>;

/**
 * POST /api/runs/:id/resume — body `{ fromStep }`: continue an ended run
 * from one of its steps, keeping the results of the steps before it. 202 +
 * `Location` once the run is under way; 404 `not_found` for an unknown run
 * and `step_not_found` for a step the run's workflow doesn't have; 409
 * `run_not_terminal` for an in-flight run (resetting it would race its
 * driver) and `run_in_flight` when another driver holds it; 501 when the
 * storage can't reset steps or the workflows layer can't resume.
 */
export function resumeRun(params: {
  storage: WorkflowStorage;
  /** Resolves the run's workflow definition (in-process or advertised). */
  runs: RunsService;
  resume?: RunResumer;
}) {
  const { storage, runs, resume } = params;
  return async (req: Request, routeParams: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params: routeParams, schema: RunParamsSchema });
    const { fromStep } = await parseBody({ req, schema: ResumeRunRequestSchema });
    const state = await storage.loadWorkflow(id);
    if (!state) return jsonError(404, "not_found");
    if (!isTerminalWorkflowStatus(state.status)) {
      return jsonError(
        409,
        "run_not_terminal",
        `Only an ended run can be resumed (status: ${state.status})`,
      );
    }
    // A cancelled run stays cancelled: rerun starts it afresh.
    if (isCancelledRun(state)) {
      return jsonError(409, "run_cancelled", new RunCancelledError(id).message);
    }
    const defs = await runs.resolveStepDefs(state.workflowName);
    const known = defs.length > 0 ? defs.map((d) => d.name) : Object.keys(state.steps);
    if (!known.includes(fromStep)) {
      return jsonError(
        404,
        "step_not_found",
        `Workflow "${state.workflowName}" has no step "${fromStep}"`,
      );
    }
    if (!resume || !hasCapability(storage, "resetSteps")) {
      return jsonError(
        501,
        "resume_not_supported",
        "This server's workflows layer or storage cannot resume a run from a step",
      );
    }
    try {
      await resume({ workflowId: id, fromStep });
    } catch (err) {
      if (err instanceof WorkflowLockError) {
        return jsonError(409, "run_in_flight", err.message);
      }
      if (err instanceof RunCancelledError) return jsonError(409, "run_cancelled", err.message);
      throw err;
    }
    const response: ResumeRunResponse = { workflowId: id, fromStep };
    return accepted(response, runLocation(id));
  };
}
