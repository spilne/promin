// ---------------------------------------------------------------------------
// Workers route — /api/workers
//
// Returns the list of registered workers via a pluggable provider. Zorya
// itself doesn't manage a worker registry; the embedder passes one in,
// OR the worker protocol layer auto-wires one from ZoryaServerConfig
// .workerProtocol.workerRegistry (see RegistryBackedWorkersProvider).
// ---------------------------------------------------------------------------

import type { WorkerInfo, WorkerRegistry } from "@promin/workflow";
import { json } from "../router.ts";
import type { WorkerDto, WorkersResponse } from "../api-types.ts";

export interface WorkersProvider {
  listWorkers(): Promise<WorkerDto[]>;
}

export const emptyWorkersProvider: WorkersProvider = {
  listWorkers: async () => [],
};

export function listWorkers(provider: WorkersProvider) {
  return async (): Promise<Response> => {
    const workers = await provider.listWorkers();
    const response: WorkersResponse = { workers };
    return json(200, response);
  };
}

/**
 * Adapts a WorkerRegistry into a WorkersProvider for the UI. Unpacks the
 * rich metadata ZoryaWorker writes (activeRuns, recentRuns, capabilities,
 * hostname, runtime, etc.) and exposes it on WorkerDto. Falls back to
 * sensible defaults when a field isn't present, so workers written by
 * other languages that only send the basics still render.
 */
export class RegistryBackedWorkersProvider implements WorkersProvider {
  constructor(
    private readonly registry: WorkerRegistry,
    /** Age in ms after which a worker is considered offline. Default 30s. */
    private readonly offlineAfterMs = 30_000,
  ) {}

  async listWorkers(): Promise<WorkerDto[]> {
    const entries = await this.registry.list();
    const now = Date.now();
    return entries.map((e) => this.toDto(e, now));
  }

  private toDto(info: WorkerInfo, now: number): WorkerDto {
    const meta = (info.metadata ?? {}) as Record<string, unknown>;
    const lastHb = info.lastHeartbeat.getTime();
    // A retired worker is its own state — never collapse it into
    // `offline`, which it would otherwise share with crashed / stale
    // workers.
    const online = info.status === "active" && now - lastHb < this.offlineAfterMs;
    const status: WorkerDto["status"] =
      info.status === "retired" ? "retired" : online ? "online" : "offline";

    return {
      workerId: info.workerId,
      status,
      ...(info.retiredAt ? { retiredAt: info.retiredAt.toISOString() } : {}),
      lastHeartbeatAt: info.lastHeartbeat.toISOString(),
      capabilities: info.capabilities,
      concurrency: info.concurrency,
      workflowNames: asStringArray(meta.workflowNames),
      workflowVersions: asStringArray(meta.workflowVersions),
      version: asString(meta.version),
      hostname: asString(meta.hostname),
      runtime: asString(meta.runtime),
      startedAt: asString(meta.startedAt) ?? info.startedAt.toISOString(),
      namespaces: asStringArray(meta.namespaces),
      labels: asLabels(meta.labels),
      activeTasks: asNumber(meta.activeCount) ?? 0,
      completedCount: asNumber(meta.completedCount),
      failedCount: asNumber(meta.failedCount),
      completedToday: asNumber(meta.completedCount) ?? 0,
      activeRuns: asActiveRuns(meta.activeRuns),
      recentRuns: asRecentRuns(meta.recentRuns),
    };
  }
}

function asString(v: unknown): string | undefined {
  return typeof v === "string" ? v : undefined;
}

function asNumber(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function asStringArray(v: unknown): readonly string[] | undefined {
  if (!Array.isArray(v)) return undefined;
  const out = v.filter((x): x is string => typeof x === "string");
  return out.length > 0 ? out : undefined;
}

function asLabels(v: unknown): Record<string, string> | undefined {
  if (v === null || typeof v !== "object") return undefined;
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    if (typeof val === "string") out[k] = val;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function asActiveRuns(v: unknown): WorkerDto["activeRuns"] {
  if (!Array.isArray(v)) return undefined;
  const out: NonNullable<WorkerDto["activeRuns"]>[number][] = [];
  for (const entry of v) {
    if (entry === null || typeof entry !== "object") continue;
    const obj = entry as Record<string, unknown>;
    if (typeof obj.workflowId !== "string" || typeof obj.workflowName !== "string") continue;
    if (typeof obj.startedAt !== "string") continue;
    out.push({
      workflowId: obj.workflowId,
      workflowName: obj.workflowName,
      startedAt: obj.startedAt,
    });
  }
  return out.length > 0 ? out : undefined;
}

function asRecentRuns(v: unknown): WorkerDto["recentRuns"] {
  if (!Array.isArray(v)) return undefined;
  const out: NonNullable<WorkerDto["recentRuns"]>[number][] = [];
  for (const entry of v) {
    if (entry === null || typeof entry !== "object") continue;
    const obj = entry as Record<string, unknown>;
    if (
      typeof obj.workflowId !== "string" ||
      typeof obj.workflowName !== "string" ||
      typeof obj.at !== "string" ||
      typeof obj.durationMs !== "number"
    ) {
      continue;
    }
    if (obj.status !== "completed" && obj.status !== "failed") continue;
    out.push({
      workflowId: obj.workflowId,
      workflowName: obj.workflowName,
      status: obj.status,
      durationMs: obj.durationMs,
      at: obj.at,
    });
  }
  return out.length > 0 ? out : undefined;
}
