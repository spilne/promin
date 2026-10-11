// ---------------------------------------------------------------------------
// WorkflowStartQueue — "please drive this run" records for worker-driven runs.
//
// A server that doesn't host a workflow's code pre-creates the run row (so a
// dashboard sees a `pending` run at once) and enqueues a start here. A worker
// that hosts the workflow claims the start, drives the whole run with the
// engine against shared storage, and completes the start once the run has
// ended or suspended. Wake-ups (a due sleep, a delivered signal), resumes and
// orphan recovery enqueue the run again; the queue keeps one record per run.
//
// Persistence: in-memory dies with the process. SQLite/Postgres backends
// survive restarts, so a trigger from the dashboard with no worker
// connected stays pending until one shows up.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

export interface WorkflowStartRecord {
  /** Queue-local id (used for `complete`). Distinct from `workflowId`. */
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  /** Workflow version of the run, if any. */
  readonly version?: string;
  /** Epoch ms. */
  readonly enqueuedAt: number;
  /**
   * Epoch ms. A pending record is not claimable before this time (set by
   * `release({ delayMs })`). Absent means claimable now.
   */
  readonly availableAt?: number;
  /** Epoch ms. Set when a worker claims the record. */
  readonly claimedAt?: number;
  /** Worker that claimed the record. */
  readonly claimedBy?: string;
  /**
   * Fencing token of the current claim, set on every claimed record.
   * `heartbeat`, `complete` and `release` only act when they carry it.
   */
  readonly claimToken?: string;
  /** Epoch ms. Last heartbeat of the current claim (the claim time until the first one). */
  readonly heartbeatAt?: number;
  /**
   * The run was enqueued again while this claim held it: completing the
   * claim puts the start back to pending instead of removing it, so the
   * run is driven once more.
   */
  readonly requeued?: boolean;
}

/** One claim of a start: the record id plus its claim token. */
export interface WorkflowStartClaimRef {
  readonly id: string;
  readonly claimToken: string;
}

/** Params of `WorkflowStartQueue.release`. */
export interface WorkflowStartReleaseParams extends WorkflowStartClaimRef {
  /** The start is claimable again this many ms from now. Default: 0 (at once). */
  readonly delayMs?: number;
}

/** Params of `WorkflowStartQueue.enqueue`. */
export interface WorkflowStartEnqueueParams {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  readonly version?: string;
  /**
   * Leave a pending start's `availableAt` (a worker's release delay) as it
   * is. Default: the start becomes claimable at once — a resume, rerun or
   * delivered signal must not wait out a delay a worker set for a sleep.
   * A scheduled wake-up passes true: the worker that set the delay knows
   * the sleep isn't due yet by its clock.
   */
  readonly keepAvailableAt?: boolean;
}

/** Answer of `WorkflowStartQueue.enqueue`. */
export interface WorkflowStartEnqueueResult {
  /** Id of the record holding the start: the new one, or the one already queued. */
  readonly id: string;
  /** False when a record for the `workflowId` already existed and nothing was added. */
  readonly created: boolean;
}

/** Params of `WorkflowStartQueue.claim`. */
export interface WorkflowStartClaimParams {
  readonly workflowSpecs: readonly WorkerWorkflowSpec[];
  readonly workerId?: string;
  readonly limit: number;
  /**
   * An id for this claim call, so a retry of a call whose answer was lost
   * gets the same starts back instead of new ones (their claims would go
   * stale unseen). Letters, digits, `-` and `_` only. A retry with the id of
   * an earlier call returns the starts that call claimed and still holds.
   */
  readonly claimId?: string;
}

/** Per-name set of versions a worker can run. */
export interface WorkerWorkflowSpec {
  readonly name: string;
  /** Versions the worker can serve. Empty means "any version". */
  readonly versions: readonly string[];
}

/**
 * Default reclaim window: the runner's default run-lock lease (120 s), so a
 * start whose worker died becomes claimable about when the run's lock
 * expires.
 */
export const DEFAULT_START_RECLAIM_AFTER_MS = 120_000;

/** Shape of a `claimId`: safe inside a claim token and a SQL LIKE pattern. */
export const CLAIM_ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;

/**
 * A queue of "please drive this run" records. A worker that hosts the
 * workflow claims a start, drives the run with the full engine, and
 * completes the start once the run has ended or suspended. One record per
 * `workflowId` at most.
 */
export interface WorkflowStartQueue {
  /**
   * Queue a start. Idempotent on `workflowId`: while a record for the run
   * exists, returns its id with `created: false`. A pending record becomes
   * claimable at once (unless `keepAvailableAt`); a claimed one is marked
   * `requeued`, so the claim's `complete` puts it back to pending — a
   * wake-up or resume that lands while a worker still holds the previous
   * start is never lost. The check and the mark are one atomic step with
   * `complete`.
   */
  enqueue(params: WorkflowStartEnqueueParams): Promise<WorkflowStartEnqueueResult>;
  /**
   * Claim up to `limit` pending starts the worker can run, oldest first. A
   * start matches a spec when the names match AND (the start has no
   * version, the spec lists no versions, or the start's version is in the
   * spec's list). A start released with a delay is skipped until its
   * `availableAt`. Each claimed record carries a fresh `claimToken`. With a
   * `claimId` the call is idempotent (see `WorkflowStartClaimParams`).
   *
   * A claim whose last heartbeat (or the claim itself, before the first
   * heartbeat) is older than the queue's reclaim window goes back to
   * pending and can be claimed again under a new token, so a dead worker
   * doesn't strand a start. A worker running a start for longer than that
   * window must `heartbeat` it.
   */
  claim(params: WorkflowStartClaimParams): Promise<WorkflowStartRecord[]>;
  /**
   * Keep a claim alive. Returns false when the claim is no longer current
   * (it went stale and was reclaimed, or was completed or released): the
   * worker has lost the start.
   */
  heartbeat(params: WorkflowStartClaimRef): Promise<boolean>;
  /**
   * The claimed start is done: removed, or — when the run was enqueued
   * again meanwhile (`requeued`) — put back to pending for another drive.
   * Only the current claim can complete it: with a stale token or an
   * unknown id it changes nothing and returns false.
   */
  complete(params: WorkflowStartClaimRef): Promise<boolean>;
  /**
   * Give a claimed start back without completing it: it is pending again
   * and claimable once `delayMs` has passed. Fenced like `complete`.
   */
  release(params: WorkflowStartReleaseParams): Promise<boolean>;
  /** Snapshot, oldest first — used by `GET /api/worker-protocol/starts` for debugging. */
  list(): Promise<WorkflowStartRecord[]>;
}

export interface InMemoryWorkflowStartQueueConfig {
  /**
   * A claim with no heartbeat for this many ms is re-claimable. Default:
   * `DEFAULT_START_RECLAIM_AFTER_MS` (the default run-lock lease).
   */
  reclaimAfterMs?: number;
  /** Time source for enqueue / claim stamps and the reclaim cutoff. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/** True when a start of `version` can run on a worker advertising `spec`. */
export function startMatchesSpec(params: {
  readonly version: string | undefined | null;
  readonly spec: WorkerWorkflowSpec;
}): boolean {
  const { version, spec } = params;
  return !version || spec.versions.length === 0 || spec.versions.includes(version);
}

/** The claim token of the `index`-th start claimed under `claimId` (or a random one). */
export function claimTokenFor(params: { claimId?: string; index: number }): string {
  if (params.claimId === undefined) return crypto.randomUUID();
  if (!CLAIM_ID_PATTERN.test(params.claimId)) {
    throw new Error(`invalid claimId "${params.claimId}": use letters, digits, - and _`);
  }
  return `${params.claimId}:${params.index}:${crypto.randomUUID()}`;
}

/** True when `token` was minted for a claim call with `claimId`. */
export function isTokenOfClaim(params: {
  token: string | undefined | null;
  claimId: string;
}): boolean {
  return params.token?.startsWith(`${params.claimId}:`) ?? false;
}

export class InMemoryWorkflowStartQueue implements WorkflowStartQueue {
  /** Records by id. */
  private readonly records = new Map<string, WorkflowStartRecord>();
  /** Record id by workflow id: one record per run. */
  private readonly byWorkflowId = new Map<string, string>();
  private readonly reclaimAfterMs: number;
  private readonly clock: WallClock;
  private seq = 0;

  constructor(config: InMemoryWorkflowStartQueueConfig = {}) {
    this.reclaimAfterMs = config.reclaimAfterMs ?? DEFAULT_START_RECLAIM_AFTER_MS;
    this.clock = config.clock ?? SystemWallClock;
  }

  async enqueue(params: WorkflowStartEnqueueParams): Promise<WorkflowStartEnqueueResult> {
    const existing = this.byWorkflowId.get(params.workflowId);
    if (existing !== undefined) {
      const rec = this.records.get(existing)!;
      if (rec.claimToken !== undefined) {
        this.records.set(existing, { ...rec, requeued: true });
      } else if (rec.availableAt !== undefined && !params.keepAvailableAt) {
        this.records.set(existing, unclaimed(rec));
      }
      return { id: existing, created: false };
    }
    const now = this.clock.currentTimeMs();
    this.seq += 1;
    const id = `start-${now.toString(36)}-${this.seq.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.records.set(id, {
      id,
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      input: params.input,
      ...(params.metadata !== undefined && { metadata: params.metadata }),
      ...(params.version !== undefined && { version: params.version }),
      enqueuedAt: now,
    });
    this.byWorkflowId.set(params.workflowId, id);
    return { id, created: true };
  }

  async claim(params: WorkflowStartClaimParams): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    if (params.claimId !== undefined) {
      const claimId = params.claimId;
      claimTokenFor({ claimId, index: 0 }); // validates the id
      const already = [...this.records.values()].filter((r) =>
        isTokenOfClaim({ token: r.claimToken, claimId }),
      );
      if (already.length > 0) return already;
    }
    const specByName = new Map<string, WorkerWorkflowSpec>();
    for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);
    const now = this.clock.currentTimeMs();
    const claimed: WorkflowStartRecord[] = [];
    // Map iteration is insertion (enqueue) order: oldest first.
    for (const rec of this.records.values()) {
      if (claimed.length >= params.limit) break;
      if (rec.claimToken !== undefined) continue;
      const spec = specByName.get(rec.workflowName);
      if (!spec || !startMatchesSpec({ version: rec.version, spec })) continue;
      if (rec.availableAt !== undefined && rec.availableAt > now) continue;
      const stamped: WorkflowStartRecord = {
        ...unclaimed(rec),
        claimedAt: now,
        heartbeatAt: now,
        claimToken: claimTokenFor({
          ...(params.claimId !== undefined && { claimId: params.claimId }),
          index: claimed.length,
        }),
        ...(params.workerId !== undefined && { claimedBy: params.workerId }),
      };
      this.records.set(rec.id, stamped);
      claimed.push(stamped);
    }
    return claimed;
  }

  async heartbeat(params: WorkflowStartClaimRef): Promise<boolean> {
    this.reclaimStale();
    const rec = this.current(params);
    if (!rec) return false;
    this.records.set(params.id, { ...rec, heartbeatAt: this.clock.currentTimeMs() });
    return true;
  }

  async complete(params: WorkflowStartClaimRef): Promise<boolean> {
    const rec = this.current(params);
    if (!rec) return false;
    if (rec.requeued) {
      this.records.set(params.id, unclaimed(rec));
      return true;
    }
    this.records.delete(params.id);
    this.byWorkflowId.delete(rec.workflowId);
    return true;
  }

  async release(params: WorkflowStartReleaseParams): Promise<boolean> {
    const rec = this.current(params);
    if (!rec) return false;
    const delayMs = Math.max(0, params.delayMs ?? 0);
    this.records.set(params.id, {
      ...unclaimed(rec),
      ...(delayMs > 0 && { availableAt: this.clock.currentTimeMs() + delayMs }),
    });
    return true;
  }

  async list(): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    return [...this.records.values()];
  }

  /** The record, if `ref` is its current claim. */
  private current(ref: WorkflowStartClaimRef): WorkflowStartRecord | undefined {
    const rec = this.records.get(ref.id);
    return rec && rec.claimToken === ref.claimToken ? rec : undefined;
  }

  /** Move stale claims back to pending so a dead worker doesn't strand a start. */
  private reclaimStale(): void {
    const cutoff = this.clock.currentTimeMs() - this.reclaimAfterMs;
    for (const [id, rec] of this.records) {
      if (rec.claimToken === undefined) continue;
      if ((rec.heartbeatAt ?? rec.claimedAt ?? 0) < cutoff) this.records.set(id, unclaimed(rec));
    }
  }
}

/** The record without its claim, availability and requeue fields: pending, claimable now. */
function unclaimed(rec: WorkflowStartRecord): WorkflowStartRecord {
  return {
    id: rec.id,
    workflowId: rec.workflowId,
    workflowName: rec.workflowName,
    input: rec.input,
    enqueuedAt: rec.enqueuedAt,
    ...(rec.metadata !== undefined && { metadata: rec.metadata }),
    ...(rec.version !== undefined && { version: rec.version }),
  };
}
