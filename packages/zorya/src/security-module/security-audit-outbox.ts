import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";
import { SECURITY_STORE_LIMITS } from "./security-store.ts";
import type {
  SecurityAuditOutboxRecord,
  SecurityAuditRecord,
  SecurityStore,
} from "./security-store.ts";

export interface SecurityAuditDestination {
  /** Must treat `idempotencyKey` as a destination-side deduplication key. */
  deliver(params: {
    readonly idempotencyKey: string;
    readonly record: SecurityAuditRecord;
  }): Promise<void>;
}

export interface SecurityAuditOutboxReport {
  readonly claimed: number;
  readonly delivered: number;
  readonly failed: number;
  readonly fenced: number;
  readonly missingRecords: number;
  readonly maxLagMs: number;
}

export interface SecurityAuditOutboxMetrics extends SecurityAuditOutboxReport {
  readonly runs: number;
  readonly lastRunAt?: number;
  readonly lastSuccessAt?: number;
  readonly lastFailureAt?: number;
}

export interface SecurityAuditOutboxWorkerConfig {
  readonly store: SecurityStore;
  readonly destination: SecurityAuditDestination;
  /** Stable replica identity recorded on leases. */
  readonly owner: string;
  readonly clock?: WallClock;
  readonly batchSize?: number;
  readonly leaseMs?: number;
  readonly pollIntervalMs?: number;
  readonly initialRetryMs?: number;
  readonly maxRetryMs?: number;
  readonly onReport?: (report: SecurityAuditOutboxReport) => void;
  /** Receives a payload-free failure classification; raw sink errors are never logged or stored. */
  readonly onWorkerError?: (failure: { readonly phase: "claim" }) => void;
}

/** Lease-fenced, restart-safe delivery of the durable security audit outbox. */
export class SecurityAuditOutboxWorker {
  private readonly store: SecurityStore;
  private readonly destination: SecurityAuditDestination;
  private readonly owner: string;
  private readonly clock: WallClock;
  private readonly batchSize: number;
  private readonly leaseMs: number;
  private readonly pollIntervalMs: number;
  private readonly initialRetryMs: number;
  private readonly maxRetryMs: number;
  private readonly onReport?: SecurityAuditOutboxWorkerConfig["onReport"];
  private readonly onWorkerError?: SecurityAuditOutboxWorkerConfig["onWorkerError"];
  private loopPromise?: Promise<void>;
  private stopRequested = false;
  private waitTimer?: TimerHandle;
  private waitResolve?: () => void;
  private totals: SecurityAuditOutboxMetrics = Object.freeze({
    runs: 0,
    claimed: 0,
    delivered: 0,
    failed: 0,
    fenced: 0,
    missingRecords: 0,
    maxLagMs: 0,
  });

  constructor(config: SecurityAuditOutboxWorkerConfig) {
    this.store = config.store;
    this.destination = config.destination;
    this.owner = required(config.owner, "audit outbox owner");
    this.clock = config.clock ?? SystemWallClock;
    this.batchSize = boundedPositive(
      config.batchSize ?? 100,
      "audit outbox batch size",
      SECURITY_STORE_LIMITS.outboxBatch,
    );
    this.leaseMs = positive(config.leaseMs ?? 30_000, "audit outbox lease");
    this.pollIntervalMs = positive(config.pollIntervalMs ?? 1_000, "audit outbox poll interval");
    this.initialRetryMs = positive(config.initialRetryMs ?? 1_000, "audit outbox retry");
    this.maxRetryMs = positive(config.maxRetryMs ?? 5 * 60_000, "audit outbox max retry");
    if (this.maxRetryMs < this.initialRetryMs) {
      throw new TypeError("audit outbox max retry must not be less than initial retry");
    }
    this.onReport = config.onReport;
    this.onWorkerError = config.onWorkerError;
  }

  metrics(): SecurityAuditOutboxMetrics {
    return this.totals;
  }

  async start(): Promise<void> {
    if (this.loopPromise !== undefined) return;
    this.stopRequested = false;
    const loop = this.runLoop().finally(() => {
      if (this.loopPromise === loop) this.loopPromise = undefined;
    });
    this.loopPromise = loop;
  }

  async stop(): Promise<void> {
    this.stopRequested = true;
    this.cancelWait();
    await this.loopPromise;
  }

  wake(): void {
    this.cancelWait();
  }

  async runOnce(): Promise<SecurityAuditOutboxReport> {
    const claimed = await this.store.claimAuditOutbox({
      owner: this.owner,
      leaseMs: this.leaseMs,
      limit: this.batchSize,
    });
    const report = {
      claimed: claimed.length,
      delivered: 0,
      failed: 0,
      fenced: 0,
      missingRecords: 0,
      maxLagMs: 0,
    };
    await Promise.all(claimed.map((lease) => this.deliverLease(lease, report)));
    const frozen = Object.freeze({ ...report });
    this.recordReport(frozen);
    return frozen;
  }

  private async deliverLease(
    lease: SecurityAuditOutboxRecord,
    report: {
      claimed: number;
      delivered: number;
      failed: number;
      fenced: number;
      missingRecords: number;
      maxLagMs: number;
    },
  ): Promise<void> {
    const leaseToken = lease.leaseToken;
    if (leaseToken === undefined) {
      report.fenced += 1;
      return;
    }
    const record = await this.store.getAuditRecord(lease.eventId);
    if (record === undefined) {
      report.failed += 1;
      report.missingRecords += 1;
      if (!(await this.release(lease, leaseToken, "audit_record_missing"))) report.fenced += 1;
      return;
    }
    report.maxLagMs = Math.max(
      report.maxLagMs,
      Math.max(0, this.clock.currentTimeMs() - record.recordedAt),
    );
    try {
      await this.destination.deliver({ idempotencyKey: lease.eventId, record });
      if (await this.store.ackAuditOutbox({ eventId: lease.eventId, leaseToken })) {
        report.delivered += 1;
      } else {
        report.fenced += 1;
      }
    } catch {
      report.failed += 1;
      if (!(await this.release(lease, leaseToken, "delivery_failed"))) report.fenced += 1;
    }
  }

  private async release(
    lease: SecurityAuditOutboxRecord,
    leaseToken: string,
    lastError: string,
  ): Promise<boolean> {
    const now = await this.store.currentTimeMs();
    return this.store.releaseAuditOutbox({
      eventId: lease.eventId,
      leaseToken,
      nextAttemptAt: safeSum(now, this.retryDelay(lease.attempts)),
      lastError,
    });
  }

  private retryDelay(attempts: number): number {
    const exponent = Math.min(Math.max(0, attempts - 1), 30);
    return Math.min(this.maxRetryMs, this.initialRetryMs * 2 ** exponent);
  }

  private recordReport(report: SecurityAuditOutboxReport): void {
    const now = this.clock.currentTimeMs();
    this.totals = Object.freeze({
      runs: this.totals.runs + 1,
      claimed: this.totals.claimed + report.claimed,
      delivered: this.totals.delivered + report.delivered,
      failed: this.totals.failed + report.failed,
      fenced: this.totals.fenced + report.fenced,
      missingRecords: this.totals.missingRecords + report.missingRecords,
      maxLagMs: Math.max(this.totals.maxLagMs, report.maxLagMs),
      lastRunAt: now,
      ...(report.delivered > 0
        ? { lastSuccessAt: now }
        : this.totals.lastSuccessAt !== undefined && { lastSuccessAt: this.totals.lastSuccessAt }),
      ...(report.failed > 0
        ? { lastFailureAt: now }
        : this.totals.lastFailureAt !== undefined && { lastFailureAt: this.totals.lastFailureAt }),
    });
    try {
      this.onReport?.(report);
    } catch {
      // Metrics must not break delivery.
    }
  }

  private async runLoop(): Promise<void> {
    while (!this.stopRequested) {
      try {
        const report = await this.runOnce();
        if (report.claimed === this.batchSize) continue;
      } catch {
        try {
          this.onWorkerError?.({ phase: "claim" });
        } catch {
          // Observability must not stop the worker.
        }
      }
      if (!this.stopRequested) await this.wait();
    }
  }

  private wait(): Promise<void> {
    return new Promise((resolve) => {
      const done = (): void => {
        this.waitTimer = undefined;
        this.waitResolve = undefined;
        resolve();
      };
      this.waitResolve = done;
      this.waitTimer = this.clock.setTimeout(done, this.pollIntervalMs);
      this.waitTimer.unref?.();
    });
  }

  private cancelWait(): void {
    this.waitTimer?.clear();
    this.waitTimer = undefined;
    const resolve = this.waitResolve;
    this.waitResolve = undefined;
    resolve?.();
  }
}

function required(value: string, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${label} must be a non-empty string`);
  }
  return value;
}

function positive(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${label} must be a positive safe integer`);
  }
  return value;
}

function boundedPositive(value: number, label: string, max: number): number {
  const checked = positive(value, label);
  if (checked > max) throw new TypeError(`${label} must not exceed ${max}`);
  return checked;
}

function safeSum(left: number, right: number): number {
  const value = left + right;
  if (!Number.isSafeInteger(value)) throw new TypeError("audit outbox retry time overflow");
  return value;
}
