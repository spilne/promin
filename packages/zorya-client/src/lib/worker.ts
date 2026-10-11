// ---------------------------------------------------------------------------
// ZoryaWorker — a process that hosts workflow code and drives whole runs.
//
// The worker advertises the workflows (and versions) it hosts, claims their
// starts from Zorya's start queue — only starts of a version it hosts — and
// drives each claimed run end to end with the engine (`createWorkflowRunner`)
// against Zorya's storage over `/rpc/storage`, fenced by the run lock. Zorya
// queues new runs, wake-ups (a due sleep, a delivered signal, a child run's
// end) and orphans for recovery; the worker just drives what it claims.
//
// Guarantees:
//   - at most `concurrency` runs are driven at once; a claim never takes
//     more starts than there are free slots
//   - a start is completed only once its run has ended or suspended; a run
//     that stopped short (its lock held elsewhere, a lost lock, a storage
//     outage) is released back to the queue, so it is driven again
//   - each claim is heartbeated; a lost claim aborts the run at its next
//     safe point (`run({ signal })`), so two workers never keep driving it
//   - `stop({ timeoutMs })` stops claiming, waits for runs in flight, then
//     abandons the rest and releases their starts for another worker
//   - start-up, advertising and every protocol call retry with backoff: a
//     worker started before Zorya, or across a Zorya restart, recovers on
//     its own and re-advertises
//   - every timer runs on the injected `WallClock`
//
// Step bodies are at-least-once: a run resumed after a crash re-runs the
// step that was in flight. Make them idempotent.
// ---------------------------------------------------------------------------

import {
  SystemWallClock,
  createWorkflowRunner,
  hasQueryHandlers,
  invokeQueryHandler,
  isTerminalWorkflowStatus,
  listQueryHandlers,
  workflowClosure,
  type RunLockOptions,
  type WallClock,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import { PollLoop, describeError, isNetworkError } from "@promin/workflow/distributed";
import type { WorkflowStartClaim, ZoryaClient } from "./zorya-client.ts";
import { WorkerControlSocket } from "./worker-control-socket.ts";

/** Which part of the worker a reported error came from. */
export type ZoryaWorkerErrorPhase = "advertise" | "claim" | "heartbeat" | "settle" | "run" | "stop";

export interface ZoryaWorkerErrorEvent {
  readonly phase: ZoryaWorkerErrorPhase;
  readonly error: unknown;
  /** The run concerned, when there is one. */
  readonly workflowId?: string;
}

export interface ZoryaWorkerConfig {
  client: ZoryaClient;
  /**
   * Workflow definitions this worker hosts. Their `previousVersions` and the
   * children of their `.subworkflow()` steps are hosted too.
   */
  workflows: ReadonlyArray<Workflow<unknown, unknown>>;
  /** Stable worker id. Default: random UUID. */
  workerId?: string;
  /** Capability tags, recorded in the worker registry. */
  capabilities?: readonly string[];
  /** Most runs driven at once; claims never exceed the free slots. Default 10. */
  concurrency?: number;
  /**
   * Registry heartbeat and re-advertise cadence (ms). Keep it well under the
   * server's advertisement TTL (60 s by default). Default 10_000.
   */
  heartbeatIntervalMs?: number;
  /**
   * Start-queue polling. `intervalMs` (default 1000) is the idle poll
   * cadence; a claim that filled every free slot polls again at once.
   * `claimHeartbeatMs` (default 30_000) keeps each claim alive; keep it well
   * under the server queue's reclaim window (120 s by default). `false`
   * disables claiming (the worker only advertises and serves control
   * commands).
   */
  pollWorkflowStarts?: false | { intervalMs?: number; claimHeartbeatMs?: number };
  /**
   * How long a start whose run is locked elsewhere waits before it can be
   * claimed again (ms). Default 5_000.
   */
  lockedRetryDelayMs?: number;
  /**
   * The run lock's lease (`WorkflowRunnerConfig.lock`). A run this worker
   * was driving when it died is recovered once the lease expires; keep the
   * server's start reclaim window at least this long. Default: the
   * runner's (120 s lease, 30 s heartbeat).
   */
  lock?: RunLockOptions;
  /** Time source for every timer. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Optional sample-input lookup for dashboard trigger forms. */
  sampleInput?: (workflowName: string) => unknown;
  /**
   * Application version, surfaced on the Workers page so operators can
   * spot stragglers during a rolling deploy.
   */
  version?: string;
  /** Free-form labels surfaced on the Workers page. */
  labels?: Record<string, string>;
  /** Namespaces this worker serves (informational). */
  namespaces?: readonly string[];
  /**
   * Raw metadata — merged with auto-detected fields (hostname, pid,
   * runtime). Anything passed here overrides the auto-detected defaults.
   */
  metadata?: Record<string, unknown>;
  /**
   * Opt in to the persistent worker → server WebSocket (`worker.control`),
   * used for agent event streaming and workflow query handlers. Default:
   * false.
   */
  controlSocket?:
    | boolean
    | {
        /** Initial reconnect delay. Default 250ms. */
        reconnectDelayMs?: number;
        /** Max reconnect delay. Default 30_000ms. */
        maxReconnectDelayMs?: number;
      };
  /** Background failures. Default: logs (network blips tersely). */
  onError?: (event: ZoryaWorkerErrorEvent) => void;
}

/** Abort reason for a run whose start claim was lost. */
export class StartClaimLostError extends Error {
  readonly _tag = "StartClaimLostError";
  constructor(workflowId: string) {
    super(`Lost the start claim of run "${workflowId}"; another worker owns it now`);
    this.name = "StartClaimLostError";
  }
}

/** Abort reason for runs abandoned by `stop({ timeoutMs })`. */
export class ZoryaWorkerStoppingError extends Error {
  readonly _tag = "ZoryaWorkerStoppingError";
  constructor() {
    super("The worker is stopping");
    this.name = "ZoryaWorkerStoppingError";
  }
}

/** A run this worker is driving. */
interface InFlightRun {
  readonly claim: WorkflowStartClaim;
  readonly abort: AbortController;
  /** Settles once the run stopped and its start was completed or released. */
  readonly done: Promise<void>;
  /** Set once the start must not be completed or released by the settle path. */
  settled: boolean;
}

/** Statuses a start is done for: the run ended or waits for a wake-up. */
function runIsAtRest(status: string): boolean {
  return status === "suspended" || isTerminalWorkflowStatus(status as never);
}

const SETTLE_RETRIES = 5;
/**
 * A run suspended on a sleep due within this long keeps its start until the
 * wake time instead of completing it (see `drive`).
 */
const HOLD_SUSPENDED_START_MS = 60_000;

export class ZoryaWorker {
  readonly workerId: string;
  readonly client: ZoryaClient;
  readonly runner: WorkflowRunner;
  /**
   * Persistent WS to the Zorya server. Defined when `config.controlSocket`
   * is truthy. Public so feature integrations (agent streaming, query
   * handlers) can register command handlers + send frames.
   */
  readonly control?: WorkerControlSocket;
  /**
   * Active agent streams hosted on this worker, keyed by workflowId. The
   * value subscribes an observer and returns an unsubscribe function.
   */
  readonly streams = new Map<string, (observer: (event: unknown) => void) => () => void>();
  /** streamId → unsubscribe handle for an active server-pushed agent stream. */
  private readonly activeStreams = new Map<string, () => void>();
  private readonly config: ZoryaWorkerConfig;
  private readonly clock: WallClock;
  private readonly concurrency: number;
  private readonly claimHeartbeatMs: number;
  private readonly lockedRetryDelayMs: number;
  /** Every definition hosted (workflows, previous versions, children). */
  private readonly hosted: Workflow<unknown, unknown>[];
  /** name → version → definition (`VERSIONLESS` for definitions without one). */
  private readonly byNameAndVersion: Map<
    string,
    Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>
  >;
  /** name → the definition passed in `config.workflows` (new runs without a version). */
  private readonly primary: Map<string, Workflow<unknown, unknown>>;
  private readonly presence: PollLoop;
  private readonly claims?: PollLoop;
  private readonly inFlight = new Map<string, InFlightRun>();
  private started = false;
  private stopping = false;
  private presenceTask?: Promise<void>;
  private claimTask?: Promise<void>;
  private advertised = false;
  private readyWaiters: Array<() => void> = [];
  private readonly startedAt: string;
  // Activity shown on the Workers page.
  private readonly activeRuns = new Map<string, { workflowName: string; startedAt: number }>();
  private completedCount = 0;
  private failedCount = 0;
  private readonly recentRuns: Array<{
    workflowId: string;
    workflowName: string;
    status: "completed" | "failed";
    durationMs: number;
    at: string;
  }> = [];
  private readonly RECENT_RUNS_CAP = 20;

  constructor(config: ZoryaWorkerConfig) {
    this.config = config;
    this.client = config.client;
    this.clock = config.clock ?? SystemWallClock;
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.concurrency = Math.max(1, Math.floor(config.concurrency ?? 10));
    const poll =
      config.pollWorkflowStarts === false ? undefined : (config.pollWorkflowStarts ?? {});
    this.claimHeartbeatMs = poll?.claimHeartbeatMs ?? 30_000;
    this.lockedRetryDelayMs = config.lockedRetryDelayMs ?? 5_000;
    this.hosted = workflowClosure(config.workflows);
    this.byNameAndVersion = buildVersionIndex(this.hosted);
    this.primary = new Map(config.workflows.map((w) => [w.name, w]));
    this.startedAt = this.clock.now().toISOString();
    this.runner = createWorkflowRunner({
      storage: config.client.storage,
      executorId: this.workerId,
      clock: this.clock,
      ...(config.lock !== undefined && { lock: config.lock }),
    });

    this.presence = new PollLoop({
      name: `zorya-worker ${this.workerId} presence`,
      intervalMs: config.heartbeatIntervalMs ?? 10_000,
      clock: this.clock,
      maxBackoffMs: 5_000,
      tick: () => this.refreshPresence(),
      onError: (error, info) => {
        if (info.consecutiveFailures === 1 || !isNetworkError(error)) {
          this.report({ phase: "advertise", error });
        }
      },
    });
    if (poll) {
      this.claims = new PollLoop({
        name: `zorya-worker ${this.workerId} claims`,
        intervalMs: poll.intervalMs ?? 1_000,
        clock: this.clock,
        maxBackoffMs: 5_000,
        tick: () => this.claimTick(),
        onError: (error, info) => {
          // The server is down: re-advertise as soon as it is back, since
          // a restarted server with in-memory stores forgot this worker.
          if (isNetworkError(error)) this.advertised = false;
          if (info.consecutiveFailures === 1 || !isNetworkError(error)) {
            this.report({ phase: "claim", error });
          }
        },
      });
    }

    if (config.controlSocket) {
      const csCfg = typeof config.controlSocket === "object" ? config.controlSocket : {};
      this.control = new WorkerControlSocket({
        url: this.client.url,
        workerId: this.workerId,
        capabilities: this.config.capabilities,
        apiKey: this.client.apiKey,
        reconnectDelayMs: csCfg.reconnectDelayMs,
        maxReconnectDelayMs: csCfg.maxReconnectDelayMs,
        clock: this.clock,
      });
      this.bindControlCommands(this.control);
    }
  }

  /**
   * Register a live event stream for a workflow this worker is hosting.
   * Returns the cleanup that unregisters it.
   */
  registerStream(
    workflowId: string,
    subscribe: (observer: (event: unknown) => void) => () => void,
  ): () => void {
    this.streams.set(workflowId, subscribe);
    return () => {
      if (this.streams.get(workflowId) === subscribe) this.streams.delete(workflowId);
    };
  }

  /**
   * Start advertising and claiming. Returns at once: a server that is not
   * up yet is retried in the background (`ready()` resolves once the
   * worker has registered and advertised).
   */
  start(): void {
    if (this.started) return;
    this.started = true;
    this.stopping = false;
    this.control?.start();
    this.presenceTask = this.presence.start();
    this.claimTask = this.claims?.start();
  }

  /** Resolves once the worker has registered and advertised at least once since `start()`. */
  ready(): Promise<void> {
    if (this.advertised) return Promise.resolve();
    return new Promise<void>((resolve) => this.readyWaiters.push(resolve));
  }

  /** Runs being driven right now. */
  get activeCount(): number {
    return this.inFlight.size;
  }

  /**
   * Stop claiming, wait for the runs in flight, deregister and unadvertise.
   * With `timeoutMs`, runs still going after that long are abandoned: each
   * stops at its next safe point and its start is released at once for
   * another worker (a step already running finishes; its result is kept
   * only if it lands before the run stops).
   */
  async stop(params: { timeoutMs?: number } = {}): Promise<void> {
    if (!this.started) return;
    this.started = false;
    this.stopping = true;
    await this.claims?.stop();
    await this.claimTask;

    const running = [...this.inFlight.values()].map((r) => r.done);
    if (running.length > 0) {
      if (params.timeoutMs === undefined) {
        await Promise.all(running);
      } else {
        await this.waitFor({ promises: running, timeoutMs: params.timeoutMs });
        for (const run of this.inFlight.values()) run.abort.abort(new ZoryaWorkerStoppingError());
        // Abandoned runs unwind at their next safe point and release their
        // starts; don't wait on a step that never ends.
        await this.waitFor({
          promises: [...this.inFlight.values()].map((r) => r.done),
          timeoutMs: params.timeoutMs,
        });
        await Promise.all(
          [...this.inFlight.values()].map((run) => this.releaseNow(run).catch(() => {})),
        );
      }
    }

    await this.presence.stop();
    await this.presenceTask;
    await this.control?.stop();
    await this.client.workerRegistry.deregister(this.workerId).catch((error: unknown) => {
      this.report({ phase: "stop", error });
    });
    await this.client.unadvertise(this.workerId).catch((error: unknown) => {
      this.report({ phase: "stop", error });
    });
    this.advertised = false;
  }

  /**
   * Run one workflow instance in this process with this worker's runner
   * (outside the start queue). Tracked on the Workers page.
   */
  async run(params: {
    workflow: string | Workflow<unknown, unknown>;
    workflowId?: string;
    input?: unknown;
  }): Promise<unknown> {
    const def =
      typeof params.workflow === "string" ? this.primary.get(params.workflow) : params.workflow;
    if (!def) throw new Error(`Unknown workflow "${String(params.workflow)}"`);
    const id = params.workflowId ?? `${def.name}-${this.clock.currentTimeMs().toString(36)}`;
    return this.tracked({
      workflowId: id,
      workflowName: def.name,
      run: () => this.runner.run({ workflow: def, workflowId: id, input: params.input }),
    });
  }

  // -------------------------------------------------------------------------
  // Presence: register + advertise, retried until it works, then periodic.
  // -------------------------------------------------------------------------

  private async refreshPresence(): Promise<void> {
    await this.client.workerRegistry.register({
      workerId: this.workerId,
      capabilities: this.config.capabilities ?? [],
      concurrency: this.concurrency,
      metadata: this.buildMetadata(),
    });
    await this.client.advertise({
      workerId: this.workerId,
      workflows: this.hosted,
      ...(this.config.sampleInput !== undefined && { sampleInput: this.config.sampleInput }),
    });
    if (!this.advertised) {
      this.advertised = true;
      const waiters = this.readyWaiters;
      this.readyWaiters = [];
      for (const wake of waiters) wake();
      this.claims?.wake();
    }
  }

  // -------------------------------------------------------------------------
  // Claims
  // -------------------------------------------------------------------------

  private async claimTick(): Promise<"again" | "idle"> {
    if (this.stopping) return "idle";
    if (!this.advertised) {
      // Re-advertise first (start-up, or the server came back).
      this.presence.wake();
      return "idle";
    }
    const free = this.concurrency - this.inFlight.size;
    if (free <= 0) return "idle";
    const limit = Math.min(free, 100);
    const claims = await this.client.claimWorkflowStarts({
      workflowSpecs: this.workflowSpecs(),
      workerId: this.workerId,
      limit,
    });
    for (const claim of claims) this.launch(claim);
    return claims.length === limit ? "again" : "idle";
  }

  private launch(claim: WorkflowStartClaim): void {
    const abort = new AbortController();
    const entry: InFlightRun = {
      claim,
      abort,
      settled: false,
      done: Promise.resolve(),
    };
    (entry as { done: Promise<void> }).done = this.drive(entry).finally(() => {
      this.inFlight.delete(claim.id);
      // A slot is free: claim again now instead of after the idle wait.
      this.claims?.wake();
    });
    this.inFlight.set(claim.id, entry);
  }

  /** Drive one claimed run, then complete or release its start. */
  private async drive(entry: InFlightRun): Promise<void> {
    const { claim, abort } = entry;
    const ref = { id: claim.id, claimToken: claim.claimToken };
    const def = this.resolveWorkflow({ name: claim.workflowName, version: claim.version });
    if (!def) {
      // The claim filter should never hand out a start this worker can't
      // run; give it back for a worker that can.
      this.report({
        phase: "run",
        workflowId: claim.workflowId,
        error: new Error(
          `no hosted definition for "${claim.workflowName}"` +
            (claim.version !== undefined ? ` version "${claim.version}"` : ""),
        ),
      });
      await this.settle({ entry, action: "release", delayMs: this.lockedRetryDelayMs });
      return;
    }

    const heartbeat = this.clock.setInterval(() => {
      void this.client.heartbeatWorkflowStart(ref).then(
        (current) => {
          if (current || abort.signal.aborted) return;
          entry.settled = true;
          abort.abort(new StartClaimLostError(claim.workflowId));
        },
        (error: unknown) =>
          this.report({ phase: "heartbeat", workflowId: claim.workflowId, error }),
      );
    }, this.claimHeartbeatMs);

    let outcome: { error: unknown } | undefined;
    try {
      await this.tracked({
        workflowId: claim.workflowId,
        workflowName: claim.workflowName,
        run: () =>
          this.runner.run({
            workflow: def,
            workflowId: claim.workflowId,
            input: claim.input,
            signal: abort.signal,
          }),
      });
    } catch (error) {
      outcome = { error };
    } finally {
      heartbeat.clear();
    }
    if (entry.settled) return; // claim lost: the new claimant owns the start

    const rest = await this.loadRest(claim.workflowId);
    if (rest !== undefined && runIsAtRest(rest.status)) {
      // Suspended on a sleep due soon (by this worker's clock): keep the
      // start, claimable at the wake time. Zorya's scan, on its own clock,
      // may already find the sleep due; completing would have it enqueue
      // the run again and again until this worker's clock agrees.
      const dueInMs =
        rest.status === "suspended" && rest.nextWakeAt !== undefined
          ? rest.nextWakeAt - this.clock.currentTimeMs()
          : undefined;
      if (dueInMs !== undefined && dueInMs > 0 && dueInMs <= HOLD_SUSPENDED_START_MS) {
        await this.settle({ entry, action: "release", delayMs: Math.ceil(dueInMs) });
        return;
      }
      await this.settle({ entry, action: "complete" });
      return;
    }
    // The run stopped short: its lock is held elsewhere, the lock was lost,
    // a checkpoint failed, or this worker abandoned it. Give the start back.
    const tag = (outcome?.error as { _tag?: string } | undefined)?._tag;
    if (tag !== "WorkflowLockError" && tag !== "WorkflowLockLostError" && outcome) {
      this.report({ phase: "run", workflowId: claim.workflowId, error: outcome.error });
    }
    const delayMs =
      tag === "WorkflowLockError" ? this.lockedRetryDelayMs : this.stopping ? 0 : 1_000;
    await this.settle({ entry, action: "release", delayMs });
  }

  /**
   * The run's status and, when it is parked on sleeps only, the earliest
   * wake time, retried with backoff; undefined when it can't be read.
   */
  private async loadRest(
    workflowId: string,
  ): Promise<{ status: string; nextWakeAt?: number } | undefined> {
    for (let attempt = 0; attempt < SETTLE_RETRIES; attempt++) {
      try {
        const state = await this.client.storage.loadWorkflow(workflowId);
        if (!state) return undefined;
        // Only a run parked on sleeps alone: a signal can arrive any time.
        const steps = Object.values(state.steps);
        let nextWakeAt: number | undefined;
        if (!steps.some((step) => step.status === "waiting_for_signal")) {
          for (const step of steps) {
            const at = step.status === "sleeping" ? step.wakeAt : undefined;
            if (at && (nextWakeAt === undefined || at.getTime() < nextWakeAt)) {
              nextWakeAt = at.getTime();
            }
          }
        }
        return { status: state.status, ...(nextWakeAt !== undefined && { nextWakeAt }) };
      } catch (error) {
        if (attempt === SETTLE_RETRIES - 1) {
          this.report({ phase: "settle", workflowId, error });
          return undefined;
        }
        await this.sleep(250 * 2 ** attempt);
      }
    }
    return undefined;
  }

  /**
   * Complete or release the start, retried with backoff. A start that still
   * can't be settled is left to the server: its claim goes stale, and the
   * start is claimed again.
   */
  private async settle(params: {
    entry: InFlightRun;
    action: "complete" | "release";
    delayMs?: number;
  }): Promise<void> {
    const { entry, action } = params;
    const ref = { id: entry.claim.id, claimToken: entry.claim.claimToken };
    for (let attempt = 0; attempt < SETTLE_RETRIES && !entry.settled; attempt++) {
      try {
        if (action === "complete") await this.client.completeWorkflowStart(ref);
        else {
          await this.client.releaseWorkflowStart({
            ...ref,
            ...(params.delayMs !== undefined && { delayMs: params.delayMs }),
          });
        }
        entry.settled = true;
        return;
      } catch (error) {
        if (attempt === SETTLE_RETRIES - 1) {
          this.report({ phase: "settle", workflowId: entry.claim.workflowId, error });
          return;
        }
        await this.sleep(250 * 2 ** attempt);
      }
    }
  }

  /** Release a run's start at once (stop timeout); the run unwinds on its own. */
  private async releaseNow(entry: InFlightRun): Promise<void> {
    if (entry.settled) return;
    entry.settled = true;
    await this.client.releaseWorkflowStart({
      id: entry.claim.id,
      claimToken: entry.claim.claimToken,
      delayMs: 0,
    });
  }

  // -------------------------------------------------------------------------
  // Definitions and versions
  // -------------------------------------------------------------------------

  /**
   * The (name, versions) the claim filter routes by. A workflow hosted
   * without an explicit version serves any version (`versions: []`).
   */
  private workflowSpecs(): Array<{ name: string; versions: readonly string[] }> {
    const out: Array<{ name: string; versions: readonly string[] }> = [];
    for (const [name, byVersion] of this.byNameAndVersion) {
      const explicit: string[] = [];
      let hasVersionless = false;
      for (const key of byVersion.keys()) {
        if (key === VERSIONLESS) hasVersionless = true;
        else explicit.push(key);
      }
      out.push({ name, versions: hasVersionless ? [] : explicit });
    }
    return out;
  }

  /**
   * The definition for a run of `name` at `version`: exactly that version,
   * else a versionless definition; a run without a version gets the
   * primary definition. Never a definition picked by step name.
   */
  private resolveWorkflow(params: {
    name: string;
    version: string | undefined;
  }): Workflow<unknown, unknown> | undefined {
    const byVersion = this.byNameAndVersion.get(params.name);
    if (!byVersion) return undefined;
    if (params.version !== undefined) {
      return byVersion.get(params.version) ?? byVersion.get(VERSIONLESS);
    }
    return (
      this.primary.get(params.name) ?? byVersion.get(VERSIONLESS) ?? [...byVersion.values()][0]
    );
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Run `run`, tracking it on the Workers page and in the outcome counters. */
  private async tracked<T>(params: {
    workflowId: string;
    workflowName: string;
    run: () => Promise<T>;
  }): Promise<T> {
    const startedAt = this.clock.currentTimeMs();
    this.activeRuns.set(params.workflowId, { workflowName: params.workflowName, startedAt });
    try {
      const result = await params.run();
      this.completedCount += 1;
      this.recordRecent({ ...params, status: "completed", startedAt });
      return result;
    } catch (err) {
      const tag = (err as { _tag?: string } | undefined)?._tag;
      // A suspension, a lock held elsewhere or an abandoned run is not a failure.
      if (
        tag !== "WorkflowSuspendedError" &&
        tag !== "WorkflowLockError" &&
        tag !== "WorkflowLockLostError"
      ) {
        this.failedCount += 1;
        this.recordRecent({ ...params, status: "failed", startedAt });
      }
      throw err;
    } finally {
      this.activeRuns.delete(params.workflowId);
    }
  }

  private recordRecent(params: {
    workflowId: string;
    workflowName: string;
    status: "completed" | "failed";
    startedAt: number;
  }): void {
    this.recentRuns.unshift({
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      status: params.status,
      durationMs: this.clock.currentTimeMs() - params.startedAt,
      at: this.clock.now().toISOString(),
    });
    if (this.recentRuns.length > this.RECENT_RUNS_CAP) {
      this.recentRuns.length = this.RECENT_RUNS_CAP;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => this.clock.setTimeout(resolve, ms));
  }

  /** Wait for all `promises`, or `timeoutMs`, whichever comes first. */
  private async waitFor(params: { promises: Promise<void>[]; timeoutMs: number }): Promise<void> {
    let timer: { clear(): void } | undefined;
    const timeout = new Promise<void>((resolve) => {
      timer = this.clock.setTimeout(resolve, params.timeoutMs);
    });
    await Promise.race([Promise.all(params.promises).then(() => undefined), timeout]);
    timer?.clear();
  }

  private report(event: ZoryaWorkerErrorEvent): void {
    try {
      if (this.config.onError) this.config.onError(event);
      else defaultOnError({ workerId: this.workerId, event });
    } catch {
      // A throwing error hook must not take a loop down.
    }
  }

  /**
   * The metadata record sent on register: auto-detected runtime info plus
   * live activity, overridden by user-supplied metadata on key collisions.
   */
  private buildMetadata(): Record<string, unknown> {
    const cfg = this.config;
    const auto: Record<string, unknown> = {
      workflowNames: [...new Set(this.hosted.map((w) => w.name))],
      workflowVersions: [
        ...new Set(this.hosted.map((w) => w.version).filter((v): v is string => !!v)),
      ],
      capabilities: cfg.capabilities ?? [],
      concurrency: this.concurrency,
      startedAt: this.startedAt,
      runtime: detectRuntime(),
      activeRuns: [...this.activeRuns.entries()].map(([id, rec]) => ({
        workflowId: id,
        workflowName: rec.workflowName,
        startedAt: new Date(rec.startedAt).toISOString(),
      })),
      activeCount: this.activeRuns.size,
      completedCount: this.completedCount,
      failedCount: this.failedCount,
      recentRuns: [...this.recentRuns],
    };
    if (typeof process !== "undefined" && typeof process.pid === "number") {
      auto.pid = process.pid;
    }
    const hostname = tryGetHostname();
    if (hostname) auto.hostname = hostname;
    if (cfg.version) auto.version = cfg.version;
    if (cfg.labels && Object.keys(cfg.labels).length > 0) auto.labels = cfg.labels;
    if (cfg.namespaces && cfg.namespaces.length > 0) auto.namespaces = [...cfg.namespaces];
    return { ...auto, ...cfg.metadata };
  }

  private bindControlCommands(control: WorkerControlSocket): void {
    // Agent streams: the server's AgentStreamHub broadcasts these when a
    // dashboard SSE client subscribes / unsubscribes. Workers that aren't
    // hosting the workflow answer `{ hosted: false }`.
    control.onCommand("agent-stream-start", (args) => {
      const { streamId, workflowId } = args as { streamId: string; workflowId: string };
      const subscribe = this.streams.get(workflowId);
      if (!subscribe) return { hosted: false };
      this.activeStreams.get(streamId)?.();
      const unsub = subscribe((event) => {
        this.control?.sendFrame(streamId, event);
      });
      this.activeStreams.set(streamId, unsub);
      return { hosted: true };
    });
    control.onCommand("agent-stream-stop", (args) => {
      const { streamId } = args as { streamId: string };
      const unsub = this.activeStreams.get(streamId);
      if (unsub) {
        unsub();
        this.activeStreams.delete(streamId);
      }
      return { ok: true };
    });
    // Query handlers registered by a run this worker drives
    // (`ctx.setQueryHandler`). `{ hosted: false }` lets the server's
    // broadcast find the worker that drives the run.
    control.onCommand("query", async (args) => {
      const {
        workflowId,
        name,
        args: queryArgs,
      } = args as { workflowId: string; name: string; args?: unknown };
      if (!hasQueryHandlers(workflowId)) return { hosted: false };
      const result = await invokeQueryHandler({ workflowId, name, args: queryArgs });
      return { hosted: true, result };
    });
    control.onCommand("query-list", (args) => {
      const { workflowId } = args as { workflowId: string };
      if (!hasQueryHandlers(workflowId)) return { hosted: false };
      return { hosted: true, handlers: listQueryHandlers(workflowId) };
    });
  }
}

/**
 * Sentinel for workflows built without an explicit `version`. The runtime
 * doesn't substitute a default — it skips the version-mismatch check — so
 * these keep their own bucket rather than pretending to be version "1".
 */
const VERSIONLESS = Symbol("versionless");

/** `name → version → definition` over every hosted definition. */
function buildVersionIndex(
  workflows: ReadonlyArray<Workflow<unknown, unknown>>,
): Map<string, Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>> {
  const out = new Map<string, Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>>();
  for (const def of workflows) {
    let inner = out.get(def.name);
    if (!inner) {
      inner = new Map();
      out.set(def.name, inner);
    }
    const key = def.version ?? VERSIONLESS;
    if (!inner.has(key)) inner.set(key, def);
  }
  return out;
}

function defaultOnError(params: { workerId: string; event: ZoryaWorkerErrorEvent }): void {
  const { workerId, event } = params;
  const where = event.workflowId ? ` (run ${event.workflowId})` : "";
  if (isNetworkError(event.error)) {
    console.warn(
      `[zorya-worker ${workerId}] ${event.phase}${where}: server unreachable, retrying — ${describeError(event.error)}`,
    );
    return;
  }
  console.error(`[zorya-worker ${workerId}] ${event.phase} failed${where}:`, event.error);
}

function detectRuntime(): string {
  const bunVersion = (globalThis as { Bun?: { version?: string } }).Bun?.version;
  if (bunVersion) return `bun-${bunVersion}`;
  const nodeVersion = (globalThis as { process?: { versions?: { node?: string } } }).process
    ?.versions?.node;
  if (nodeVersion) return `node-${nodeVersion}`;
  return "unknown";
}

function tryGetHostname(): string | undefined {
  try {
    // Required lazily so browser bundles that include this module don't fail at load.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const os = require("node:os") as { hostname?: () => string };
    return os.hostname?.();
  } catch {
    return undefined;
  }
}
