// ---------------------------------------------------------------------------
// Background maintenance shared by the workflow layers: a leader-elected
// periodic loop, and the orphan listing both execution modes recover from.
//
// An orphan is a `pending`, `running` or `compensating` run nobody drives:
// its lock is free or expired, and it has not changed for a grace period.
// A run with a live lock is never an orphan, so recovery never touches a
// run another instance or worker is driving.
// ---------------------------------------------------------------------------

import {
  hasCapability,
  isTerminalWorkflowStatus,
  wakeParentOfEndedRun,
  type WallClock,
  type WorkflowStorage,
} from "@promin/workflow";
import { CHILD_ENDED_SIGNAL_PREFIX } from "@promin/workflow/storage-kit";
import { PollLoop, type LeaderElection } from "@promin/workflow/distributed";

/** A run found by `forEachOrphanedRun`. */
export interface OrphanRef {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
}

const ORPHAN_PAGE_SIZE = 200;
const ORPHAN_STATUSES = ["pending", "running", "compensating"] as const;

/**
 * Visit every orphaned run, one at a time, page by page (keyset on
 * `workflowId` where the storage has `listOrphanedRuns`). Resolves with how
 * many runs were visited. Without `listOrphanedRuns` the storage cannot
 * tell a live lock apart, so runs are picked by age alone; whatever the
 * visitor does must then still be arbitrated by the run lock.
 */
export async function forEachOrphanedRun(params: {
  readonly storage: WorkflowStorage;
  readonly clock: WallClock;
  /** Leave runs changed within this window alone (a trigger still enqueueing, a driver starting). */
  readonly graceMs: number;
  readonly visit: (run: OrphanRef) => Promise<void>;
  /** Stop early (e.g. on shutdown). */
  readonly stopped?: () => boolean;
  /**
   * Checked before each page: false stops the sweep (e.g. this instance
   * lost its leadership mid-sweep). Renewing a lease here keeps a long
   * sweep from overlapping the next leader's.
   */
  readonly keepGoing?: () => Promise<boolean>;
}): Promise<number> {
  const { storage, clock, graceMs, visit } = params;
  const now = clock.now();
  const updatedBefore = new Date(now.getTime() - graceMs);
  let visited = 0;
  if (storage.listOrphanedRuns) {
    let afterWorkflowId: string | undefined;
    while (!params.stopped?.()) {
      if (params.keepGoing && !(await params.keepGoing())) break;
      const page = await storage.listOrphanedRuns({
        now,
        updatedBefore,
        limit: ORPHAN_PAGE_SIZE,
        ...(afterWorkflowId !== undefined && { afterWorkflowId }),
      });
      for (const run of page) {
        if (params.stopped?.()) break;
        await visit(run);
        visited++;
      }
      if (page.length < ORPHAN_PAGE_SIZE) break;
      afterWorkflowId = page[page.length - 1]!.workflowId;
    }
    return visited;
  }
  for (const status of ORPHAN_STATUSES) {
    // Snapshot every page first: visiting may change statuses and shift offsets.
    const found: OrphanRef[] = [];
    for (let offset = 0; ; offset += ORPHAN_PAGE_SIZE) {
      const page = await storage.listWorkflows({
        status,
        limit: ORPHAN_PAGE_SIZE,
        offset,
        orderBy: "createdAt",
        orderDir: "asc",
      });
      for (const wf of page) {
        if (wf.updatedAt.getTime() < updatedBefore.getTime()) found.push(wf);
      }
      if (page.length < ORPHAN_PAGE_SIZE) break;
    }
    for (const [i, run] of found.entries()) {
      if (params.stopped?.()) return visited;
      if (i % ORPHAN_PAGE_SIZE === 0 && params.keepGoing && !(await params.keepGoing())) {
        return visited;
      }
      await visit(run);
      visited++;
    }
  }
  return visited;
}

/**
 * Re-deliver lost child-ended wake-ups. A parent parked on a child waits
 * for the reserved `workflow.child-ended:<childId>#<run>` signal, which the
 * child's driver delivers when the child ends — best effort: a driver that
 * died, or a storage outage that outlasted its retries, drops it, and a
 * parent with no timeout of its own would wait forever. This finds
 * suspended parents waiting on a child that has ended and delivers the
 * signal again (a no-op for one already delivered: last delivery wins).
 * Resolves with how many signals it delivered.
 *
 * With `listChildEndedWaits` the storage hands over only parents parked on
 * an undelivered child wake-up, keyset-paged; without it every suspended
 * run is listed.
 */
export async function redeliverChildEndedSignals(params: {
  readonly storage: WorkflowStorage;
  readonly stopped?: () => boolean;
  /** Checked every page: false stops (see `forEachOrphanedRun`). */
  readonly keepGoing?: () => Promise<boolean>;
}): Promise<number> {
  const { storage } = params;
  let delivered = 0;
  const redeliver = async (parentId: string, signalName: string): Promise<void> => {
    const ref = signalName.slice(CHILD_ENDED_SIGNAL_PREFIX.length);
    const at = ref.lastIndexOf("#");
    if (at <= 0) return;
    const child = await storage.loadWorkflow(ref.slice(0, at));
    if (!child || child.run !== Number(ref.slice(at + 1))) return;
    if (!isTerminalWorkflowStatus(child.status)) return;
    const signals = await storage.loadSignals(parentId);
    if (signals.some((s) => s.signalName === signalName)) return;
    await wakeParentOfEndedRun({ storage, state: child });
    delivered++;
  };

  if (hasCapability(storage, "childEndedWaits")) {
    let afterWorkflowId: string | undefined;
    while (!params.stopped?.()) {
      if (params.keepGoing && !(await params.keepGoing())) break;
      const page = await storage.listChildEndedWaits({
        limit: ORPHAN_PAGE_SIZE,
        ...(afterWorkflowId !== undefined && { afterWorkflowId }),
      });
      for (const run of page) {
        for (const wait of run.waits) {
          if (params.stopped?.()) return delivered;
          await redeliver(run.workflowId, wait.signalName);
        }
      }
      if (page.length < ORPHAN_PAGE_SIZE) break;
      afterWorkflowId = page[page.length - 1]!.workflowId;
    }
    return delivered;
  }

  const parked: Array<{ parentId: string; signalName: string }> = [];
  for (let offset = 0; ; offset += ORPHAN_PAGE_SIZE) {
    if (params.keepGoing && !(await params.keepGoing())) return 0;
    const page = await storage.listWorkflows({
      status: "suspended",
      limit: ORPHAN_PAGE_SIZE,
      offset,
      orderBy: "createdAt",
      orderDir: "asc",
    });
    for (const wf of page) {
      for (const step of Object.values(wf.steps)) {
        const name = step.signalName;
        if (step.status !== "waiting_for_signal" || !name?.startsWith(CHILD_ENDED_SIGNAL_PREFIX)) {
          continue;
        }
        parked.push({ parentId: wf.workflowId, signalName: name });
      }
    }
    if (page.length < ORPHAN_PAGE_SIZE) break;
  }
  for (const [i, p] of parked.entries()) {
    if (params.stopped?.()) break;
    if (i % ORPHAN_PAGE_SIZE === 0 && params.keepGoing && !(await params.keepGoing())) break;
    await redeliver(p.parentId, p.signalName);
  }
  return delivered;
}

/**
 * A periodic background loop that runs only while this instance holds its
 * leadership. A failing iteration is reported and backed off; it never
 * ends the loop. `stop()` waits for the iteration in flight and releases
 * the leadership.
 */
export class LeaderLoop {
  private readonly loop: PollLoop;
  private readonly leaderElection: LeaderElection;
  private readonly onError: (error: unknown) => void;
  private running?: Promise<void>;
  private leader = false;

  constructor(params: {
    readonly name: string;
    readonly intervalMs: number;
    readonly clock: WallClock;
    readonly leaderElection: LeaderElection;
    readonly tick: () => Promise<void>;
    readonly onError: (error: unknown) => void;
  }) {
    this.leaderElection = params.leaderElection;
    this.onError = params.onError;
    this.loop = new PollLoop({
      name: params.name,
      intervalMs: params.intervalMs,
      clock: params.clock,
      tick: async () => {
        this.leader = await this.leaderElection.tryAcquire();
        if (!this.leader) return;
        await params.tick();
      },
      onError: (error) => this.onError(error),
    });
  }

  /** True while the loop is between iterations or in one and has not been asked to stop. */
  get stopping(): boolean {
    return this.loop.stopRequested;
  }

  start(): void {
    this.running ??= this.loop.start();
  }

  /** Run the next iteration now. */
  wake(): void {
    this.loop.wake();
  }

  /**
   * Take (or renew) the leadership for an iteration run outside the loop,
   * such as a direct operator call: false when another instance holds it
   * or the loop is stopping. A leadership taken here is released by
   * `stop()` or lapses with its lease.
   */
  async acquire(): Promise<boolean> {
    if (this.loop.stopRequested) return false;
    this.leader = await this.leaderElection.tryAcquire();
    return this.leader;
  }

  /**
   * Renew the leadership mid-iteration: false once it is lost (or the loop
   * is stopping), so a long iteration stops instead of overlapping the
   * next leader's.
   */
  async stillLeading(): Promise<boolean> {
    if (!this.leader || this.loop.stopRequested) return false;
    this.leader = await this.leaderElection.tryAcquire();
    return this.leader;
  }

  async stop(): Promise<void> {
    await this.loop.stop();
    await this.running;
    this.running = undefined;
    if (!this.leader) return;
    this.leader = false;
    try {
      await this.leaderElection.release();
    } catch (error) {
      this.onError(error);
    }
  }
}
