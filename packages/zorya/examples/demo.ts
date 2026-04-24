// ---------------------------------------------------------------------------
// Demo server — runs real workflows on an in-memory engine and serves the
// dashboard. Run with:
//   bun run packages/zorya/examples/demo.ts
//
// What's happening:
// - DefaultWorkflowRunner drives two real workflow definitions
//   (`order`, `payment`) with random-delay async steps and occasional
//   failures.
// - POST /api/runs/trigger/:name via ZoryaServer spawns a run.
// - A background loop triggers random runs every few seconds so the Runs
//   list and stats bar animate on their own.
// - A lightweight poll loop fires due schedules (every second) and calls
//   the same runner — so the Schedules page ticks increment live.
// ---------------------------------------------------------------------------

import {
  InMemorySchedulerStorage,
  createWorkflowRunner,
  createSleepScanner,
  completeSignal,
  computeNextRun,
  isJournaledSuspendStorage,
  type Workflow,
  type DurableScheduleConfig,
} from "@promin/workflow";
import type { Clock } from "@promin/core";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { Database } from "bun:sqlite";
import { ZoryaServer, scanWorkflowsFolder } from "../src/index.ts";
import path from "node:path";

// ---------------------------------------------------------------------------
// Storage + runner
//
// `ZORYA_DB` overrides the sqlite path. Default: `:memory:` so repeated runs
// start clean. Set to a file path to persist across restarts:
//   ZORYA_DB=./zorya.db bun run zorya

const dbPath = process.env.ZORYA_DB ?? ":memory:";
const db = new Database(dbPath);
// Turn on WAL + foreign keys for file-backed DBs. No-op for :memory:.
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });
const schedulerStorage = new InMemorySchedulerStorage();
const runner = createWorkflowRunner({ storage });

// Registry so trigger-by-name works.
// Auto-discover workflows by scanning ./workflows. Every .ts module under
// that directory whose exports include a Workflow is registered under its
// `workflow.name`. Add a new file and restart — no edits here needed.
const scanRoot = path.join(import.meta.dir, "workflows");
const scanResult = await scanWorkflowsFolder(scanRoot, {
  onWorkflow: (name, _wf, src) =>
    console.log(`[zorya] discovered workflow ${name} (${path.relative(scanRoot, src)})`),
});
for (const w of scanResult.warnings) console.warn(`[zorya] ${w}`);
const workflowsByName: Record<string, Workflow<unknown, unknown>> = scanResult.workflows;

function inputFor(name: string): unknown {
  switch (name) {
    case "order":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        customer: `cust-${Math.floor(Math.random() * 100)}`,
      };
    case "payment":
      return { amount: Math.floor(Math.random() * 5_000) + 100, currency: "USD" };
    case "video-transcode":
      return {
        videoId: `vid-${Math.floor(Math.random() * 1_000_000)}`,
        url: "https://example.com/video.mp4",
      };
    case "onboarding":
      return { email: `user-${Math.floor(Math.random() * 10_000)}@example.com` };
    case "etl":
      return { source: "events-prod", batch: Math.floor(Math.random() * 100) };
    case "order-fulfillment":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        items: ["sku-a", "sku-b"],
      };
    case "batch-process":
      return {
        batchId: `batch-${Math.floor(Math.random() * 10_000)}`,
        itemCount: 6 + Math.floor(Math.random() * 8),
      };
    case "approval-flow":
      return {
        requestId: Math.floor(Math.random() * 10_000),
        requester: `user-${Math.floor(Math.random() * 100)}`,
      };
    default:
      return {};
  }
}

let idCounter = 0;
function nextId(name: string): string {
  idCounter += 1;
  return `${name}-${Date.now().toString(36)}-${idCounter}`;
}

async function triggerRun(
  name: string,
  input: unknown,
  opts: { workflowId?: string } = {},
): Promise<{ workflowId: string }> {
  const wf = workflowsByName[name];
  if (!wf) throw new Error(`Unknown workflow: ${name}`);
  const workflowId = opts.workflowId ?? nextId(name);
  // Fire-and-forget: we don't await run() so the server responds immediately.
  runner.run({ workflow: wf, workflowId, input }).catch(() => {
    // Failures are stored as workflow.failed state; swallow here so the
    // background loop keeps running.
  });
  return { workflowId };
}

// ---------------------------------------------------------------------------
// Background: seed a handful of runs on startup so the first page has data.
// Ongoing traffic comes from schedules — pausing a schedule actually stops
// its runs (no hidden random loop).

async function seedInitialRuns() {
  for (const name of Object.keys(workflowsByName)) {
    await triggerRun(name, inputFor(name));
  }
}

// ---------------------------------------------------------------------------
// Schedules — seed + lightweight poll-based firing

async function seedSchedules() {
  await schedulerStorage.upsertSchedule({
    id: "orders-every-15s",
    name: "Orders every 15s",
    intervalMs: 15_000,
    enabled: true,
    metadata: { workflowName: "order" },
  });
  await schedulerStorage.upsertSchedule({
    id: "payments-every-30s",
    name: "Payments every 30s",
    intervalMs: 30_000,
    enabled: true,
    jitterMs: 2_000,
    metadata: { workflowName: "payment" },
  });
  await schedulerStorage.upsertSchedule({
    id: "video-transcodes-every-45s",
    name: "Video transcodes every 45s",
    intervalMs: 45_000,
    enabled: true,
    metadata: { workflowName: "video-transcode" },
  });
  await schedulerStorage.upsertSchedule({
    id: "onboarding-every-60s",
    name: "Onboarding every 60s",
    intervalMs: 60_000,
    enabled: true,
    metadata: { workflowName: "onboarding" },
  });
  await schedulerStorage.upsertSchedule({
    id: "etl-hourly",
    name: "ETL hourly",
    cron: "0 * * * *",
    timezone: "UTC",
    enabled: true,
    metadata: { workflowName: "etl" },
  });
  await schedulerStorage.upsertSchedule({
    id: "fulfillment-every-40s",
    name: "Order fulfillment saga every 40s",
    intervalMs: 40_000,
    enabled: true,
    metadata: { workflowName: "order-fulfillment" },
  });
  await schedulerStorage.upsertSchedule({
    id: "batch-every-50s",
    name: "Batch process every 50s",
    intervalMs: 50_000,
    enabled: true,
    metadata: { workflowName: "batch-process" },
  });
  await schedulerStorage.upsertSchedule({
    id: "approvals-every-75s",
    name: "Approval flow every 75s",
    intervalMs: 75_000,
    enabled: true,
    metadata: { workflowName: "approval-flow" },
  });
  await schedulerStorage.upsertSchedule({
    id: "weekly-payment-audit",
    name: "Weekly payment audit (paused)",
    cron: "0 2 * * 1",
    timezone: "UTC",
    enabled: false,
    metadata: { workflowName: "payment", input: { mode: "audit" } },
  });
}

/**
 * Auto-delivers the "approval" signal to any approval-flow run that's been
 * waiting for one. Runs every 5s. Half the approvals are approved, half
 * rejected — so the dashboard shows both terminal outcomes.
 */
async function startApprovalAutoSignaler(): Promise<void> {
  const delivered = new Set<string>();
  setInterval(async () => {
    const runs = await storage.listWorkflows({
      name: "approval-flow",
      status: "suspended",
      limit: 50,
    });
    for (const r of runs) {
      if (delivered.has(r.workflowId)) continue;
      // Only deliver to runs whose review step is waiting for a signal.
      // Engine currently stores "waiting_signal" (bug: should be
      // "waiting_for_signal" per the type declaration) — check both so
      // this demo keeps working after the upstream fix.
      const waiting = Object.values(r.steps).some(
        (s) =>
          s.stepName === "review" &&
          (s.status === "waiting_for_signal" || (s.status as string) === "waiting_signal"),
      );
      if (!waiting) continue;
      const approved = Math.random() < 0.5;
      const payload = { approved, by: `auto-signaler` };
      // Three steps: store for the Signals tab, complete the pending journal
      // entry, and re-run the workflow so the journaled step picks up the
      // completed entry and continues. The sleep scanner only handles
      // sleep resumption — signal completion needs its own nudge.
      await storage.deliverSignal(r.workflowId, "approval", payload);
      if (isJournaledSuspendStorage(storage)) {
        await completeSignal({
          storage,
          workflowId: r.workflowId,
          stepName: "review",
          signalName: "approval",
          value: payload,
        });
      }
      delivered.add(r.workflowId);
      runner
        .run({
          workflow: workflowsByName["approval-flow"]!,
          workflowId: r.workflowId,
          input: r.input,
        })
        .catch(() => {
          // Suspended errors are expected; failures are recorded in storage.
        });
    }
  }, 5_000);
}

/**
 * Minimal schedule firing loop. Every second, scans for schedules whose
 * next run is <= now and fires them. Not as sophisticated as the real
 * DurableScheduler (no jitter handling, no catch-up, no overlap policy),
 * but enough to show live activity on the Schedules page.
 */
async function startScheduleFirer() {
  const lastNextRunById = new Map<string, Date | null>();

  const tick = async () => {
    const all = await schedulerStorage.listSchedules({ limit: 500 });
    const now = new Date();
    for (const s of all) {
      if (!s.enabled) continue;
      const state = await schedulerStorage.loadScheduleState(s.id);
      const lastFired = state?.lastFired ?? null;
      let next = lastNextRunById.get(s.id) ?? null;
      if (next === null) {
        // First tick for this schedule in this process — fire immediately so
        // the UI shows activity, then schedule the next run from now.
        if (!lastFired) {
          next = now;
        } else {
          next = computeNextScheduleRun(s, lastFired);
        }
        lastNextRunById.set(s.id, next);
      }
      if (next && next.getTime() <= now.getTime()) {
        const wfName = (s.metadata?.["workflowName"] as string | undefined) ?? undefined;
        if (wfName && workflowsByName[wfName]) {
          // Prefer explicit metadata.input, otherwise synthesise one using the
          // same generator as ad-hoc runs.
          const explicit = s.metadata?.["input"];
          const input = explicit === undefined ? inputFor(wfName) : explicit;
          await triggerRun(wfName, input);
        }
        await schedulerStorage.recordFire(s.id, now);
        const after = computeNextScheduleRun(s, now);
        lastNextRunById.set(s.id, after);
      }
    }
  };

  setInterval(() => void tick(), 1_000);
  void tick();
}

function computeNextScheduleRun(s: DurableScheduleConfig, from: Date): Date | null {
  // computeNextRun takes a Clock (not a Date); fake one anchored at `from`.
  const fakeClock: Clock = {
    currentTimeMs: () => from.getTime(),
    now: () => new Date(from.getTime()),
    setTimeout: (fn, ms) => {
      const h = setTimeout(fn, ms);
      return { clear: () => clearTimeout(h) };
    },
    setInterval: (fn, ms) => {
      const h = setInterval(fn, ms);
      return { clear: () => clearInterval(h) };
    },
  };
  try {
    return computeNextRun(s, fakeClock);
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Boot

await seedSchedules();
void startScheduleFirer();
void startApprovalAutoSignaler();

// Wake suspended workflows whose sleep has expired or whose signal was
// delivered. Without this, runs that entered ctx.sleep / ctx.signal never
// resume after their wake condition — they just sit in "suspended" forever.
const sleepScanner = createSleepScanner({
  storage,
  runner,
  scanIntervalMs: 2_000,
  resolveWorkflow: (name) => workflowsByName[name],
});
void sleepScanner.start();

await seedInitialRuns();

const uiDir = process.env.ZORYA_UI_DIR ?? path.join(import.meta.dir, "..", "dist", "public");

const server = new ZoryaServer({
  storage,
  scheduler: schedulerStorage,
  workflows: workflowsByName,
  uiDir,
  trigger: (name, input) => triggerRun(name, input),
  workers: {
    listWorkers: async () => [
      {
        workerId: "worker-demo",
        status: "online",
        queue: "default",
        activeTasks: 0,
        completedToday: 0,
        lastHeartbeatAt: new Date().toISOString(),
      },
    ],
  },
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya demo server on http://${host}:${actualPort}`);
console.log(`  - Storage: sqlite (${dbPath})`);
console.log(`  - Traffic comes only from schedules — pause one to stop its runs`);
