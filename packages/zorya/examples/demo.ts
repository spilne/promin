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
  computeNextRun,
  type Workflow,
  type DurableScheduleConfig,
} from "@promin/workflow";
import type { Clock } from "@promin/core";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { Database } from "bun:sqlite";
import { ZoryaServer } from "../src/index.ts";
import path from "node:path";
import { orderWorkflow } from "./workflows/order.ts";
import { paymentWorkflow } from "./workflows/payment.ts";

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
const workflowsByName: Record<string, Workflow<unknown, unknown>> = {
  order: orderWorkflow as unknown as Workflow<unknown, unknown>,
  payment: paymentWorkflow as unknown as Workflow<unknown, unknown>,
};

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
// Background: random triggers to keep the UI alive

async function startRandomTraffic() {
  const names = Object.keys(workflowsByName);
  const tick = async () => {
    const name = names[Math.floor(Math.random() * names.length)]!;
    const input =
      name === "order"
        ? {
            orderId: Math.floor(Math.random() * 10_000),
            customer: `cust-${Math.floor(Math.random() * 100)}`,
          }
        : { amount: Math.floor(Math.random() * 5_000) + 100, currency: "USD" };
    await triggerRun(name, input);
  };
  // Seed a handful immediately so the page has something on first load.
  for (let i = 0; i < 6; i++) void tick();
  setInterval(() => void tick(), 3_500);
}

// ---------------------------------------------------------------------------
// Schedules — seed + lightweight poll-based firing

async function seedSchedules() {
  await schedulerStorage.upsertSchedule({
    id: "orders-every-minute",
    name: "Orders every minute",
    cron: "* * * * *",
    timezone: "UTC",
    enabled: true,
    metadata: { workflowName: "order", input: { source: "scheduled-minute" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "orders-every-10s",
    name: "Orders every 10s",
    intervalMs: 10_000,
    enabled: true,
    metadata: { workflowName: "order", input: { source: "fast-schedule" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "payments-every-30s",
    name: "Payments every 30s",
    intervalMs: 30_000,
    enabled: true,
    jitterMs: 2_000,
    metadata: { workflowName: "payment", input: { mode: "scheduled" } },
  });
  await schedulerStorage.upsertSchedule({
    id: "daily-orders-batch",
    name: "Daily orders batch (9am local)",
    cron: "0 9 * * *",
    timezone: "America/Edmonton",
    enabled: true,
    metadata: { workflowName: "order", input: { source: "daily-batch" } },
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
        const input = (s.metadata?.["input"] as unknown) ?? {};
        if (wfName && workflowsByName[wfName]) {
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
void startRandomTraffic();

const uiDir = path.join(import.meta.dir, "..", "dist", "public");

const server = new ZoryaServer({
  storage,
  scheduler: schedulerStorage,
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
console.log(`  - Random traffic every ~3.5s`);
console.log(`  - Schedules fire on their cadence (1s poll)`);
