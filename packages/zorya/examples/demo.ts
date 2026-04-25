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
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
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
import { mkdirSync } from "node:fs";

// ---------------------------------------------------------------------------
// Storage + runner
//
// Defaults to a persistent file under ./target so runs, schedules, and
// advertised workflows survive server restarts (and the dev hot-reload
// loop, which restarts the subprocess on .ts changes). Override with:
//   ZORYA_DB=:memory:        bun run zorya     # fresh on every boot
//   ZORYA_DB=./somewhere.db  bun run zorya     # custom path

const dbPath = process.env.ZORYA_DB ?? "./target/zorya.db";
if (dbPath !== ":memory:") {
  // mkdir -p the parent so first-time runs don't crash on a missing dir.
  mkdirSync(path.dirname(dbPath), { recursive: true });
}
const db = new Database(dbPath);
// Turn on WAL + foreign keys for file-backed DBs. No-op for :memory:.
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });
const schedulerStorage = new InMemorySchedulerStorage();
const runner = createWorkflowRunner({ storage });

// ---------------------------------------------------------------------------
// Workers — register two mock workers so the dashboard's Workers page has
// something to display. The demo runs every workflow in-process via
// `runner.run` (no actual task dispatch over a queue), so these workers
// don't claim any work; they just heartbeat and show up in the registry.
// To see real worker behavior (capability claims, run distribution, dead
// detection on stop), run the split example in `examples/split/`.

const workerRegistry = new InMemoryWorkerRegistry();
const MOCK_WORKERS = [
  {
    workerId: "demo-worker-eu-1",
    capabilities: ["any"],
    concurrency: 4,
    metadata: {
      hostname: "eu-1.demo.local",
      runtime: "bun",
      version: "0.4.2",
      // Custom tags go under `labels` so the dashboard can render them
      // as a distinct category (versus capabilities / workflows / namespaces).
      labels: { region: "eu-west", env: "demo" },
    },
  },
  {
    workerId: "demo-worker-us-2",
    capabilities: ["video", "etl"],
    concurrency: 2,
    metadata: {
      hostname: "us-2.demo.local",
      runtime: "bun",
      version: "0.4.2",
      labels: { region: "us-east", env: "demo" },
    },
  },
] as const;

for (const w of MOCK_WORKERS) {
  await workerRegistry.register({
    workerId: w.workerId,
    capabilities: w.capabilities,
    concurrency: w.concurrency,
    metadata: w.metadata,
  });
}
// Heartbeat each mock worker so detectDead() doesn't tip them into the
// "dead" bucket. 5s cadence stays well below typical 30s timeouts and
// keeps the lastHeartbeat field visibly fresh in the dashboard.
const heartbeatHandle = setInterval(() => {
  for (const w of MOCK_WORKERS) {
    void workerRegistry.heartbeat(w.workerId);
  }
}, 5_000);
process.on("SIGINT", () => clearInterval(heartbeatHandle));
process.on("SIGTERM", () => clearInterval(heartbeatHandle));

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
    case "research":
      return {
        topic: ["durable-execution", "distributed-systems", "saga-patterns", "event-sourcing"][
          Math.floor(Math.random() * 4)
        ],
        sourceCount: 3 + Math.floor(Math.random() * 3),
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
  opts: { workflowId?: string; namespace?: string } = {},
): Promise<{ workflowId: string }> {
  const wf = workflowsByName[name];
  if (!wf) throw new Error(`Unknown workflow: ${name}`);
  const workflowId = opts.workflowId ?? nextId(name);
  // Pre-create the row with the requested namespace so it sticks. The
  // runner's own internal createWorkflow inside run() is idempotent — it
  // sees the existing row and resumes against it instead of overwriting.
  // Without this hop the namespace would always be null because runner.run
  // doesn't take a namespace param.
  if (opts.namespace) {
    await storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      namespace: opts.namespace,
      version: wf.version,
    });
  }
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
  // Skip when the persistent DB already has runs — keep accumulated
  // history intact across restarts. Schedules still fire on their own
  // cadence so the dashboard stays animated.
  const existing = await storage.listWorkflows({ limit: 1 });
  if (existing.length > 0) {
    console.log("[zorya] storage already has runs — skipping initial seed");
    return;
  }
  // Seed every workflow once so the dashboard has data on first load,
  // and rotate through a few namespaces so the sidebar's namespace
  // switcher actually has alternates (it would otherwise only see
  // `default`/null on every row).
  const namespaces = ["tenant-a", "tenant-b", undefined];
  let i = 0;
  for (const name of Object.keys(workflowsByName)) {
    const namespace = namespaces[i++ % namespaces.length];
    await triggerRun(name, inputFor(name), { namespace });
  }
}

// ---------------------------------------------------------------------------
// Schedules — seed + lightweight poll-based firing

async function seedSchedules() {
  // Pin some schedules to specific namespaces so the runs they produce
  // populate tenant-a / tenant-b consistently — gives the namespace
  // switcher in the sidebar live, scheduled traffic to filter on.
  // Schedules carry the namespace BOTH on the top-level field (so the
  // dashboard's tenant switcher filters them via storage.listSchedules)
  // AND in metadata.namespace (so the demo's firer hands it to
  // triggerRun, which pre-creates the resulting workflow row in that
  // namespace). They're conceptually the same axis — the demo just has
  // to write it twice because schedule.namespace is what scheduler
  // storage queries on, while metadata.namespace is the input the
  // demo's firer reads.
  await schedulerStorage.upsertSchedule({
    id: "orders-every-15s",
    name: "Orders every 15s",
    intervalMs: 15_000,
    enabled: true,
    namespace: "tenant-a",
    metadata: { workflowName: "order", namespace: "tenant-a" },
  });
  await schedulerStorage.upsertSchedule({
    id: "payments-every-30s",
    name: "Payments every 30s",
    intervalMs: 30_000,
    enabled: true,
    jitterMs: 2_000,
    namespace: "tenant-a",
    metadata: { workflowName: "payment", namespace: "tenant-a" },
  });
  await schedulerStorage.upsertSchedule({
    id: "video-transcodes-every-45s",
    name: "Video transcodes every 45s",
    intervalMs: 45_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "video-transcode", namespace: "tenant-b" },
  });
  await schedulerStorage.upsertSchedule({
    id: "onboarding-every-60s",
    name: "Onboarding every 60s",
    intervalMs: 60_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "onboarding", namespace: "tenant-b" },
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
    namespace: "tenant-a",
    metadata: { workflowName: "order-fulfillment", namespace: "tenant-a" },
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
    id: "research-every-90s",
    name: "Research (journaled multi-activity) every 90s",
    intervalMs: 90_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "research", namespace: "tenant-b" },
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
      const waiting = Object.values(r.steps).some(
        (s) => s.stepName === "review" && s.status === "waiting_for_signal",
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
 * Resume runs that were left in `pending` or `running` from a previous
 * server session. The demo drives execution in-process (no coordinator),
 * so when the bun --hot subprocess hot-replaces or the user kills the
 * server, every in-flight `runner.run()` promise dies with it. The rows
 * stay in storage with their last-observed status; without this sweep
 * they sit there forever.
 *
 * Recovery: re-call `runner.run` for each. The runner is idempotent on
 * (workflowId, input) — it loads the existing row, replays journal
 * entries for any journaled steps, and continues from the next pending
 * step. Stale runs from yesterday will resume now and complete with
 * a fresh end-time, which is fine for a demo but obviously the wrong
 * policy for production (you'd want a stale-cutoff + auto-fail).
 */
async function resumeOrphanedRuns() {
  const candidates = await storage.listWorkflows({ status: "pending", limit: 500 });
  const running = await storage.listWorkflows({ status: "running", limit: 500 });
  const all = [...candidates, ...running];
  if (all.length === 0) return;
  console.log(`[zorya] resuming ${all.length} orphaned run(s) from prior session`);
  for (const state of all) {
    const def = workflowsByName[state.workflowName];
    if (!def) continue; // workflow registry may have changed since the row was created
    runner.run({ workflow: def, workflowId: state.workflowId, input: state.input }).catch(() => {
      // Recovery best-effort; failures land in storage as workflow.failed.
    });
  }
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
          // Schedules can pin a namespace via metadata so all their fires
          // land in the same tenant. Falls through to undefined when the
          // schedule didn't set one — same as a global / cross-tenant
          // schedule.
          const namespace = s.metadata?.["namespace"] as string | undefined;
          await triggerRun(wfName, input, { namespace });
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
void resumeOrphanedRuns();

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
  // Feeds the "Trigger workflow" form on the Workflows page with plausible
  // defaults so users can tweak fields instead of writing raw JSON.
  sampleInput: (name) => inputFor(name),
  uiDir,
  // Workers page reads from the registry. The demo mocks two entries
  // above; `stepQueue` is structurally required by the workerProtocol
  // type but unused at runtime here because we keep the explicit
  // `trigger` callback below — that path runs workflows in-process via
  // the runner, never actually enqueues to the step queue.
  workerProtocol: { stepQueue: new InMemoryStepQueue(), workerRegistry },
  trigger: (name, input, opts) => triggerRun(name, input, { namespace: opts?.namespace }),
  rerun: async (workflowId) => {
    // After startFreshRun the row is reset; we still need to drive the
    // workflow again. Look up the name from storage, find its definition,
    // and call runner.run with the same workflow id.
    const state = await storage.loadWorkflow(workflowId);
    if (!state) return;
    const def = workflowsByName[state.workflowName];
    if (!def) return;
    runner.run({ workflow: def, workflowId, input: state.input }).catch(() => {});
  },
  // No explicit `workers` provider — the server falls through to
  // RegistryBackedWorkersProvider over `workerProtocol.workerRegistry`,
  // which surfaces the two mock workers we registered above.
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya demo server on http://${host}:${actualPort}`);
console.log(`  - Storage: sqlite (${dbPath})`);
console.log(`  - Traffic comes only from schedules — pause one to stop its runs`);
