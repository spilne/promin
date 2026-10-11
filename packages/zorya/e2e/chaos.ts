// ---------------------------------------------------------------------------
// Chaos run for worker-driven runs: one Zorya host on Postgres and two worker
// processes; N runs mixing plain steps, sleeps, signals and child workflows;
// one worker is killed with SIGKILL half-way through the triggers and Zorya
// is killed and restarted once. Every run must complete with its expected
// result. Prints a JSON report (counts, re-executed steps, durations).
//
//   DATABASE_URL=postgres://… N=200 \
//     bun --conditions=@promin/source packages/zorya/e2e/chaos.ts
// ---------------------------------------------------------------------------

import path from "node:path";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";
import type { DrizzleDb } from "@spilne/perfect-postgres";

const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");
const N = Number(process.env.N ?? 200);
const PORT = Number(process.env.PORT ?? 4390);
const base = `http://127.0.0.1:${PORT}`;
const tag = `c${Date.now().toString(36)}`;

const HOST_ENV = {
  RECLAIM_MS: "10000",
  SCAN_MS: "500",
  MAINT_MS: "2000",
  GRACE_MS: "5000",
  ADV_TTL_MS: "30000",
  WORKER_TIMEOUT_MS: "15000",
};
const WORKER_ENV = {
  LOCK_MS: "10000",
  CLAIM_HEARTBEAT_MS: "2500",
  POLL_MS: "250",
  HEARTBEAT_MS: "3000",
  LOCKED_RETRY_MS: "2000",
  CONCURRENCY: "20",
  QUIET: "1",
};

const sql = postgres(url, { max: 4, onnotice: () => {} });
const db = drizzle(sql) as unknown as DrizzleDb;
await migrate(db);
const storage = await PostgresWorkflowStorage.create({ db });

const spawn = (script: string, env: Record<string, string>) => {
  const child = Bun.spawn(
    ["bun", "--conditions=@promin/source", path.join(import.meta.dir, script)],
    {
      env: { ...process.env, ...env },
      stdout: "pipe",
      stderr: "inherit",
    },
  );
  const ready = (async () => {
    const decoder = new TextDecoder();
    let out = "";
    for await (const chunk of child.stdout as ReadableStream<Uint8Array>) {
      out += decoder.decode(chunk);
      if (/READY/.test(out)) return;
    }
  })();
  return { child, ready };
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const startHost = async () => {
  const h = spawn("host.ts", {
    ...HOST_ENV,
    DATABASE_URL: url,
    PORT: String(PORT),
    INSTANCE_ID: "chaos-host",
  });
  await h.ready;
  return h;
};
const startWorker = async (id: string) => {
  const w = spawn("worker.ts", { ...WORKER_ENV, ZORYA_URL: base, WORKER_ID: id });
  await w.ready;
  return w;
};
const post = async (p: string, body: unknown) => {
  for (let i = 0; ; i++) {
    try {
      const res = await fetch(`${base}${p}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (res.status < 300) return;
      if (i > 50) throw new Error(`${p}: ${res.status} ${await res.text()}`);
    } catch (err) {
      if (i > 50) throw err;
    }
    await sleep(200); // the host is down (restart) or not up yet
  }
};

const started = Date.now();
let host = await startHost();
const w1 = await startWorker("chaos-w1");
const w2 = await startWorker("chaos-w2");

/** id → expected result check. */
const expected = new Map<string, (result: unknown) => boolean>();
const waiters: string[] = [];
for (let i = 0; i < N; i++) {
  const kind = i % 4;
  const id = `${tag}-${i}`;
  if (kind === 0) {
    await post("/api/workflows/e2e-slow/runs", {
      workflowId: id,
      input: { ms: 300 + (i % 7) * 200 },
    });
    expected.set(id, (r) => typeof (r as { pid?: number })?.pid === "number");
  } else if (kind === 1) {
    await post("/api/workflows/e2e-sleeper/runs", { workflowId: id, input: {} });
    expected.set(id, (r) => (r as { woke?: boolean })?.woke === true);
  } else if (kind === 2) {
    await post("/api/workflows/e2e-waiter/runs", { workflowId: id, input: { n: i } });
    expected.set(id, (r) => r === i + 1);
    waiters.push(id);
  } else {
    await post("/api/workflows/e2e-parent/runs", { workflowId: id, input: { n: i, id } });
    expected.set(id, (r) => r === i * 2 + 1);
  }
  if (i === Math.floor(N / 2)) {
    w1.child.kill(9);
    console.error(`[chaos] SIGKILL worker chaos-w1 after ${i + 1} triggers`);
  }
  if (i === Math.floor((3 * N) / 4)) {
    host.child.kill(9);
    await host.child.exited;
    console.error(`[chaos] SIGKILL zorya after ${i + 1} triggers; restarting in 2s`);
    await sleep(2_000);
    host = await startHost();
  }
}
// Deliver every waiter's signal once it has parked (or right away: a
// delivery before the wait is picked up when the wait starts).
for (const id of waiters) {
  await post(`/api/runs/${id}/signal`, { signalName: "go", payload: { add: 1 } });
}
const triggeredAt = Date.now();

const deadline = Date.now() + Number(process.env.TIMEOUT_MS ?? 300_000);
let pending = [...expected.keys()];
while (pending.length > 0 && Date.now() < deadline) {
  const next: string[] = [];
  for (const id of pending) {
    const s = await storage.loadWorkflowStatus(id);
    if (s?.status !== "completed" && s?.status !== "failed") next.push(id);
  }
  pending = next;
  if (pending.length > 0) await sleep(1_000);
}

const report = {
  runs: N,
  completed: 0,
  wrongResult: [] as string[],
  failed: [] as Array<{ id: string; error?: string }>,
  stuck: pending,
  stepsReExecuted: 0,
  runsWithReExecutedSteps: [] as string[],
  byWorker: {} as Record<string, number>,
  /** Runs started by the killed worker and finished by the other one. */
  runsTakenOver: [] as string[],
  triggerSeconds: Math.round((triggeredAt - started) / 100) / 10,
  totalSeconds: 0,
};
const pidToWorker = new Map<number, string>([
  [w1.child.pid, "chaos-w1"],
  [w2.child.pid, "chaos-w2"],
]);
for (const [id, ok] of expected) {
  const run = await storage.loadWorkflow(id);
  if (run?.status === "completed") {
    report.completed++;
    if (!ok(run.result)) report.wrongResult.push(id);
  } else if (run?.status === "failed") {
    report.failed.push({ id, ...(run.error !== undefined && { error: run.error }) });
  }
  const attempts = await storage.loadStepAttempts({ workflowId: id });
  const perStep = new Map<string, number>();
  for (const a of attempts) perStep.set(a.stepName, (perStep.get(a.stepName) ?? 0) + 1);
  const extra = [...perStep.values()].reduce((sum, c) => sum + Math.max(0, c - 1), 0);
  if (extra > 0) {
    report.stepsReExecuted += extra;
    report.runsWithReExecutedSteps.push(id);
  }
  const workers = new Set<string>();
  for (const step of Object.values(run?.steps ?? {})) {
    const pid = (step.result as { pid?: number } | undefined)?.pid;
    if (typeof pid === "number") {
      const w = pidToWorker.get(pid) ?? `pid-${pid}`;
      workers.add(w);
      report.byWorker[w] = (report.byWorker[w] ?? 0) + 1;
    }
  }
  if (workers.size > 1) report.runsTakenOver.push(id);
}
report.totalSeconds = Math.round((Date.now() - started) / 100) / 10;
console.log(JSON.stringify(report, null, 2));

w2.child.kill("SIGTERM");
host.child.kill("SIGTERM");
await Promise.all([w2.child.exited, host.child.exited]);
await sql.end({ timeout: 2 });
process.exit(report.completed === N && report.wrongResult.length === 0 ? 0 : 1);
