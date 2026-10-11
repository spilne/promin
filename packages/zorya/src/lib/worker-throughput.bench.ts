// ---------------------------------------------------------------------------
// Worker throughput, start latency and claim cost: an in-process Zorya
// (in-memory stores, real port, real `/ws/worker` sockets) and ZoryaWorkers
// driving trivial workflows, each scenario run twice — polling only (the
// old default: no control socket, 1 s poll) and with wake-ups (the
// defaults: control socket, 5 s jittered fallback poll).
//
//   bun run bench:zorya
//
// Scenarios:
//   - small fleet (WORKERS=4 × CONCURRENCY=10): start latency p50 / p99
//     (trigger request sent → first step running) for LATENCY_RUNS runs
//     triggered one every LATENCY_GAP_MS, then runs/s for a burst of RUNS
//   - large fleet (FLEET_WORKERS=50 × FLEET_CONCURRENCY=4) at a low rate
//     (FLEET_RUNS runs, one every FLEET_GAP_MS): start latency and claim
//     requests per trigger (idle polls included) over the same window
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { InMemoryWorkflowStorage, workflow, type Workflow } from "@promin/workflow";
import {
  InMemoryWorkerRegistry,
  InMemoryWorkflowAdvertisementRegistry,
  InMemoryWorkflowStartQueue,
} from "@promin/workflow/distributed";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../server/server.ts";
import { QueuedWorkflows } from "../server/services/workflows/index.ts";

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);

type Mode = "polling" | "wakeups";

interface Scenario {
  readonly name: string;
  readonly workers: number;
  readonly concurrency: number;
  readonly latencyRuns: number;
  readonly gapMs: number;
  /** Burst size for runs/s; 0 skips the burst. */
  readonly burstRuns: number;
}

interface Result {
  readonly mode: Mode;
  readonly p50: number;
  readonly p99: number;
  readonly runsPerSec?: number;
  /** Claim requests per trigger during the latency phase. */
  readonly claimsPerTrigger: number;
  readonly wakeupsPerTrigger: number;
}

const SCENARIOS: Scenario[] = [
  {
    name: "small fleet",
    workers: env("WORKERS", 4),
    concurrency: env("CONCURRENCY", 10),
    latencyRuns: env("LATENCY_RUNS", 200),
    gapMs: env("LATENCY_GAP_MS", 20),
    burstRuns: env("RUNS", 1_000),
  },
  {
    name: "large fleet, low rate",
    workers: env("FLEET_WORKERS", 50),
    concurrency: env("FLEET_CONCURRENCY", 4),
    latencyRuns: env("FLEET_RUNS", 100),
    gapMs: env("FLEET_GAP_MS", 50),
    burstRuns: 0,
  },
];

const quiet = { log() {}, warn() {}, error() {} };

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return Number.NaN;
  const i = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, i)]!;
}

async function waitFor(predicate: () => boolean, maxMs = 120_000): Promise<void> {
  const deadline = performance.now() + maxMs;
  while (!predicate()) {
    if (performance.now() > deadline) throw new Error("bench: condition not reached");
    await new Promise((r) => setTimeout(r, 1));
  }
}

async function run(params: { scenario: Scenario; mode: Mode }): Promise<Result> {
  const { scenario, mode } = params;
  const firstStepAt = new Map<string, number>();
  let completed = 0;
  const trivial = workflow<number>({ name: "trivial" })
    .step("first", ({ workflowId }) => {
      firstStepAt.set(workflowId, performance.now());
      return succeed(1);
    })
    .step("second", () => {
      completed++;
      return succeed(2);
    })
    .build() as Workflow<unknown, unknown>;

  const queued = new QueuedWorkflows({
    storage: new InMemoryWorkflowStorage(),
    workflowStarts: new InMemoryWorkflowStartQueue(),
    advertisements: new InMemoryWorkflowAdvertisementRegistry(),
    workerRegistry: new InMemoryWorkerRegistry(),
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows: queued,
    remoteWorkers: {},
    logger: quiet,
  });
  const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
  const url = `http://127.0.0.1:${handle.port}`;

  const workers = Array.from(
    { length: scenario.workers },
    (_, i) =>
      new ZoryaWorker({
        client: new ZoryaClient({ url }),
        workflows: [trivial],
        workerId: `bench-${mode}-${i}`,
        concurrency: scenario.concurrency,
        ...(mode === "polling"
          ? { controlSocket: false, pollWorkflowStarts: { intervalMs: 1_000, jitter: 0 } }
          : {}),
        onError: () => {},
      }),
  );
  for (const w of workers) w.start();
  await Promise.all(workers.map((w) => w.ready()));
  if (mode === "wakeups") {
    await waitFor(() => server.workerWs.connectedCount() === scenario.workers);
  }
  await new Promise((r) => setTimeout(r, 200));

  const trigger = async (workflowId: string): Promise<number> => {
    const sentAt = performance.now();
    const res = await fetch(`${url}/api/workflows/trivial/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ workflowId, input: 1 }),
    });
    if (!res.ok) throw new Error(`trigger ${workflowId}: ${res.status} ${await res.text()}`);
    return sentAt;
  };
  const totals = () =>
    workers.reduce(
      (acc, w) => ({
        claims: acc.claims + w.stats.claimCalls,
        wakeups: acc.wakeups + w.stats.wakeups,
      }),
      { claims: 0, wakeups: 0 },
    );

  // Latency (and claim cost): one trigger every gapMs.
  const startTotals = totals();
  const sentAt = new Map<string, number>();
  for (let i = 0; i < scenario.latencyRuns; i++) {
    const id = `lat-${i}`;
    sentAt.set(id, await trigger(id));
    await new Promise((r) => setTimeout(r, scenario.gapMs));
  }
  await waitFor(() => completed >= scenario.latencyRuns);
  const endTotals = totals();
  const latencies = [...sentAt].map(([id, at]) => firstStepAt.get(id)! - at).sort((a, b) => a - b);

  // Throughput: a burst, 50 triggers in flight at a time.
  let runsPerSec: number | undefined;
  if (scenario.burstRuns > 0) {
    const before = completed;
    const burstStart = performance.now();
    for (let i = 0; i < scenario.burstRuns; i += 50) {
      await Promise.all(
        Array.from({ length: Math.min(50, scenario.burstRuns - i) }, (_, j) =>
          trigger(`burst-${i + j}`),
        ),
      );
    }
    await waitFor(() => completed - before >= scenario.burstRuns);
    runsPerSec = (scenario.burstRuns / (performance.now() - burstStart)) * 1_000;
  }

  await Promise.all(workers.map((w) => w.stop({ timeoutMs: 1_000 })));
  await handle.stop();

  return {
    mode,
    p50: percentile(latencies, 50),
    p99: percentile(latencies, 99),
    ...(runsPerSec !== undefined && { runsPerSec }),
    claimsPerTrigger: (endTotals.claims - startTotals.claims) / scenario.latencyRuns,
    wakeupsPerTrigger: (endTotals.wakeups - startTotals.wakeups) / scenario.latencyRuns,
  };
}

const fmt = (n: number | undefined) => (n === undefined ? "-" : n.toFixed(1));
for (const scenario of SCENARIOS) {
  const results: Result[] = [];
  for (const mode of ["polling", "wakeups"] as const) results.push(await run({ scenario, mode }));
  console.log(
    `\n${scenario.name}: workers=${scenario.workers} concurrency=${scenario.concurrency} ` +
      `latency runs=${scenario.latencyRuns} (every ${scenario.gapMs} ms)` +
      (scenario.burstRuns > 0 ? ` burst=${scenario.burstRuns}` : "") +
      `; trivial 2-step runs, in-memory stores`,
  );
  console.log("mode      start p50 ms  start p99 ms   runs/s  claims/trigger  wake-ups/trigger");
  for (const r of results) {
    console.log(
      `${r.mode.padEnd(9)} ${fmt(r.p50).padStart(12)}  ${fmt(r.p99).padStart(12)}  ` +
        `${fmt(r.runsPerSec).padStart(7)}  ${fmt(r.claimsPerTrigger).padStart(14)}  ` +
        `${fmt(r.wakeupsPerTrigger).padStart(16)}`,
    );
  }
}
process.exit(0);
