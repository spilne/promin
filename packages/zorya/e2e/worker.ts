// ---------------------------------------------------------------------------
// A worker process for the worker-driven end-to-end tests and chaos runs.
//
//   ZORYA_URL=http://127.0.0.1:4310 WORKER_ID=w1 \
//     bun --conditions=@promin/source packages/zorya/e2e/worker.ts
//
// VERSION=<v> hosts only `e2e-versioned@<v>` (rolling-deploy tests);
// otherwise it hosts the e2e workflows. Tuning (ms): LOCK_MS (run-lock
// lease), CLAIM_HEARTBEAT_MS, POLL_MS, HEARTBEAT_MS; CONCURRENCY.
// Prints `READY <workerId>` once registered and advertised.
// ---------------------------------------------------------------------------

import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ALL, versioned } from "./workflows.ts";

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const url = process.env.ZORYA_URL ?? "http://127.0.0.1:4310";
const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;
const lockMs = env("LOCK_MS", 120_000);

const worker = new ZoryaWorker({
  client: new ZoryaClient({ url, retry: { maxElapsedMs: env("RETRY_MS", 60_000) } }),
  workflows: process.env.VERSION ? [versioned(process.env.VERSION)] : ALL,
  workerId,
  concurrency: env("CONCURRENCY", 10),
  heartbeatIntervalMs: env("HEARTBEAT_MS", 10_000),
  pollWorkflowStarts: {
    // Unset: the worker's default (a slow fallback poll; wake-ups claim new work).
    ...(process.env.POLL_MS !== undefined && { intervalMs: env("POLL_MS", 0) }),
    claimHeartbeatMs: env("CLAIM_HEARTBEAT_MS", 30_000),
  },
  lockedRetryDelayMs: env("LOCKED_RETRY_MS", 5_000),
  lock: { durationMs: lockMs, heartbeatIntervalMs: Math.max(250, Math.floor(lockMs / 4)) },
  onError: (e) => {
    if (process.env.QUIET !== "1") {
      console.error(
        `[${workerId}] ${e.phase}${e.workflowId ? ` ${e.workflowId}` : ""}:`,
        String(e.error),
      );
    }
  },
});
worker.start();
await worker.ready();
console.log(`READY ${workerId}`);

const shutdown = async () => {
  await worker.stop({ timeoutMs: env("STOP_TIMEOUT_MS", 10_000) });
  process.exit(0);
};
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
