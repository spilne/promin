import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { distributedJob } from "./workflows.ts";

const url = process.env.ZORYA_URL ?? "http://central:4100";
const workerId = required("WORKER_ID");
const concurrency = Number(process.env.WORKER_CONCURRENCY ?? 4);

const worker = ZoryaWorker.builder()
  .client(new ZoryaClient({ url, apiKey: required("ZORYA_WORKER_API_KEY") }))
  .workflow(distributedJob)
  .workerId(workerId)
  .concurrency(concurrency)
  .heartbeatIntervalMs(1_000)
  .sampleInput(() => ({ id: 1, delayMs: 50 }))
  .build();

worker.start();
await worker.ready();
console.log(`[${workerId}] connected to ${url} with concurrency=${concurrency}`);

let stopping = false;
async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log(`[${workerId}] draining`);
  await worker.stop({ timeoutMs: 10_000 });
}

process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
