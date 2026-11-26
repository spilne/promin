interface WorkerDto {
  readonly workerId: string;
  readonly status: string;
  readonly completedCount?: number;
}

export {};

interface RunDto {
  readonly status: string;
  readonly result?: unknown;
}

const baseUrl = process.env.ZORYA_URL ?? "http://central:4100";
const apiKey = required("ZORYA_API_KEY");
const workflowRuns = Number(process.env.SMOKE_WORKFLOW_RUNS ?? 64);
const agentInvocations = Number(process.env.SMOKE_AGENT_INVOCATIONS ?? 40);
const agents = ["research-agent", "support-agent"] as const;
let workerBaseline = new Map<string, number>();

await waitFor("coordinator readiness", async () => (await fetch(`${baseUrl}/api/ready`)).ok);
await waitFor("four online workers", async () => {
  const body = await api<{ workers: WorkerDto[] }>("/api/workers");
  const online = body.workers.filter((worker) => worker.status === "online");
  if (online.length !== 4) return false;
  workerBaseline = new Map(
    online.map((worker) => [worker.workerId, worker.completedCount ?? 0] as const),
  );
  return true;
});
await waitFor("two remote agents", async () => {
  const body = await api<{ agents: Array<{ id: string }> }>("/api/agents");
  return agents.every((id) => body.agents.some((agent) => agent.id === id));
});

const agentLatencies: number[] = [];
await Promise.all(
  Array.from({ length: agentInvocations }, async (_, index) => {
    const agentId = agents[index % agents.length]!;
    const started = performance.now();
    const result = await api<{ text: string }>(`/api/agents/${agentId}/invoke`, {
      method: "POST",
      body: JSON.stringify({
        task: `request-${index}`,
        namespaceId: "docker-smoke",
        resourceId: "load-client",
      }),
    });
    agentLatencies.push(performance.now() - started);
    assert(result.text.includes(agentId), `${agentId} returned an unexpected response`);
  }),
);

const workflowStarted = performance.now();
const workflowIds = await Promise.all(
  Array.from({ length: workflowRuns }, async (_, index) => {
    const response = await api<{ workflowId: string }>("/api/workflows/distributed-job/runs", {
      method: "POST",
      body: JSON.stringify({ input: { id: index, delayMs: 50 } }),
    });
    return response.workflowId;
  }),
);
await Promise.all(
  workflowIds.map((workflowId) =>
    waitFor(`run ${workflowId}`, async () => {
      const run = await api<RunDto>(`/api/runs/${encodeURIComponent(workflowId)}`);
      if (["failed", "cancelled", "tripwire"].includes(run.status)) {
        throw new Error(`run ${workflowId} ended as ${run.status}`);
      }
      return run.status === "completed";
    }),
  ),
);
const workflowElapsedMs = performance.now() - workflowStarted;

await waitFor("work distributed to all workers", async () => {
  const body = await api<{ workers: WorkerDto[] }>("/api/workers");
  const online = body.workers.filter((worker) => worker.status === "online");
  return (
    online.length === 4 &&
    online.every(
      (worker) =>
        (worker.completedCount ?? 0) > (workerBaseline.get(worker.workerId) ?? Number.MAX_VALUE),
    )
  );
});

agentLatencies.sort((a, b) => a - b);
const p50 = percentile(agentLatencies, 0.5);
const p95 = percentile(agentLatencies, 0.95);
const throughput = workflowRuns / (workflowElapsedMs / 1_000);

console.log("\nDistributed Zorya smoke test passed");
console.log(`  topology:         1 coordinator, 4 workers, 2 remote agents`);
console.log(`  agent requests:   ${agentInvocations}`);
console.log(`  agent latency:    p50=${p50.toFixed(1)}ms p95=${p95.toFixed(1)}ms`);
console.log(`  workflow runs:    ${workflowRuns} in ${workflowElapsedMs.toFixed(1)}ms`);
console.log(`  run throughput:   ${throughput.toFixed(1)} completed runs/s`);

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
      ...init.headers,
    },
  });
  if (!response.ok)
    throw new Error(`${init.method ?? "GET"} ${path}: ${response.status} ${await response.text()}`);
  return (await response.json()) as T;
}

async function waitFor(label: string, check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(200);
  }
  throw new Error(`Timed out waiting for ${label}${lastError ? `: ${String(lastError)}` : ""}`);
}

function percentile(values: readonly number[], quantile: number): number {
  if (values.length === 0) return 0;
  return values[Math.min(values.length - 1, Math.floor(values.length * quantile))]!;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
