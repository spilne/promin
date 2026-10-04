// ---------------------------------------------------------------------------
// PostgresWorkflowStorage benchmarks — wall time and SQL round trips of the
// runner's hot paths against a real Postgres (testcontainers, Docker
// required). Every statement the driver sends (BEGIN / COMMIT included)
// counts as one round trip.
//
// Scenarios:
//   * a 100-step chain of inline steps, without and with attempt rows
//   * one journaled step with 50 activities
//   * loadWorkflow of a run with 1k step rows (and 1k task rows)
//   * a 10k-element mapOver
//   * listWorkflowSummaries / countWorkflows over 2k runs (the dashboard
//     metrics queries)
//
// Each scenario reports the median of a few runs after one warm-up run.
//
// Run: bun --conditions=@promin/source \
//        packages/postgres/src/lib/postgres-workflow-storage.bench.ts
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { GenericContainer, Wait } from "testcontainers";
import { createWorkflowRunner, workflow, type Workflow } from "@promin/workflow";
import { migrate } from "./migrate.ts";
import { PostgresWorkflowStorage } from "./postgres-workflow-storage.ts";

const container = await new GenericContainer("postgres:17-alpine")
  .withExposedPorts(5432)
  .withEnvironment({ POSTGRES_USER: "b", POSTGRES_PASSWORD: "b", POSTGRES_DB: "b" })
  .withCommand(["postgres", "-c", "fsync=off", "-c", "synchronous_commit=off"])
  .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
  .start();

let roundTrips = 0;
const sql = postgres(`postgres://b:b@${container.getHost()}:${container.getMappedPort(5432)}/b`, {
  onnotice: () => {},
  debug: () => void roundTrips++,
});
const db = drizzle(sql);
await migrate(db);

let counter = 0;
const nextId = (): string => `bench-${++counter}`;

async function storage(params?: { recordAttempts?: boolean }): Promise<PostgresWorkflowStorage> {
  return PostgresWorkflowStorage.create({
    db,
    autoSeedLookups: false,
    recordAttempts: params?.recordAttempts ?? false,
  });
}

/** Median wall time and round trips per run of `run`, after one warm-up run. */
async function measure(label: string, run: () => Promise<unknown>, reps = 5): Promise<void> {
  await run();
  const samples: Array<{ ms: number; trips: number }> = [];
  for (let i = 0; i < reps; i++) {
    const trips = roundTrips;
    const start = performance.now();
    await run();
    samples.push({ ms: performance.now() - start, trips: roundTrips - trips });
  }
  samples.sort((a, b) => a.ms - b.ms);
  const median = samples[Math.floor(samples.length / 2)]!;
  console.log(
    `${label.padEnd(44)} median ${median.ms.toFixed(1).padStart(8)} ms   ` +
      `round trips ${String(median.trips).padStart(6)}`,
  );
}

function chain(n: number): Workflow<number, number> {
  let b: any = workflow<number>({ name: `chain-${n}` });
  for (let i = 0; i < n; i++) {
    b = b.step(`s${i}`, ({ prev, input }: { prev?: number; input: number }) =>
      succeed((prev ?? input) + 1),
    );
  }
  return b.build();
}

for (const recordAttempts of [false, true]) {
  const s = await storage({ recordAttempts });
  const wf = chain(100);
  await measure(`chain 100${recordAttempts ? " + attempt rows" : ""}`, () =>
    createWorkflowRunner({ storage: s }).run({ workflow: wf, workflowId: nextId(), input: 0 }),
  );
}

{
  const s = await storage();
  const wf = workflow<number>({ name: "journaled-50" })
    .journaled("body", function* (ctx) {
      let total = 0;
      for (let i = 0; i < 50; i++) {
        total += yield* ctx.activity(`a${i}`, async () => i);
      }
      return total;
    })
    .build();
  await measure("journaled step, 50 activities", () =>
    createWorkflowRunner({ storage: s }).run({ workflow: wf, workflowId: nextId(), input: 0 }),
  );
}

{
  const s = await storage();
  const workflowId = nextId();
  await s.createWorkflow({ workflowId, workflowName: "wide", input: {} });
  const startedAt = new Date();
  await s.batchSaveStepResults({
    records: Array.from({ length: 1_000 }, (_, i) => ({
      workflowId,
      stepName: `s${i}`,
      result: { i, payload: "x".repeat(64) },
      durationMs: 1,
      startedAt,
    })),
  });
  for (let i = 0; i < 1_000; i++) {
    await s.saveTaskResult({ workflowId, stepName: "map", taskIndex: i, result: i });
  }
  await measure("loadWorkflow, 1k steps + 1k tasks", () => s.loadWorkflow(workflowId), 21);
}

{
  const s = await storage();
  const wf = workflow<number>({ name: "map-10k" })
    .step("source", ({ input }) => succeed(Array.from({ length: input }, (_, i) => i)))
    .mapOver("double", { array: "source" }, (n) => succeed(n * 2))
    .build();
  await measure(
    "mapOver 10k elements",
    () =>
      createWorkflowRunner({ storage: s }).run({
        workflow: wf,
        workflowId: nextId(),
        input: 10_000,
      }),
    3,
  );
}

{
  const s = await storage();
  for (let i = 0; i < 2_000; i++) {
    const workflowId = `dash-${i}`;
    await s.createWorkflow({
      workflowId,
      workflowName: "dash",
      input: { payload: "x".repeat(2_048) },
    });
    await s.completeWorkflow({ workflowId, result: { payload: "y".repeat(2_048) } });
  }
  const lister = (
    s as { listWorkflowSummaries?: PostgresWorkflowStorage["listWorkflows"] }
  ).listWorkflowSummaries?.bind(s);
  await measure("listWorkflows completed, limit 2000", () =>
    s.listWorkflows({ status: "completed", limit: 2_000 }),
  );
  if (lister) {
    await measure("listWorkflowSummaries completed, limit 2000", () =>
      lister({ status: "completed", limit: 2_000 }),
    );
  }
  await measure("countWorkflows x 7 statuses", () =>
    Promise.all(
      ["pending", "running", "suspended", "completed", "failed", "compensating", "tripwire"].map(
        (status) => s.countWorkflows({ status: status as "completed" }),
      ),
    ),
  );
}

await sql.end();
await container.stop();
