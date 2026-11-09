// ---------------------------------------------------------------------------
// PgStepQueue.claim scaling benchmark against a real Postgres (Docker).
//
//   cd packages/postgres
//   bun --conditions=@promin/source run scripts/pg-claim-bench.ts [scan|lanes] [scenario]
//
// Each scenario seeds ~50k pending tasks, then times `claim({ limit: 10 })`
// for a worker the backlog does not concern. The claim must cost the same
// whether the throttled backlog holds 0 or 49k rows.
// ---------------------------------------------------------------------------

import { PgStepQueue } from "../src/index.ts";
import { migrate } from "../src/lib/migrate.ts";
import { PostgresTestContainer } from "../src/lib/test-utils.ts";

const strategy = (process.argv[2] ?? "lanes") as "scan" | "lanes";
const pg = new PostgresTestContainer();
await pg.start();
await migrate(pg.db);
const q = new PgStepQueue({ db: pg.db, claimStrategy: strategy });

async function seedScenario(
  sqlc: typeof pg.sql,
  name: "hot-key" | "hot-group" | "many-keys" | "plain",
  backlog: number,
): Promise<void> {
  await sqlc`TRUNCATE wf_step_queue RESTART IDENTITY`;
  if (name === "hot-key") {
    // One running task fills the hot key; its backlog is older than everything else.
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, status, claimed_at, created_at)
      VALUES ('run-hot', 's', 'hot', 'sc', 1, 'running', now(), now() - interval '2 days')`;
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, created_at)
      SELECT 'hot-' || g, 's', 'hot', 'sc', 1, now() - interval '1 day' + g * interval '1 millisecond' FROM generate_series(1, ${backlog}::int) g`;
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit)
      SELECT 'o-' || g, 's', 'k' || (g % 100), 'sc', 5 FROM generate_series(1, 1000) g`;
  } else if (name === "hot-group") {
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, group_name, group_max_concurrent, fairness_key, status, claimed_at, created_at)
      VALUES ('run-g', 's', 'g', 1, 'f0', 'running', now(), now() - interval '2 days')`;
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, group_name, group_max_concurrent, fairness_key, created_at)
      SELECT 'g-' || g, 's', 'g', 1, 'f' || (g % 4), now() - interval '1 day' + g * interval '1 millisecond' FROM generate_series(1, ${backlog}::int) g`;
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name)
      SELECT 'o-' || g, 's' FROM generate_series(1, 1000) g`;
  } else if (name === "many-keys") {
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit)
      SELECT 'm-' || g, 's', 'k' || g, 'sc', 1 FROM generate_series(1, ${backlog}::int) g`;
  } else {
    await sqlc`INSERT INTO wf_step_queue (workflow_id, step_name)
      SELECT 'p-' || g, 's' FROM generate_series(1, ${backlog}::int) g`;
  }
  await sqlc`ANALYZE wf_step_queue`;
}

async function time(name: Parameters<typeof seedScenario>[1], backlog: number): Promise<number> {
  await seedScenario(pg.sql, name, backlog);
  const samples: number[] = [];
  for (let i = 0; i < 7; i++) {
    const t = performance.now();
    const got = await q.claim({ workerId: "w", limit: 10 });
    samples.push(performance.now() - t);
    if (got.length === 0) throw new Error(`${name}: nothing claimed`);
  }
  samples.sort((a, b) => a - b);
  return samples[3]!;
}

console.log(`strategy=${strategy}`);
for (const name of (process.argv[3]
  ? [process.argv[3]]
  : ["hot-key", "hot-group", "many-keys", "plain"]) as any[]) {
  const small = await time(name, name === "many-keys" || name === "plain" ? 1000 : 10);
  const big = await time(name, name === "many-keys" || name === "plain" ? 50_000 : 49_000);
  console.log(
    `${name.padEnd(10)} small backlog ${small.toFixed(1)} ms · 50k backlog ${big.toFixed(1)} ms`,
  );
}
await pg.stop();
