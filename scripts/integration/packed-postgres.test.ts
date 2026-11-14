import { expect, test } from "bun:test";
import { join } from "node:path";
import { symlink, writeFile } from "node:fs/promises";
import { PostgresTestContainer } from "../../packages/postgres/src/lib/test-utils.ts";
import { packPackages } from "../pack-packages.ts";
import { ROOT } from "../workspace.ts";

test("a packed Postgres consumer migrates using its shipped SQL and journal", async () => {
  const packed = await packPackages();
  const pg = new PostgresTestContainer();
  try {
    await pg.start();
    await symlink(join(ROOT, "node_modules"), join(packed.directory, "node_modules"), "dir");
    const postgres = packed.packages.find((p) => p.manifest.name === "@promin/postgres")!;
    // Load the packed entrypoint with no source condition. The default path
    // must find SQL beside this package, not in the repository checkout.
    await writeFile(
      join(packed.directory, "migrate-consumer.ts"),
      [
        `import { migrate } from ${JSON.stringify(join(postgres.unpacked, "dist/index.js"))};`,
        'import postgres from "postgres";',
        'import { drizzle } from "drizzle-orm/postgres-js";',
        "const client = postgres(process.env.TEST_DATABASE_URL!);",
        "try { const db = drizzle(client); await migrate(db); await migrate(db); }",
        "finally { await client.end(); }",
      ].join("\n"),
    );
    const child = Bun.spawn(["bun", "migrate-consumer.ts"], {
      cwd: packed.directory,
      env: { ...process.env, TEST_DATABASE_URL: pg.url },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [code, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
    expect(code, stderr).toBe(0);
    const applied = await pg.sql`SELECT count(*)::int AS n FROM drizzle.__drizzle_migrations`;
    const journal = await Bun.file(join(postgres.unpacked, "drizzle/meta/_journal.json")).json();
    expect(applied[0]!.n).toBe(journal.entries.length);
    const lane = await pg.sql`SELECT attgenerated FROM pg_attribute
      WHERE attrelid = 'wf_step_queue'::regclass AND attname = 'lane'`;
    expect(lane[0]!.attgenerated).toBe("s");
  } finally {
    await pg.stop();
    await packed.close();
  }
}, 120_000);
