import { afterEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { FakeWallClock, workflow } from "@promin/workflow";
import { createSqliteZoryaStack, type SqliteZoryaStack } from "../sqlite-stack.ts";

const stacks: SqliteZoryaStack[] = [];
const dirs: string[] = [];

afterEach(() => {
  for (const stack of stacks.splice(0)) stack.db.close();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function open(config: Parameters<typeof createSqliteZoryaStack>[0]): SqliteZoryaStack {
  const stack = createSqliteZoryaStack(config);
  stacks.push(stack);
  return stack;
}

describe("createSqliteZoryaStack", () => {
  it("runs workflows through its runner into its storage", async () => {
    const clock = FakeWallClock.create(1_700_000_000_000);
    const stack = open({ path: ":memory:", clock });
    const wf = workflow<number>({ name: "double" })
      .step("double", ({ input }) => input * 2)
      .build();

    await expect(stack.runner.run({ workflow: wf, workflowId: "d-1", input: 21 })).resolves.toBe(
      42,
    );
    const state = await stack.storage.loadWorkflow("d-1");
    expect(state?.status).toBe("completed");
    expect(state?.createdAt.getTime()).toBe(1_700_000_000_000);
  });

  it("has no secrets storage without a passphrase", () => {
    expect(open({ path: ":memory:" }).secretsStorage).toBeUndefined();
  });

  it("stores secrets under the given passphrase", async () => {
    const stack = open({ path: ":memory:", secretsPassphrase: "test-passphrase" });
    const secrets = stack.secretsStorage;
    expect(secrets).toBeDefined();
    const scope = { kind: "global" } as const;
    await secrets!.set({ scope, key: "api-key", value: "s3cret" });
    await expect(secrets!.get({ scope, key: "api-key" })).resolves.toBe("s3cret");
  });

  it("keeps namespaces in the shared database", async () => {
    const clock = FakeWallClock.create(1_700_000_000_000);
    const stack = open({ path: ":memory:", clock });
    const created = await stack.namespaceRegistry.create({ id: "acme", displayName: "Acme" });
    expect(created.createdAt.getTime()).toBe(1_700_000_000_000);
    const tables = stack.db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table'")
      .all()
      .map((r) => r.name);
    expect(tables).toContain("promin_wf_namespaces");
  });

  it("creates the database file's directory and shares one database", async () => {
    const dir = mkdtempSync(path.join(tmpdir(), "zorya-stack-"));
    dirs.push(dir);
    const dbPath = path.join(dir, "nested", "zorya.db");

    const first = open({ path: dbPath });
    expect(existsSync(dbPath)).toBe(true);
    const wf = workflow<null>({ name: "noop" })
      .step("noop", () => "ok")
      .build();
    await first.runner.run({ workflow: wf, workflowId: "n-1", input: null });
    first.db.close();
    stacks.splice(stacks.indexOf(first), 1);

    const reopened = open({ path: dbPath });
    expect((await reopened.storage.loadWorkflow("n-1"))?.status).toBe("completed");
  });
});
