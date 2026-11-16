import { describe, expect, it } from "bun:test";

describe("root import isolation", () => {
  it("does not load the optional jose-backed security module", async () => {
    const script = [
      'import { mock } from "bun:test";',
      'mock.module("jose", () => { throw new Error("root import loaded jose"); });',
      'await import("@promin/zorya");',
    ].join("\n");
    const child = Bun.spawn({
      cmd: [process.execPath, "--conditions=@promin/source", "-e", script],
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, FORCE_COLOR: "0" },
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);

    expect({ exitCode, stdout, stderr }).toEqual({ exitCode: 0, stdout: "", stderr: "" });
  });

  it("keeps optional PostgreSQL peers out of the root and security subpaths", async () => {
    const script = [
      'import { mock } from "bun:test";',
      "const fail = (name) => () => { throw new Error(`optional DB peer loaded: ${name}`); };",
      'mock.module("@spilne/perfect-postgres", fail("@spilne/perfect-postgres"));',
      'mock.module("drizzle-orm", fail("drizzle-orm"));',
      'mock.module("drizzle-orm/pg-core", fail("drizzle-orm/pg-core"));',
      'mock.module("drizzle-orm/postgres-js/migrator", fail("drizzle migrator"));',
      'mock.module("postgres", fail("postgres"));',
      'await import("@promin/zorya");',
      'await import("@promin/zorya/security");',
    ].join("\n");
    const child = Bun.spawn({
      cmd: [process.execPath, "--conditions=@promin/source", "-e", script],
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, FORCE_COLOR: "0" },
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);

    expect({ exitCode, stdout, stderr }).toEqual({ exitCode: 0, stdout: "", stderr: "" });
  });
});
