import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const PACKAGE = resolve(import.meta.dir, "../../..");

describe("PostgreSQL security package", () => {
  let packDir = "";
  let packedFiles: string[] = [];

  beforeAll(async () => {
    packDir = await mkdtemp(join(tmpdir(), "promin-zorya-security-pack-"));
    const archive = join(packDir, "zorya.tgz");
    const pack = Bun.spawn({
      cmd: [process.execPath, "pm", "pack", "--filename", archive, "--ignore-scripts", "--quiet"],
      cwd: PACKAGE,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [packCode, packStdout, packStderr] = await Promise.all([
      pack.exited,
      new Response(pack.stdout).text(),
      new Response(pack.stderr).text(),
    ]);
    expect({ packCode, packStderr }).toEqual({ packCode: 0, packStderr: "" });
    expect(packStdout.trim()).toEndWith("zorya.tgz");

    const tar = Bun.spawn({
      cmd: ["tar", "-tzf", archive],
      stdout: "pipe",
      stderr: "pipe",
    });
    const [tarCode, tarStdout, tarStderr] = await Promise.all([
      tar.exited,
      new Response(tar.stdout).text(),
      new Response(tar.stderr).text(),
    ]);
    expect({ tarCode, tarStderr: tarCode === 0 ? "" : tarStderr }).toEqual({
      tarCode: 0,
      tarStderr: "",
    });
    packedFiles = tarStdout.trim().split("\n");
  });

  afterAll(async () => {
    if (packDir) await rm(packDir, { recursive: true, force: true });
  });

  it("ships the dedicated migration stream", () => {
    expect(packedFiles).toContain("package/security-drizzle/0000_security_baseline.sql");
    expect(packedFiles).toContain("package/security-drizzle/meta/_journal.json");
  });

  it("publishes only the opt-in PostgreSQL subpath", async () => {
    const manifest = (await Bun.file(join(PACKAGE, "package.json")).json()) as {
      exports: Record<string, Record<string, string>>;
      files: string[];
      peerDependenciesMeta: Record<string, { optional?: boolean }>;
    };

    expect(manifest.files).toContain("security-drizzle");
    expect(manifest.exports["./security/postgres"]).toEqual({
      types: "./dist/security-postgres.d.ts",
      "@promin/source": "./src/security-postgres.ts",
      import: "./dist/security-postgres.js",
      default: "./dist/security-postgres.js",
    });
    for (const peer of ["@spilne/perfect-postgres", "drizzle-orm", "postgres"]) {
      expect(manifest.peerDependenciesMeta[peer]?.optional).toBe(true);
    }
  });
});
