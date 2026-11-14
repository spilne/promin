import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { ROOT } from "../workspace.ts";

async function command(cwd: string, args: string[], version = "", ref = "") {
  const child = Bun.spawn(args, {
    cwd,
    env: {
      ...process.env,
      VERSION: version,
      GITHUB_REF: ref,
      NX_DAEMON: "false",
      NX_ISOLATE_PLUGINS: "false",
      BUN_INSTALL_CACHE_DIR: join(cwd, ".bun-cache"),
      CI: "true",
    },
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, output: stdout + stderr };
}

async function fixture(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "promin-release-cli-"));
  await mkdir(join(dir, "scripts"));
  await symlink(join(ROOT, "node_modules"), join(dir, "node_modules"), "dir");
  for (const name of ["release.ts", "workspace.ts"]) {
    await writeFile(join(dir, "scripts", name), await readFile(join(ROOT, "scripts", name)));
  }
  const nx = await Bun.file(join(ROOT, "nx.json")).json();
  // The fixture exercises Nx's installed release implementation with only
  // manifest metadata; no production projects or source graphs are loaded.
  delete nx.plugins;
  await writeFile(join(dir, "nx.json"), JSON.stringify(nx));
  await writeFile(
    join(dir, "package.json"),
    JSON.stringify({
      name: "release-fixture",
      private: true,
      packageManager: "bun@1.4.4",
      workspaces: ["packages/*"],
      scripts: {
        ci: "bun --version",
        "release:check": "bun --version",
        "test:packages": "bun --version",
        "documentation:build:html": "bun --version",
      },
    }),
  );
  await writeFile(join(dir, ".gitignore"), "node_modules/\n.nx/\n.bun-cache/\n");
  for (const name of ["base", "app"]) {
    await mkdir(join(dir, "packages", name), { recursive: true });
    await writeFile(
      join(dir, "packages", name, "package.json"),
      JSON.stringify({
        name: `@promin/${name}`,
        version: "0.1.0",
        ...(name === "app"
          ? {
              dependencies: { "@promin/base": "workspace:*" },
              peerDependencies: { "@promin/base": "workspace:*" },
            }
          : {}),
      }),
    );
  }
  for (const args of [
    ["git", "init", "-b", "main"],
    ["git", "config", "user.name", "Release test"],
    ["git", "config", "user.email", "test@example.com"],
    ["git", "add", "."],
    ["git", "commit", "-m", "feat: fixture"],
  ]) {
    const result = await command(dir, args);
    if (result.code !== 0) throw new Error(result.output);
  }
  return dir;
}

test("Nx release commits versions, all dependency ranges and a frozen lockfile; tag verification fails closed", async () => {
  const dir = await fixture();
  try {
    const release = await command(dir, ["bun", "scripts/release.ts"], "0.2.0");
    expect(release.code, release.output).toBe(0);
    const app = await Bun.file(join(dir, "packages/app/package.json")).json();
    expect(app.version).toBe("0.2.0");
    expect(app.dependencies["@promin/base"]).toBe("^0.2.0");
    expect(app.peerDependencies["@promin/base"]).toBe("^0.2.0");
    expect((await command(dir, ["git", "status", "--porcelain"])).output).toBe("");
    const frozen = await command(dir, ["bun", "install", "--frozen-lockfile", "--lockfile-only"]);
    expect(frozen.code, frozen.output).toBe(0);
    const verify = await command(
      dir,
      ["bun", "scripts/release.ts", "--verify-tag"],
      "",
      "refs/tags/v0.2.0",
    );
    expect(verify.code, verify.output).toBe(0);
    expect(
      (await command(dir, ["bun", "scripts/release.ts", "--verify-tag"], "", "refs/tags/v0.3.0"))
        .code,
    ).not.toBe(0);
    await command(dir, ["git", "commit", "--allow-empty", "-m", "chore: after tag"]);
    expect((await command(dir, ["bun", "scripts/release.ts", "--verify-tag"])).code).not.toBe(0);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}, 60_000);

test("release preview does not mutate files or tags and a dirty release is rejected", async () => {
  const dir = await fixture();
  try {
    await writeFile(join(dir, "pending.txt"), "uncommitted");
    const release = await command(dir, ["bun", "scripts/release.ts"], "0.2.0");
    expect(release.code).not.toBe(0);
    expect(release.output).toContain("Commit or stash");
    const before = (await command(dir, ["git", "status", "--porcelain"])).output;
    const preview = await command(dir, ["bun", "scripts/release.ts", "--dry-run"], "0.2.0");
    expect(preview.code, preview.output).toBe(0);
    expect((await command(dir, ["git", "status", "--porcelain"])).output).toBe(before);
    expect((await command(dir, ["git", "tag", "--list"])).output).toBe("");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}, 60_000);
