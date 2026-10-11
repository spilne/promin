// ---------------------------------------------------------------------------
// Package manifests for the Zorya Docker image's runtime stage.
//
// The image runs the demo server from source (it scans example folders for
// workflows, agents and tools at boot, so it cannot be one bundle). Instead
// of the whole monorepo it carries only the workspaces the server and the
// demo import, with their runtime dependencies. This script writes, under
// `<outDir>`:
//   - package.json                 a root manifest for that reduced
//                                  workspace: the repo's catalog, the runtime
//                                  workspaces, and the demo's own imports
//   - packages/<name>/package.json each runtime workspace's manifest with
//                                  its dev dependencies removed
// A plain `bun install` there, next to the repo's bun.lock, installs exactly
// the runtime dependencies at their locked versions.
//
//   bun packages/zorya/scripts/image-manifest.ts /runtime
// ---------------------------------------------------------------------------

import { mkdirSync } from "node:fs";
import path from "node:path";

/** Workspaces the server and the demo load at run time (transitively closed). */
const IMAGE_WORKSPACES = [
  "agent",
  "evals",
  "postgres",
  "sqlite",
  "workflow",
  "workflow-remote",
  "zorya",
] as const;

const outDir = process.argv[2];
if (!outDir) throw new Error("usage: bun image-manifest.ts <outDir>");

const repoRoot = path.resolve(import.meta.dir, "..", "..", "..");
const readJson = async (file: string): Promise<Record<string, unknown>> =>
  (await Bun.file(file).json()) as Record<string, unknown>;
const writeJson = async (file: string, value: unknown): Promise<void> => {
  mkdirSync(path.dirname(file), { recursive: true });
  await Bun.write(file, `${JSON.stringify(value, null, 2)}\n`);
};

const root = (await readJson(path.join(repoRoot, "package.json"))) as {
  workspaces: { catalog: Record<string, string> };
};

await writeJson(path.join(outDir, "package.json"), {
  name: "zorya-image",
  private: true,
  workspaces: {
    packages: IMAGE_WORKSPACES.map((name) => `packages/${name}`),
    catalog: root.workspaces.catalog,
  },
  // The demo imports these directly; for @promin/zorya they are dev
  // dependencies, which are dropped below.
  dependencies: {
    "@promin/postgres": "workspace:*",
    "@promin/sqlite": "workspace:*",
    "@promin/zorya": "workspace:*",
    "@spilne/perfect-core": "catalog:",
    "@spilne/perfect-postgres": "catalog:",
  },
});

for (const name of IMAGE_WORKSPACES) {
  const manifest = await readJson(path.join(repoRoot, "packages", name, "package.json"));
  delete manifest.devDependencies;
  delete manifest.scripts;
  delete manifest.nx;
  await writeJson(path.join(outDir, "packages", name, "package.json"), manifest);
}
