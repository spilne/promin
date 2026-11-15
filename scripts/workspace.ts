import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

export const ROOT = resolve(import.meta.dir, "..");
export const DEPENDENCY_GROUPS = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

export interface Manifest {
  name: string;
  version: string;
  private?: boolean;
  main?: string;
  types?: string;
  exports?: Record<string, string | Record<string, string>>;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
}

export interface WorkspacePackage {
  directory: string;
  manifest: Manifest;
}

export async function workspacePackages(root = ROOT): Promise<WorkspacePackage[]> {
  const entries = await readdir(join(root, "packages"), { withFileTypes: true });
  const packages: WorkspacePackage[] = [];
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isDirectory()) continue;
    const directory = join(root, "packages", entry.name);
    const file = Bun.file(join(directory, "package.json"));
    if (await file.exists()) packages.push({ directory, manifest: await file.json() });
  }
  return packages;
}

export function dependencyOrder(packages: WorkspacePackage[]): WorkspacePackage[] {
  const byName = new Map(packages.map((p) => [p.manifest.name, p]));
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const result: WorkspacePackage[] = [];
  const visit = (entry: WorkspacePackage): void => {
    const name = entry.manifest.name;
    if (visited.has(name)) return;
    if (visiting.has(name)) throw new Error(`Internal dependency cycle at ${name}`);
    visiting.add(name);
    for (const group of DEPENDENCY_GROUPS) {
      for (const dependency of Object.keys(entry.manifest[group] ?? {}).sort()) {
        const target = byName.get(dependency);
        if (target) visit(target);
      }
    }
    visiting.delete(name);
    visited.add(name);
    result.push(entry);
  };
  for (const entry of packages) visit(entry);
  return result;
}

export function assertPublishedManifest(manifest: Manifest): void {
  for (const group of DEPENDENCY_GROUPS) {
    for (const [name, range] of Object.entries(manifest[group] ?? {})) {
      if (/^(workspace:|catalog:|file:|link:)/.test(range)) {
        throw new Error(`${manifest.name}: unresolved ${group} ${name}: ${range}`);
      }
    }
  }
}

export async function run(command: string[], cwd = ROOT): Promise<void> {
  const child = Bun.spawn(command, {
    cwd,
    env: { ...process.env, NX_DAEMON: "false", NX_ISOLATE_PLUGINS: "false" },
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await child.exited) !== 0) throw new Error(`Failed: ${command.join(" ")}`);
}

export async function readManifest(path: string): Promise<Manifest> {
  return JSON.parse(await readFile(path, "utf8"));
}
