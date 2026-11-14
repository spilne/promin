import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ROOT,
  assertPublishedManifest,
  dependencyOrder,
  readManifest,
  run,
  workspacePackages,
  type WorkspacePackage,
} from "./workspace.ts";

export interface PackedPackage extends WorkspacePackage {
  tarball: string;
  unpacked: string;
}

/** Inspect the tarball, rather than trusting the source manifest or build tree. */
export async function packPackages(root = ROOT): Promise<{
  directory: string;
  packages: PackedPackage[];
  close(): Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), "promin-release-"));
  try {
    const packages: PackedPackage[] = [];
    const entries = dependencyOrder(
      (await workspacePackages(root)).filter((p) => !p.manifest.private),
    );
    if (entries.length === 0) throw new Error("No public packages found");
    for (const entry of entries) {
      const name = entry.manifest.name.replaceAll(/[^a-zA-Z0-9-]/g, "-");
      const tarball = join(directory, `${name}.tgz`);
      const unpacked = join(directory, name);
      await mkdir(unpacked);
      await run(
        ["bun", "pm", "pack", "--quiet", "--ignore-scripts", "--filename", tarball],
        entry.directory,
      );
      await run(["tar", "-xzf", tarball, "--strip-components=1", "-C", unpacked], root);
      const manifest = await readManifest(join(unpacked, "package.json"));
      assertPublishedManifest(manifest);
      if (manifest.name !== entry.manifest.name || manifest.version !== entry.manifest.version) {
        throw new Error(`Packed manifest differs from source: ${entry.manifest.name}`);
      }
      packages.push({ ...entry, manifest, tarball, unpacked });
    }
    return { directory, packages, close: () => rm(directory, { recursive: true, force: true }) };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}
