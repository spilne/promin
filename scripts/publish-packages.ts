import { packPackages } from "./pack-packages.ts";
import { ROOT, run } from "./workspace.ts";

export async function isPublished(
  name: string,
  version: string,
  registry: string,
): Promise<boolean> {
  const response = await fetch(
    `${registry.replace(/\/$/, "")}/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
  );
  if (response.ok) return true;
  if (response.status === 404) return false;
  throw new Error(`Registry lookup for ${name}@${version} failed: HTTP ${response.status}`);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.some((a) => a !== "--dry-run")) throw new Error("Only --dry-run is supported");
  const dry = args.includes("--dry-run");
  if (!dry) {
    await run(["bun", "scripts/release.ts", "--verify-tag"]);
    await run(["bun", "pm", "whoami"]);
  }
  const packed = await packPackages();
  try {
    // Validate every package before publishing the first one. Retrying the
    // same immutable tag skips versions a previous attempt already published.
    for (const entry of packed.packages) {
      const { name, version } = entry.manifest;
      if (dry) {
        console.log(`checked ${name}@${version}`);
      } else if (
        await isPublished(
          name,
          version,
          process.env.NPM_CONFIG_REGISTRY ?? "https://registry.npmjs.org",
        )
      ) {
        console.log(`skip ${name}@${version}: already published`);
      } else {
        await run(
          [
            "bun",
            "publish",
            entry.tarball,
            "--access",
            "public",
            "--tag",
            process.env.PROMIN_NPM_TAG ?? "latest",
            "--no-save",
            "--ignore-scripts",
          ],
          ROOT,
        );
      }
    }
  } finally {
    await packed.close();
  }
}

if (import.meta.main) await main();
