import { ROOT, DEPENDENCY_GROUPS, run, workspacePackages } from "./workspace.ts";

const STABLE = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

function git(args: string[]): string {
  const result = Bun.spawnSync(["git", ...args], { cwd: ROOT });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString().trim());
  return result.stdout.toString().trim();
}

export function nextVersion(current: string, tags: string[], requested: string): string {
  const versions = [current, ...tags.filter((t) => t.startsWith("v")).map((t) => t.slice(1))]
    .filter((v) => STABLE.test(v))
    .sort(Bun.semver.order);
  const latest = versions.at(-1)!;
  let version = requested;
  if (["patch", "minor", "major"].includes(requested)) {
    const [major, minor, patch] = latest.split(".").map(Number) as [number, number, number];
    version =
      requested === "major"
        ? `${major + 1}.0.0`
        : requested === "minor"
          ? `${major}.${minor + 1}.0`
          : `${major}.${minor}.${patch + 1}`;
  }
  if (!STABLE.test(version))
    throw new Error("VERSION must be patch, minor, major, or stable x.y.z");
  const first = !tags.some((t) => t.startsWith("v") && STABLE.test(t.slice(1)));
  if (Bun.semver.order(version, latest) <= 0 && !(first && version === current)) {
    throw new Error(`VERSION must be newer than ${latest}`);
  }
  if (tags.includes(`v${version}`)) throw new Error(`Tag v${version} already exists`);
  return version;
}

async function main(): Promise<void> {
  const packages = (await workspacePackages()).filter((p) => !p.manifest.private);
  const versions = new Set(packages.map((p) => p.manifest.version));
  if (versions.size !== 1) throw new Error("All public packages must share one version");
  const current = [...versions][0]!;
  if (!STABLE.test(current)) throw new Error(`Expected stable package version, got ${current}`);
  const args = process.argv.slice(2);
  if (args.includes("--verify-tag")) {
    if (args.length !== 1) throw new Error("Use --verify-tag alone");
    const tag = `v${current}`;
    if (process.env.GITHUB_REF && process.env.GITHUB_REF !== `refs/tags/${tag}`) {
      throw new Error(`Release must run from refs/tags/${tag}`);
    }
    if (git(["rev-parse", `${tag}^{commit}`]) !== git(["rev-parse", "HEAD"])) {
      throw new Error(`${tag} must point to HEAD`);
    }
    for (const { manifest } of packages) {
      for (const group of DEPENDENCY_GROUPS) {
        for (const dependency of packages) {
          const range = manifest[group]?.[dependency.manifest.name];
          if (range && ![current, `^${current}`, `~${current}`, `=${current}`].includes(range)) {
            throw new Error(
              `${manifest.name}: ${dependency.manifest.name} must reference ${current}, got ${range}`,
            );
          }
        }
      }
    }
    console.log(`Verified ${tag}: ${packages.length} public packages`);
    return;
  }
  if (args.some((a) => a !== "--dry-run")) throw new Error("Use VERSION=x.y.z bun run release");
  const dry = args.includes("--dry-run");
  const tags = git(["tag", "--list", "v*"]).split("\n");
  const version = nextVersion(current, tags, process.env.VERSION?.trim() || "patch");
  if (!dry) {
    if (git(["branch", "--show-current"]) !== "main") throw new Error("Release from main");
    if (git(["status", "--porcelain"])) throw new Error("Commit or stash changes before releasing");
    await run(["bun", "run", "ci"]);
    await run(["bun", "run", "release:check"]);
    await run(["bun", "run", "test:packages"]);
    await run(["bun", "run", "documentation:build:html"]);
    if (git(["status", "--porcelain"]))
      throw new Error("Validation changed tracked files; review before releasing");
  }
  await run([
    "bun",
    "nx",
    "release",
    version,
    "--skip-publish",
    ...(!tags.some((t) => STABLE.test(t.slice(1))) ? ["--first-release"] : []),
    ...(dry ? ["--dry-run"] : []),
  ]);
  if (!dry) console.log(`Release prepared. Push with: git push --atomic origin main v${version}`);
}

if (import.meta.main) await main();
