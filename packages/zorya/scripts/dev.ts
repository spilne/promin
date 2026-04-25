// ---------------------------------------------------------------------------
// zorya dev — builds the UI, runs the demo server, and watches both sides
// for changes.
//
//   UI changes (src/ui/**)         → rebuild + browser auto-reload via a
//                                    polling sentinel under dist/public/
//   Server changes (.ts in deps)   → bun --hot reloads the demo subprocess
//                                    in place (port stays bound)
//
// Usage:
//   bun run packages/zorya/scripts/dev.ts             # incremental, watching
//   bun run packages/zorya/scripts/dev.ts --rebuild   # nuke + rebuild first
//   bun run packages/zorya/scripts/dev.ts --no-watch  # one-shot, no watcher
//   PORT=4200 bun run packages/zorya/scripts/dev.ts
//
// Or from the repo root: `bun run zorya [--rebuild]`.
// ---------------------------------------------------------------------------

import { $ } from "bun";
import path from "node:path";
import { existsSync, watch } from "node:fs";
import { writeFile } from "node:fs/promises";

const pkgRoot = path.resolve(import.meta.dir, "..");
const indexHtml = path.join(pkgRoot, "dist", "public", "index.html");
const reloadFile = path.join(pkgRoot, "dist", "public", ".reload-timestamp");
const uiSrc = path.join(pkgRoot, "src", "ui");
const forceRebuild = process.argv.includes("--rebuild");
const noWatch = process.argv.includes("--no-watch");

async function runBuild(): Promise<void> {
  // ZORYA_DEV_RELOAD signals build-ui.ts to inject the polling client into
  // dist/public/index.html. Without it, prod-shaped builds skip the script
  // so end-user installs aren't polling for a sentinel that doesn't exist.
  await $`bun ${path.join(pkgRoot, "scripts", "build-ui.ts")}`
    .cwd(pkgRoot)
    .env({ ...process.env, ZORYA_DEV_RELOAD: "1" });
  // Sentinel must be written AFTER the build completes — the build clears
  // dist/public on each run, so writing earlier would lose the file.
  await writeFile(reloadFile, String(Date.now()));
}

if (forceRebuild || !existsSync(indexHtml)) {
  await runBuild();
} else {
  console.log("[zorya] UI cached at dist/public (pass --rebuild to force)");
  // Refresh the sentinel anyway so the polling client has a stable
  // baseline even when we skip the build.
  await writeFile(reloadFile, String(Date.now()));
}

// Spawn the demo server as a subprocess so we can:
//   1. pass --hot for module-level HMR (port + sqlite handle stay alive)
//   2. preserve --conditions=@promin/source so workspace imports resolve
//      to src/ rather than dist/, matching what the parent dev script set
console.log("[zorya] starting demo server (hot)…");
const serverProc = Bun.spawn({
  cmd: ["bun", "--hot", "--conditions=@promin/source", path.join(pkgRoot, "examples", "demo.ts")],
  stdout: "inherit",
  stderr: "inherit",
  stdin: "inherit",
  env: { ...process.env, ZORYA_DEV_RELOAD: "1" },
});

const stopAndExit = (code = 0) => {
  serverProc.kill();
  process.exit(code);
};
process.on("SIGINT", () => stopAndExit(0));
process.on("SIGTERM", () => stopAndExit(0));

// Watch the UI tree and rebuild on edit. Debounce so a flurry of saves
// (editor formatter, prettier-on-save, IDE multi-file rename) collapses
// into a single rebuild + reload.
if (!noWatch) {
  console.log(
    `[zorya] watching ${path.relative(process.cwd(), uiSrc)} for UI changes (Ctrl+C to stop)`,
  );
  let pending: ReturnType<typeof setTimeout> | undefined;
  let busy = false;
  let queued = false;

  watch(uiSrc, { recursive: true }, (_evt, filename) => {
    if (!filename) return;
    if (pending) clearTimeout(pending);
    pending = setTimeout(async () => {
      pending = undefined;
      if (busy) {
        // A rebuild was already in flight when the new save fired — note
        // it and run another build right after the current one finishes.
        queued = true;
        return;
      }
      busy = true;
      try {
        do {
          queued = false;
          console.log(`[zorya] rebuild → ${filename}`);
          await runBuild();
          console.log(`[zorya] reload broadcast`);
        } while (queued);
      } catch (err) {
        console.error("[zorya] rebuild failed:", (err as Error).message);
      } finally {
        busy = false;
      }
    }, 150);
  });
}

const exitCode = await serverProc.exited;
process.exit(exitCode ?? 0);
