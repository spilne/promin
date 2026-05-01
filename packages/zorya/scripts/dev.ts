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
// Sister script `examples/start-all.ts` (`bun run demo`) layers a worker
// on top of this same loop. UI build + watch lives in `dev-ui.ts`.
// ---------------------------------------------------------------------------

import path from "node:path";
import { ZORYA_DEV_PATHS, ensureUiBuilt, watchUi } from "./dev-ui.ts";

const forceRebuild = process.argv.includes("--rebuild");
const noWatch = process.argv.includes("--no-watch");

await ensureUiBuilt({ forceRebuild });

// Spawn the demo server as a subprocess so we can:
//   1. pass --hot for module-level HMR (port + sqlite handle stay alive)
//   2. preserve --conditions=@promin/source so workspace imports resolve
//      to src/ rather than dist/, matching what the parent dev script set
console.log("[zorya] starting demo server (hot)…");
const serverProc = Bun.spawn({
  cmd: ["bun", "--hot", "--conditions=@promin/source", ZORYA_DEV_PATHS.demoScript],
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

if (!noWatch) {
  console.log(
    `[zorya] watching ${path.relative(process.cwd(), ZORYA_DEV_PATHS.uiSrc)} for UI changes (Ctrl+C to stop)`,
  );
  watchUi({
    onRebuildStart: (filename) => console.log(`[zorya] rebuild → ${filename}`),
    onRebuildDone: () => console.log(`[zorya] reload broadcast`),
    onRebuildError: (err) => console.error("[zorya] rebuild failed:", err.message),
  });
}

const exitCode = await serverProc.exited;
process.exit(exitCode ?? 0);
