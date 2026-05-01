// ---------------------------------------------------------------------------
// start-all.ts — boot the demo server + remote worker together with UI
// auto-rebuild and server-side hot-reload.
//
// Run:
//   bun --conditions=@promin/source run packages/zorya/examples/start-all.ts
//   # or from the repo root:
//   bun run demo
//
// Three things happen on boot:
//   1. UI is built once (src/ui/** → dist/public/) — skipped if a recent
//      build exists; pass --rebuild to force.
//   2. demo.ts is spawned with --hot so server-side .ts saves reload in
//      place (port + sqlite handle stay bound).
//   3. /api/health polls until green, then worker.ts joins.
//
// While running:
//   - Edits under packages/zorya/src/ui/** trigger a debounced rebuild;
//     the dashboard polls a sentinel file under dist/public and reloads
//     when it changes.
//   - Edits under .ts files imported by demo.ts hot-reload the server
//     (Bun --hot does this automatically).
//   - SIGINT / SIGTERM shut down both children cleanly.
//
// USER_APP=true also spawns user-app.ts as a third (one-shot) process.
// --rebuild forces a fresh UI build even if dist/public already exists.
//
// UI build + watch lives in `../scripts/dev-ui.ts`, shared with `dev.ts`.
// ---------------------------------------------------------------------------

import { spawn } from "bun";
import path from "node:path";
import { ZORYA_DEV_PATHS, ensureUiBuilt, watchUi } from "../scripts/dev-ui.ts";

const PORT = process.env["PORT"] ?? "4100";
const ZORYA_URL = `http://localhost:${PORT}`;
const FORCE_REBUILD = process.argv.includes("--rebuild");

const COLOURS = {
  build: "\x1b[34m", // blue
  demo: "\x1b[36m", // cyan
  worker: "\x1b[35m", // magenta
  app: "\x1b[33m", // yellow
  reset: "\x1b[0m",
};

interface Child {
  label: string;
  proc: ReturnType<typeof spawn>;
}
const children: Child[] = [];

async function pipePrefixed(stream: ReadableStream<Uint8Array>, label: string, colour: string) {
  const decoder = new TextDecoder();
  const reader = stream.getReader();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      process.stdout.write(`${colour}[${label}]${COLOURS.reset} ${line}\n`);
    }
  }
  if (buffer.length > 0) {
    process.stdout.write(`${colour}[${label}]${COLOURS.reset} ${buffer}\n`);
  }
}

function start(label: string, cmd: string[], env: Record<string, string>, colour: string): Child {
  const proc = spawn({
    cmd,
    env: { ...process.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  void pipePrefixed(proc.stdout as unknown as ReadableStream<Uint8Array>, label, colour);
  void pipePrefixed(proc.stderr as unknown as ReadableStream<Uint8Array>, label, colour);
  return { label, proc };
}

async function waitForServer(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${url}/api/health`, { signal: AbortSignal.timeout(1_000) });
      if (res.ok) return;
    } catch {
      // server not ready yet
    }
    await Bun.sleep(500);
  }
  throw new Error(`Server at ${url} did not become ready within ${timeoutMs}ms`);
}

async function shutdown(signal: string): Promise<void> {
  console.log(`\n[start-all] ${signal} — stopping ${children.length} child process(es)…`);
  for (const c of children) {
    try {
      c.proc.kill();
    } catch {
      // already gone
    }
  }
  await Promise.all(children.map((c) => c.proc.exited.catch(() => {})));
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

const buildLog = (line: string) =>
  process.stdout.write(
    `${COLOURS.build}[build]${COLOURS.reset} ${line.replace(/^\[zorya\] /, "")}\n`,
  );

// 1. Build the UI (or refresh sentinel if already built).
await ensureUiBuilt({ forceRebuild: FORCE_REBUILD, log: buildLog });

// 2. Start the demo server with --hot for in-place server reload.
console.log(`[start-all] booting demo server on ${ZORYA_URL} (hot)…`);
children.push(
  start(
    "demo",
    ["bun", "--hot", "--conditions=@promin/source", ZORYA_DEV_PATHS.demoScript],
    { PORT, ZORYA_DEV_RELOAD: "1" },
    COLOURS.demo,
  ),
);

try {
  await waitForServer(ZORYA_URL);

  // 3. Worker joins.
  console.log(`[start-all] server up. starting worker…`);
  children.push(
    start(
      "worker",
      ["bun", "--hot", "--conditions=@promin/source", ZORYA_DEV_PATHS.workerScript],
      { ZORYA_URL },
      COLOURS.worker,
    ),
  );

  if (process.env["USER_APP"] === "true") {
    console.log(`[start-all] USER_APP=true — running user-app.ts (one-shot)…`);
    children.push(
      start(
        "app",
        ["bun", "--conditions=@promin/source", ZORYA_DEV_PATHS.userAppScript],
        { ZORYA_URL },
        COLOURS.app,
      ),
    );
  }

  // 4. Watch UI source for edits and rebuild via the shared helper.
  console.log(
    `[start-all] watching ${path.relative(process.cwd(), ZORYA_DEV_PATHS.uiSrc)} for UI changes (Ctrl+C to stop)`,
  );
  watchUi({
    onRebuildStart: (filename) => buildLog(`rebuild → ${filename}`),
    onRebuildDone: () => buildLog("reload broadcast"),
    onRebuildError: (err) => buildLog(`rebuild failed: ${err.message}`),
  });

  console.log(`[start-all] all up. dashboard: ${ZORYA_URL}/`);

  // Block until SIGINT/SIGTERM.
  await new Promise<void>(() => {});
} catch (err) {
  console.error(`[start-all] ${(err as Error).message}`);
  await shutdown("server-timeout");
}
