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
// ---------------------------------------------------------------------------

import { spawn, $ } from "bun";
import path from "node:path";
import { existsSync, watch } from "node:fs";
import { writeFile } from "node:fs/promises";

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

const pkgRoot = path.resolve(import.meta.dir, "..");
const indexHtml = path.join(pkgRoot, "dist", "public", "index.html");
const reloadFile = path.join(pkgRoot, "dist", "public", ".reload-timestamp");
const uiSrc = path.join(pkgRoot, "src", "ui");
const buildScript = path.join(pkgRoot, "scripts", "build-ui.ts");

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

async function runBuild(): Promise<void> {
  // ZORYA_DEV_RELOAD makes build-ui.ts inject the polling client that
  // watches the .reload-timestamp sentinel — without it the dashboard
  // doesn't know about the rebuild and you have to hit cmd+R.
  await $`bun ${buildScript}`.cwd(pkgRoot).env({ ...process.env, ZORYA_DEV_RELOAD: "1" });
  // Write sentinel AFTER build (the build clears dist/public).
  await writeFile(reloadFile, String(Date.now()));
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

// 1. Build the UI (or refresh sentinel if already built).
if (FORCE_REBUILD || !existsSync(indexHtml)) {
  console.log(`${COLOURS.build}[build]${COLOURS.reset} building UI…`);
  await runBuild();
  console.log(`${COLOURS.build}[build]${COLOURS.reset} UI ready at dist/public/`);
} else {
  console.log(
    `${COLOURS.build}[build]${COLOURS.reset} UI cached at dist/public (pass --rebuild to force)`,
  );
  await writeFile(reloadFile, String(Date.now()));
}

// 2. Start the demo server with --hot for in-place server reload.
console.log(`[start-all] booting demo server on ${ZORYA_URL} (hot)…`);
children.push(
  start(
    "demo",
    ["bun", "--hot", "--conditions=@promin/source", path.join(pkgRoot, "examples", "demo.ts")],
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
      ["bun", "--hot", "--conditions=@promin/source", path.join(pkgRoot, "examples", "worker.ts")],
      { ZORYA_URL },
      COLOURS.worker,
    ),
  );

  if (process.env["USER_APP"] === "true") {
    console.log(`[start-all] USER_APP=true — running user-app.ts (one-shot)…`);
    children.push(
      start(
        "app",
        ["bun", "--conditions=@promin/source", path.join(pkgRoot, "examples", "user-app.ts")],
        { ZORYA_URL },
        COLOURS.app,
      ),
    );
  }

  // 4. Watch UI source for edits and rebuild. Debounced so a burst of
  //    saves (editor formatter, multi-file rename) collapses into one
  //    rebuild + browser reload.
  console.log(
    `[start-all] watching ${path.relative(process.cwd(), uiSrc)} for UI changes (Ctrl+C to stop)`,
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
        queued = true;
        return;
      }
      busy = true;
      try {
        do {
          queued = false;
          console.log(`${COLOURS.build}[build]${COLOURS.reset} rebuild → ${filename}`);
          await runBuild();
          console.log(`${COLOURS.build}[build]${COLOURS.reset} reload broadcast`);
        } while (queued);
      } catch (err) {
        console.error(
          `${COLOURS.build}[build]${COLOURS.reset} rebuild failed:`,
          (err as Error).message,
        );
      } finally {
        busy = false;
      }
    }, 150);
  });

  console.log(`[start-all] all up. dashboard: ${ZORYA_URL}/`);

  // Block until SIGINT/SIGTERM.
  await new Promise<void>(() => {});
} catch (err) {
  console.error(`[start-all] ${(err as Error).message}`);
  await shutdown("server-timeout");
}
