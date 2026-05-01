// ---------------------------------------------------------------------------
// start-all.ts — boot the demo server + remote worker together.
//
// Run:
//   bun --conditions=@promin/source run packages/zorya/examples/start-all.ts
//
// Spawns two processes:
//   1. demo.ts     — the dashboard / server (port 4100 by default)
//   2. worker.ts   — joins the server, advertises hello-world + fan-out-demo
//
// Both stdout / stderr streams are forwarded with prefixes so you can see
// what came from where. Ctrl+C cleans up both children.
//
// To run the third pattern (user-app.ts demo) in the same shell, set
// USER_APP=true — it runs once and exits while demo + worker keep going.
// ---------------------------------------------------------------------------

import { spawn } from "bun";

const PORT = process.env["PORT"] ?? "4100";
const ZORYA_URL = `http://localhost:${PORT}`;

const COLOURS = {
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

function start(label: string, file: string, env: Record<string, string>, colour: string): Child {
  const proc = spawn({
    cmd: ["bun", "--conditions=@promin/source", "run", file],
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

console.log(`[start-all] booting demo server on ${ZORYA_URL}…`);
const demoDir = import.meta.dir;
children.push(start("demo", `${demoDir}/demo.ts`, { PORT }, COLOURS.demo));

try {
  await waitForServer(ZORYA_URL);

  console.log(`[start-all] server up. starting worker…`);
  children.push(start("worker", `${demoDir}/worker.ts`, { ZORYA_URL }, COLOURS.worker));

  if (process.env["USER_APP"] === "true") {
    console.log(`[start-all] USER_APP=true — running user-app.ts (one-shot)…`);
    children.push(start("app", `${demoDir}/user-app.ts`, { ZORYA_URL }, COLOURS.app));
  }

  console.log(`[start-all] all up. dashboard: ${ZORYA_URL}/  (Ctrl+C to stop)`);

  // Block forever until SIGINT/SIGTERM.
  await new Promise<void>(() => {});
} catch (err) {
  console.error(`[start-all] ${(err as Error).message}`);
  await shutdown("server-timeout");
}
