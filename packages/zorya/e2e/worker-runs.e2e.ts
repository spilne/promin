// ---------------------------------------------------------------------------
// Worker-driven runs across real processes on a real Postgres
// (testcontainers): Zorya hosts and workers are separate `bun` processes
// on real ports, killed with SIGKILL where a test says so.
//
//   - a worker killed mid-run: another worker resumes the run and finishes it
//   - Zorya killed and restarted mid-run: the worker's writes ride it out
//   - sleeps, signals (over the HTTP API) and child workflows on workers
//   - a rolling deploy: v1 and v2 workers each run only their version
//   - two Zorya instances on one database, workers attached to different
//     instances, one worker registry visible from both
//   - orphan recovery never takes over a run a live worker is driving
//
// These are about processes and wall-clock leases, so they use real time,
// with short leases and scan intervals passed through the environment —
// long enough that a few seconds' stall of a loaded machine is no lost
// lease. Not part of `bun run test`: run it with `bun run test:e2e` (the
// zorya `e2e` target).
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, expect, it } from "bun:test";
import path from "node:path";
import { postgresDescribe } from "@promin/postgres/testing";
import { migrate, PostgresWorkflowStorage } from "@promin/postgres";

const E2E_DIR = import.meta.dir;
const TEST_TIMEOUT_MS = 120_000;

/** Fast leases and scans, so recovery happens within seconds. */
const HOST_ENV = {
  RECLAIM_MS: "10000",
  SCAN_MS: "200",
  MAINT_MS: "500",
  GRACE_MS: "1000",
  ADV_TTL_MS: "10000",
  WORKER_TIMEOUT_MS: "5000",
};
const WORKER_ENV = {
  LOCK_MS: "10000",
  CLAIM_HEARTBEAT_MS: "2000",
  POLL_MS: "100",
  HEARTBEAT_MS: "1000",
  LOCKED_RETRY_MS: "500",
  QUIET: "1",
  STOP_TIMEOUT_MS: "1000",
};

/** A spawned `bun` process whose stdout is scanned for readiness lines. */
class Proc {
  readonly child: ReturnType<typeof Bun.spawn>;
  private out = "";
  private readonly exited: Promise<number>;

  constructor(params: { script: string; env: Record<string, string> }) {
    this.child = Bun.spawn(
      ["bun", "--conditions=@promin/source", path.join(E2E_DIR, params.script)],
      { env: { ...process.env, ...params.env }, stdout: "pipe", stderr: "pipe" },
    );
    void this.drain(this.child.stdout as ReadableStream<Uint8Array>, true);
    void this.drain(this.child.stderr as ReadableStream<Uint8Array>, false);
    this.exited = this.child.exited;
  }

  get pid(): number {
    return this.child.pid;
  }

  async waitForLine(pattern: RegExp, timeoutMs = 30_000): Promise<string> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const line = this.out.split("\n").find((l) => pattern.test(l));
      if (line) return line;
      await sleep(25);
    }
    throw new Error(`no line matching ${pattern} within ${timeoutMs}ms; got:\n${this.out}`);
  }

  /** `kill -9`: no cleanup of any kind. */
  async kill9(): Promise<void> {
    this.child.kill(9);
    await this.exited;
  }

  async stop(): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.kill("SIGTERM");
    const done = await Promise.race([
      this.exited.then(() => true),
      sleep(15_000).then(() => false),
    ]);
    if (!done) await this.kill9();
  }

  private async drain(stream: ReadableStream<Uint8Array>, keep: boolean): Promise<void> {
    const decoder = new TextDecoder();
    for await (const chunk of stream) {
      const text = decoder.decode(chunk);
      if (keep) this.out += text;
      else if (process.env.E2E_VERBOSE) process.stderr.write(text);
    }
  }
}

const procs: Proc[] = [];
afterEach(async () => {
  await Promise.all(procs.splice(0).map((p) => p.stop()));
}, 30_000);

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitUntil(params: {
  what: string;
  check: () => Promise<boolean>;
  timeoutMs?: number;
}): Promise<void> {
  const deadline = Date.now() + (params.timeoutMs ?? 60_000);
  while (Date.now() < deadline) {
    if (await params.check()) return;
    await sleep(100);
  }
  throw new Error(`timed out waiting for ${params.what}`);
}

async function freePort(): Promise<number> {
  const probe = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response("") });
  const port = probe.port as number;
  await probe.stop(true);
  return port;
}

postgresDescribe("worker-driven runs across processes", { migrate }, (pg) => {
  let storage: PostgresWorkflowStorage;
  beforeAll(async () => {
    storage = await PostgresWorkflowStorage.create({ db: pg.db });
  });

  async function startHost(params: {
    port: number;
    instanceId: string;
    cluster?: boolean;
  }): Promise<{ proc: Proc; url: string }> {
    const proc = new Proc({
      script: "host.ts",
      env: {
        ...HOST_ENV,
        DATABASE_URL: pg.url,
        PORT: String(params.port),
        INSTANCE_ID: params.instanceId,
        ...(params.cluster && { CLUSTER: "1" }),
      },
    });
    procs.push(proc);
    await proc.waitForLine(/^READY /);
    return { proc, url: `http://127.0.0.1:${params.port}` };
  }

  async function startWorker(params: {
    url: string;
    workerId: string;
    version?: string;
  }): Promise<Proc> {
    const proc = new Proc({
      script: "worker.ts",
      env: {
        ...WORKER_ENV,
        ZORYA_URL: params.url,
        WORKER_ID: params.workerId,
        ...(params.version !== undefined && { VERSION: params.version }),
      },
    });
    procs.push(proc);
    await proc.waitForLine(/^READY /);
    return proc;
  }

  async function trigger(params: {
    url: string;
    name: string;
    input: unknown;
    workflowId: string;
    version?: string;
  }): Promise<void> {
    const res = await fetch(`${params.url}/api/workflows/${params.name}/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        input: params.input,
        workflowId: params.workflowId,
        ...(params.version !== undefined && { version: params.version }),
      }),
    });
    expect(res.status).toBeLessThan(300);
  }

  const status = async (id: string) => (await storage.loadWorkflowStatus(id))?.status;
  const completed = (ids: string[]) => async () =>
    (await Promise.all(ids.map(status))).every((s) => s === "completed");
  const stepResult = async (id: string, step: string) =>
    (await storage.loadWorkflow(id))?.steps[step]?.result as { pid?: number } | undefined;

  it(
    "a worker killed mid-run: another worker resumes the run and completes it",
    async () => {
      const { url } = await startHost({ port: await freePort(), instanceId: "kill-host" });
      const doomed = await startWorker({ url, workerId: "doomed" });
      await trigger({ url, name: "e2e-slow", input: { ms: 3_000 }, workflowId: "kill-1" });
      await waitUntil({
        what: "the first step",
        check: async () => (await stepResult("kill-1", "first")) !== undefined,
      });
      await doomed.kill9();

      const survivor = await startWorker({ url, workerId: "survivor" });
      await waitUntil({ what: "kill-1 to complete", check: completed(["kill-1"]) });
      expect((await stepResult("kill-1", "first"))?.pid).toBe(doomed.pid);
      expect((await stepResult("kill-1", "second"))?.pid).toBe(survivor.pid);
    },
    TEST_TIMEOUT_MS,
  );

  it(
    "Zorya killed and restarted mid-run: the worker's writes ride it out",
    async () => {
      const port = await freePort();
      const first = await startHost({ port, instanceId: "restart-host" });
      const worker = await startWorker({ url: first.url, workerId: "steady" });
      await trigger({ url: first.url, name: "e2e-slow", input: { ms: 2_500 }, workflowId: "rs-1" });
      await waitUntil({
        what: "the first step",
        check: async () => (await stepResult("rs-1", "first")) !== undefined,
      });
      await first.proc.kill9();
      await sleep(1_500);
      const second = await startHost({ port, instanceId: "restart-host" });
      await waitUntil({ what: "rs-1 to complete", check: completed(["rs-1"]) });
      expect((await stepResult("rs-1", "second"))?.pid).toBe(worker.pid);
      // Nothing is left queued. The worker completes its start just after
      // the run reaches `completed`, so wait for the queue to drain.
      await waitUntil({
        what: "the start queue to drain",
        check: async () => {
          const res = await fetch(`${second.url}/api/worker-protocol/starts`);
          return ((await res.json()) as { starts: unknown[] }).starts.length === 0;
        },
        timeoutMs: 15_000,
      });
    },
    TEST_TIMEOUT_MS,
  );

  it(
    "sleeps, signals and child workflows resume on workers",
    async () => {
      const { url } = await startHost({ port: await freePort(), instanceId: "wake-host" });
      await startWorker({ url, workerId: "waker" });
      await trigger({ url, name: "e2e-sleeper", input: { ms: 0 }, workflowId: "sl-1" });
      await trigger({ url, name: "e2e-waiter", input: { n: 1 }, workflowId: "wt-1" });
      await trigger({ url, name: "e2e-parent", input: { n: 5, id: "pa-1" }, workflowId: "pa-1" });
      await waitUntil({
        what: "the waiter to park",
        check: async () => (await status("wt-1")) === "suspended",
      });
      const res = await fetch(`${url}/api/runs/wt-1/signal`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ signalName: "go", payload: { add: 41 } }),
      });
      expect(res.status).toBeLessThan(300);

      await waitUntil({ what: "all three", check: completed(["sl-1", "wt-1", "pa-1"]) });
      expect((await storage.loadWorkflow("wt-1"))?.result).toBe(42);
      expect((await storage.loadWorkflow("pa-1"))?.result).toBe(11);
      expect(await status("pa-1-kid")).toBe("completed");
    },
    TEST_TIMEOUT_MS,
  );

  it(
    "a rolling deploy: each run is driven by a worker hosting its version",
    async () => {
      const { url } = await startHost({ port: await freePort(), instanceId: "deploy-host" });
      await startWorker({ url, workerId: "v1-worker", version: "1" });
      await startWorker({ url, workerId: "v2-worker", version: "2" });
      const ids: string[] = [];
      for (let i = 0; i < 5; i++) {
        await trigger({ url, name: "e2e-versioned", input: {}, workflowId: `ver-new-${i}` });
        await trigger({
          url,
          name: "e2e-versioned",
          input: {},
          workflowId: `ver-old-${i}`,
          version: "1",
        });
        ids.push(`ver-new-${i}`, `ver-old-${i}`);
      }
      await waitUntil({ what: "every versioned run", check: completed(ids) });
      for (const id of ids) {
        const run = await storage.loadWorkflow(id);
        expect((run?.result as { version?: string } | undefined)?.version).toBe(run?.version);
        expect(run?.version).toBe(id.startsWith("ver-old") ? "1" : "2");
      }
    },
    TEST_TIMEOUT_MS,
  );

  it(
    "two Zorya instances on one database serve workers attached to either",
    async () => {
      const a = await startHost({ port: await freePort(), instanceId: "inst-a", cluster: true });
      const b = await startHost({ port: await freePort(), instanceId: "inst-b", cluster: true });
      await startWorker({ url: a.url, workerId: "on-a" });
      await startWorker({ url: b.url, workerId: "on-b" });
      const ids: string[] = [];
      for (let i = 0; i < 10; i++) {
        await trigger({ url: a.url, name: "e2e-slow", input: { ms: 50 }, workflowId: `mi-a-${i}` });
        await trigger({ url: b.url, name: "e2e-sleeper", input: {}, workflowId: `mi-b-${i}` });
        ids.push(`mi-a-${i}`, `mi-b-${i}`);
      }
      await waitUntil({ what: "every run", check: completed(ids) });

      // One registry: each instance lists both workers.
      for (const host of [a, b]) {
        const body = (await (await fetch(`${host.url}/api/workers`)).json()) as {
          workers: Array<{ workerId: string; status: string }>;
        };
        const online = body.workers.filter((w) => w.status === "online").map((w) => w.workerId);
        expect(online.sort()).toEqual(["on-a", "on-b"]);
      }
    },
    TEST_TIMEOUT_MS,
  );

  it(
    "orphan recovery never takes over a run a live worker is driving",
    async () => {
      const { url } = await startHost({ port: await freePort(), instanceId: "live-host" });
      const driver = await startWorker({ url, workerId: "driver" });
      // A run that outlives the lease, the reclaim window and many sweeps.
      await trigger({ url, name: "e2e-slow", input: { ms: 25_000 }, workflowId: "live-1" });
      await waitUntil({
        what: "the first step",
        check: async () => (await stepResult("live-1", "first")) !== undefined,
      });
      // An idle worker that would happily take the run over.
      await startWorker({ url, workerId: "bystander" });
      await waitUntil({ what: "live-1 to complete", check: completed(["live-1"]) });
      expect((await stepResult("live-1", "first"))?.pid).toBe(driver.pid);
      expect((await stepResult("live-1", "second"))?.pid).toBe(driver.pid);
      const attempts = await storage.loadStepAttempts({ workflowId: "live-1" });
      expect(attempts.filter((a) => a.stepName === "second")).toHaveLength(1);
    },
    TEST_TIMEOUT_MS,
  );
});
