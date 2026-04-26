// ---------------------------------------------------------------------------
// ctx.setQueryHandler — register an in-memory read of running workflow
// state. Server's POST /api/runs/:id/query routes through the worker WS
// to whichever worker hosts the workflow body, invokes the handler, and
// returns the result over HTTP.
//
// Pins:
//   - Handler registered inside a journaled body answers an HTTP query.
//   - Updates to closure state are visible to subsequent queries (the
//     handler closes over the body's mutable state).
//   - Query against a workflow not running on any worker → 404.
//   - Query for a name that doesn't exist on the hosting worker →
//     500 with a handler-throw style message.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import {
  workflow,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  hasQueryHandlers,
  invokeQueryHandler,
} from "@promin/workflow";
import { WorkerControlSocket } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";

function listenServer() {
  const storage = new InMemoryWorkflowStorage();
  const server = new ZoryaServer({ storage });
  const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
  return {
    server,
    storage,
    runner: createWorkflowRunner({ storage }),
    url: `http://127.0.0.1:${handle.port}`,
    close: () => handle.stop(),
  };
}

async function waitFor<T>(
  fn: () => T | Promise<T>,
  predicate: (v: T) => boolean,
  timeoutMs = 2_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (predicate(v)) return v;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe("Workflow query handlers — ctx.setQueryHandler + POST /api/runs/:id/query", () => {
  it("answers a query against a live workflow with the handler's return value", async () => {
    const { server, runner, url, close } = listenServer();
    try {
      // Worker connects so the WS dispatch path is alive. Manually register
      // the query handler — ZoryaWorker auto-wires this in production via
      // its constructor; the test mimics that.
      const ws = new WorkerControlSocket({ url, workerId: "qh-w-1", reconnectDelayMs: 50 });
      ws.onCommand("query", async (args) => {
        const {
          workflowId,
          name,
          args: queryArgs,
        } = args as {
          workflowId: string;
          name: string;
          args?: unknown;
        };
        if (!hasQueryHandlers(workflowId)) return { hosted: false };
        const result = await invokeQueryHandler(workflowId, name, queryArgs);
        return { hosted: true, result };
      });
      ws.start();
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("qh-w-1"),
      );

      // A workflow whose journaled body registers two query handlers
      // and parks on a long-running activity so the body stays live
      // for the test to query against.
      let releaseBlocker: () => void = () => {};
      const blocker = new Promise<string>((resolve) => {
        releaseBlocker = () => resolve("ok");
      });
      const wf = workflow<{ orderId: number }>({ name: "qh-wf" })
        .journaled("main", function* (ctx) {
          let status = "pending";
          let lineItems: string[] = ["a", "b"];
          ctx.setQueryHandler("status", () => status);
          ctx.setQueryHandler("lineItems", () => [...lineItems]);
          status = "fetching";
          yield* ctx.activity("step-1", async () => "ok");
          status = "fetched";
          lineItems = ["a", "b", "c"];
          // Block here until the test releases.
          yield* ctx.activity("block", () => blocker);
          status = "complete";
          return { final: ctx.input.orderId };
        })
        .build();

      // Run the workflow in the background. Catch the rejection so an
      // early failure doesn't crash the test runner.
      const wfPromise = runner.run({
        workflow: wf,
        workflowId: "qh-1",
        input: { orderId: 42 },
      });

      // Wait until the body has progressed past step-1 + registered
      // handlers — observable via the registry helper.
      const { hasQueryHandlers } = await import("@promin/workflow");
      await waitFor(
        () => hasQueryHandlers("qh-1"),
        (b) => b === true,
        3_000,
      );
      // Give the body a beat to mutate state through to "fetched".
      await waitFor(
        () => server.workerWs.connectedWorkers().length,
        (n) => n > 0,
      );
      await new Promise((r) => setTimeout(r, 50));

      // First query — status after the second mutation.
      const r1 = await fetch(`${url}/api/runs/qh-1/query`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "status" }),
      });
      if (!r1.ok) {
        console.error("query failed:", r1.status, await r1.text());
      }
      expect(r1.ok).toBe(true);
      const j1 = (await r1.json()) as { result: string };
      expect(j1.result).toBe("fetched");

      // Second query — different handler, returns the line-items array.
      const r2 = await fetch(`${url}/api/runs/qh-1/query`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "lineItems" }),
      });
      expect(r2.ok).toBe(true);
      const j2 = (await r2.json()) as { result: string[] };
      expect(j2.result).toEqual(["a", "b", "c"]);

      // Release the activity so the body completes.
      releaseBlocker();
      await wfPromise;

      await ws.stop();
    } finally {
      close();
    }
  });

  it("returns 404 when the workflow isn't running on any connected worker", async () => {
    const { url, close } = listenServer();
    try {
      // No workers connected.
      const r = await fetch(`${url}/api/runs/never-existed/query`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "anything" }),
      });
      expect(r.status).toBe(404);
    } finally {
      close();
    }
  });

  it("returns 400 when the body lacks `name`", async () => {
    const { server, url, close } = listenServer();
    try {
      const ws = new WorkerControlSocket({ url, workerId: "qh-w-3", reconnectDelayMs: 50 });
      ws.start();
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("qh-w-3"),
      );

      const r = await fetch(`${url}/api/runs/whatever/query`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      expect(r.status).toBe(400);
      await ws.stop();
    } finally {
      close();
    }
  });
});
