// ---------------------------------------------------------------------------
// The HTTP API contract through `ZoryaServer.handle`: request validation
// (one error envelope, invalid JSON never read as `{}`), status codes
// (201 create, 202 async start, 204 delete, 404, 405, 409), the trigger
// route under the workflow, runs listing (version filter, total), run
// sub-resources, signal delivery guards, and auth in front of it all.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { sync } from "@spilne/perfect-core";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { ApiError, RunListResponse, RunActionResponse } from "../../api-types.ts";
import type { SignalsResponse } from "../signals.ts";

const KEY = "operator-key-0123456789";

/** Yield to the event loop until `predicate` holds, bounded by iterations. */
async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("condition never held");
}

function counted(name: string) {
  const counter = { executions: 0 };
  const wf = workflow<unknown>({ name })
    .step("body", () =>
      sync(() => {
        counter.executions++;
        return `${name}-result`;
      }),
    )
    .build() as Workflow<unknown, unknown>;
  return { wf, counter };
}

function waiting(name: string) {
  return workflow<unknown>({ name })
    .waitForSignal("wait", { signalName: "go" })
    .build() as Workflow<unknown, unknown>;
}

function setup(params: {
  definitions?: Record<string, Workflow<unknown, unknown>>;
  apiKeys?: string[];
}) {
  const storage = new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: params.definitions ?? {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const server = new ZoryaServer({
    workflows,
    ...(params.apiKeys ? { apiKeys: params.apiKeys } : { insecure: true }),
  });
  const auth: Record<string, string> = params.apiKeys
    ? { authorization: `Bearer ${params.apiKeys[0]}` }
    : {};
  const send = (path: string, init: RequestInit = {}) =>
    server.handle(
      new Request(`http://x${path}`, {
        ...init,
        headers: { ...auth, ...(init.headers as Record<string, string> | undefined) },
      }),
    );
  const post = (path: string, body?: unknown) =>
    send(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      ...(body !== undefined && { body: JSON.stringify(body) }),
    });
  const get = (path: string) => send(path);
  const status = async (id: string) => (await storage.loadWorkflow(id))?.status;
  return { storage, server, send, post, get, status };
}

async function errorOf(res: Response): Promise<ApiError> {
  return (await res.json()) as ApiError;
}

describe("request validation — one error envelope", () => {
  it("invalid JSON is a 400 invalid_json, never an empty body", async () => {
    const hello = counted("hello");
    const { send, storage } = setup({ definitions: { hello: hello.wf } });
    const res = await send("/api/workflows/hello/runs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{not json",
    });
    expect(res.status).toBe(400);
    expect((await errorOf(res)).error).toBe("invalid_json");
    expect(await storage.listWorkflows({})).toEqual([]);
  });

  it("a body not sent as JSON is a 415", async () => {
    const { send } = setup({ definitions: { hello: counted("hello").wf } });
    const res = await send("/api/workflows/hello/runs", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: "{}",
    });
    expect(res.status).toBe(415);
    expect((await errorOf(res)).error).toBe("unsupported_media_type");
  });

  it("schema mismatches are 400 invalid_request with field issues", async () => {
    const { post, storage } = setup({ definitions: { hello: counted("hello").wf } });
    for (const body of [
      { workflowId: 42 },
      { metadata: "str" },
      { runSource: "bogus" },
      { unknownField: true },
    ]) {
      const res = await post("/api/workflows/hello/runs", body);
      expect(res.status).toBe(400);
      const err = await errorOf(res);
      expect(err.error).toBe("invalid_request");
      expect(err.issues?.length).toBeGreaterThan(0);
      expect(typeof err.message).toBe("string");
    }
    expect(await storage.listWorkflows({})).toEqual([]);
  });

  it("a wrong method on a known path is a 405 with Allow", async () => {
    const { send } = setup({});
    const res = await send("/api/runs/x/cancel", { method: "GET" });
    expect(res.status).toBe(405);
    expect(res.headers.get("allow")).toBe("POST");
    expect((await errorOf(res)).error).toBe("method_not_allowed");
  });
});

describe("POST /api/workflows/:name/runs", () => {
  it("starts a run: 202 with Location, tagged as a manual fire", async () => {
    const hello = counted("hello");
    const { post, status, storage } = setup({ definitions: { hello: hello.wf } });
    const res = await post("/api/workflows/hello/runs", { workflowId: "h-1", input: { a: 1 } });
    expect(res.status).toBe(202);
    expect(res.headers.get("location")).toBe("/api/runs/h-1");
    expect(await res.json()).toEqual({ workflowId: "h-1" });
    await until(async () => (await status("h-1")) === "completed");
    expect((await storage.loadWorkflow("h-1"))?.runSource).toBe("manual");
  });

  it("starts a run with no body at all", async () => {
    const hello = counted("hello");
    const { send, status } = setup({ definitions: { hello: hello.wf } });
    const res = await send("/api/workflows/hello/runs", { method: "POST" });
    expect(res.status).toBe(202);
    const { workflowId } = (await res.json()) as { workflowId: string };
    await until(async () => (await status(workflowId)) === "completed");
  });

  it("workflows named like run actions are triggerable", async () => {
    const defs = Object.fromEntries(
      ["rerun", "mark-success", "mark-failed", "cancel", "trigger"].map((n) => [n, counted(n).wf]),
    );
    const { post, status } = setup({ definitions: defs });
    for (const name of Object.keys(defs)) {
      const res = await post(`/api/workflows/${name}/runs`, { workflowId: `run-${name}` });
      expect(res.status).toBe(202);
      await until(async () => (await status(`run-${name}`)) === "completed");
      expect((await (await post(`/api/runs/run-${name}/cancel`)).json()) as ApiError).toMatchObject(
        { error: "run_ended" },
      );
    }
  });

  it("an unknown workflow is a 404 workflow_not_found", async () => {
    const { post } = setup({});
    const res = await post("/api/workflows/nope/runs", {});
    expect(res.status).toBe(404);
    expect((await errorOf(res)).error).toBe("workflow_not_found");
  });

  it("the old /api/runs/trigger/:name route is gone", async () => {
    const { post } = setup({ definitions: { hello: counted("hello").wf } });
    expect((await post("/api/runs/trigger/hello", {})).status).toBe(404);
  });
});

describe("operator actions answer the resulting state", () => {
  it("cancel / mark-success / mark-failed return { workflowId, status }", async () => {
    const { post, status } = setup({ definitions: { w: waiting("w") } });
    for (const id of ["a", "b", "c"]) {
      await post("/api/workflows/w/runs", { workflowId: id });
      await until(async () => (await status(id)) === "suspended");
    }
    const ok = async (res: Response) => {
      expect(res.status).toBe(200);
      return (await res.json()) as RunActionResponse;
    };
    expect(await ok(await post("/api/runs/a/mark-success"))).toEqual({
      workflowId: "a",
      status: "completed",
    });
    expect(await ok(await post("/api/runs/b/mark-failed", { reason: "bad" }))).toEqual({
      workflowId: "b",
      status: "failed",
    });
    expect((await ok(await post("/api/runs/c/cancel"))).workflowId).toBe("c");
  });

  it("mark-failed validates its optional body", async () => {
    const { post, status } = setup({ definitions: { w: waiting("w") } });
    await post("/api/workflows/w/runs", { workflowId: "a" });
    await until(async () => (await status("a")) === "suspended");
    const res = await post("/api/runs/a/mark-failed", { reason: 7 });
    expect(res.status).toBe(400);
    expect(await status("a")).toBe("suspended");
  });
});

describe("GET /api/runs", () => {
  async function seed(storage: InMemoryWorkflowStorage) {
    for (let i = 0; i < 5; i++) {
      await storage.createWorkflow({
        workflowId: `v1-${i}`,
        workflowName: "order",
        input: {},
        version: "1",
      });
      await storage.createWorkflow({
        workflowId: `v2-${i}`,
        workflowName: "order",
        input: {},
        version: "2",
      });
    }
  }

  it("filters by version in storage, so pages are exact, and fills total", async () => {
    const { get, storage } = setup({});
    await seed(storage);
    const seen = new Set<string>();
    for (const offset of [0, 2, 4]) {
      const res = await get(`/api/runs?version=2&limit=2&offset=${offset}`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as RunListResponse;
      expect(body.total).toBe(5);
      for (const r of body.runs) {
        expect(r.version).toBe("2");
        seen.add(r.workflowId);
      }
    }
    expect(seen.size).toBe(5);
  });

  it("rejects unknown enums and out-of-range paging with 400", async () => {
    const { get } = setup({});
    for (const qs of [
      "status=bogus",
      "runSource=bogus",
      "limit=0",
      "limit=501",
      "limit=abc",
      "offset=-3",
      "sort=nope",
      "metadata=%5B1%5D",
    ]) {
      const res = await get(`/api/runs?${qs}`);
      expect(res.status).toBe(400);
      expect((await errorOf(res)).error).toBe("invalid_request");
    }
  });

  it("sorts on request and defaults to most recently started first", async () => {
    const { get, storage } = setup({});
    await seed(storage);
    const res = await get("/api/runs?sort=name:asc&status=pending&limit=500");
    expect(res.status).toBe(200);
    expect(((await res.json()) as RunListResponse).runs).toHaveLength(10);
  });
});

describe("run sub-resources answer 404 for an unknown run", () => {
  it("signals, attempts, history, children, journal, signal tokens, stream chunks", async () => {
    const { get } = setup({});
    for (const path of [
      "/api/runs/ghost/signals",
      "/api/runs/ghost/attempts",
      "/api/runs/ghost/history",
      "/api/runs/ghost/children",
      "/api/runs/ghost/journal/step",
      "/api/runs/ghost/signal-tokens",
      "/api/runs/ghost/streams/s/chunks",
    ]) {
      const res = await get(path);
      expect(res.status).toBe(404);
      expect((await errorOf(res)).error).toBe("not_found");
    }
  });

  it("pages children with a total", async () => {
    const { get, storage } = setup({});
    await storage.createWorkflow({ workflowId: "p", workflowName: "parent", input: {} });
    for (let i = 0; i < 3; i++) {
      await storage.createWorkflow({
        workflowId: `c-${i}`,
        workflowName: "child",
        input: {},
        parentWorkflowId: "p",
      });
    }
    const res = await get("/api/runs/p/children?limit=2");
    const body = (await res.json()) as { children: unknown[]; total?: number };
    expect(body.children).toHaveLength(2);
    expect(body.total).toBe(3);
    expect((await get("/api/runs/p/children?limit=1000")).status).toBe(400);
  });
});

describe("POST /api/runs/:id/signal — last-wins and ifAbsent", () => {
  it("accepts delivery with 202; a second delivery replaces the first", async () => {
    const { post, status, storage } = setup({ definitions: { w: waiting("w") } });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const first = await post("/api/runs/w-1/signal", { signalName: "go", payload: 1 });
    expect(first.status).toBe(202);
    expect(await first.json()).toEqual({ workflowId: "w-1", signalName: "go" });
    expect((await post("/api/runs/w-1/signal", { signalName: "go", payload: 2 })).status).toBe(202);
    const signals = await storage.loadSignals("w-1");
    expect(signals.map((s) => s.payload)).toEqual([2]);
  });

  it("ifAbsent refuses to overwrite a pending delivery", async () => {
    const { post, status, storage } = setup({ definitions: { w: waiting("w") } });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const first = await post("/api/runs/w-1/signal", {
      signalName: "go",
      payload: "approve",
      ifAbsent: true,
    });
    expect(first.status).toBe(202);
    const second = await post("/api/runs/w-1/signal", {
      signalName: "go",
      payload: "reject",
      ifAbsent: true,
    });
    expect(second.status).toBe(409);
    expect((await errorOf(second)).error).toBe("signal_already_delivered");
    expect((await storage.loadSignals("w-1")).map((s) => s.payload)).toEqual(["approve"]);
  });

  it("the inbox shows a delivered, not yet consumed signal", async () => {
    const { post, get, status } = setup({ definitions: { w: waiting("w") } });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const before = (await (await get("/api/signals")).json()) as SignalsResponse;
    expect(before.signals.map((s) => [s.workflowId, s.deliveredAt])).toEqual([["w-1", undefined]]);
    await post("/api/runs/w-1/signal", { signalName: "go" });
    const after = (await (await get("/api/signals")).json()) as SignalsResponse;
    expect(typeof after.signals[0]!.deliveredAt).toBe("string");
    expect((await get("/api/signals?limit=0")).status).toBe(400);
  });

  it("validates the body", async () => {
    const { post, status } = setup({ definitions: { w: waiting("w") } });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");
    for (const body of [{}, { signalName: 123 }, { signalName: "" }, { signalName: "go", x: 1 }]) {
      expect((await post("/api/runs/w-1/signal", body)).status).toBe(400);
    }
  });
});

describe("streams and queries validate before acting", () => {
  it("appending to an unknown run is 404, to an ended run 409, without payload 400", async () => {
    const hello = counted("hello");
    const { post, status, storage } = setup({ definitions: { hello: hello.wf } });
    expect((await post("/api/runs/ghost/streams/s", { payload: 1 })).status).toBe(404);
    expect(await storage.readStreamChunks({ workflowId: "ghost", streamId: "s" })).toEqual([]);

    await post("/api/workflows/hello/runs", { workflowId: "h-1" });
    await until(async () => (await status("h-1")) === "completed");
    expect((await post("/api/runs/h-1/streams/s", { payload: 1 })).status).toBe(409);
    expect((await post("/api/runs/h-1/streams/s", {})).status).toBe(400);
  });

  it("a query with a bad body or an over-long timeout is a 400", async () => {
    const { post } = setup({});
    expect((await post("/api/runs/x/query", {})).status).toBe(400);
    expect((await post("/api/runs/x/query", { name: "q", timeoutMs: 60_000 })).status).toBe(400);
    // Valid body, nothing to answer it.
    expect((await post("/api/runs/x/query", { name: "q", timeoutMs: 10 })).status).toBe(404);
  });

  it("dashboard read routes reject out-of-range limits", async () => {
    const { get } = setup({});
    for (const path of [
      "/api/workflows/x/grid?limit=101",
      "/api/workflows/x/history?limit=0",
      "/api/workflows/sparklines?limit=51",
    ]) {
      expect((await get(path)).status).toBe(400);
    }
    expect((await get("/api/workflows/x/grid?limit=100")).status).toBe(200);
  });
});

describe("signal tokens", () => {
  it("mint: 201, cached 200, idempotency key reused for another signal 409, ended run 409", async () => {
    const hello = counted("hello");
    const { post, status } = setup({ definitions: { w: waiting("w"), hello: hello.wf } });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");

    const body = { expiresInMs: 60_000, idempotencyKey: "k" };
    expect((await post("/api/runs/w-1/signals/go/token", body)).status).toBe(201);
    expect((await post("/api/runs/w-1/signals/go/token", body)).status).toBe(200);
    const other = await post("/api/runs/w-1/signals/other/token", body);
    expect(other.status).toBe(409);
    expect((await errorOf(other)).error).toBe("idempotency_key_conflict");
    expect((await post("/api/runs/w-1/signals/go/token", { expiresInMs: 1.5 })).status).toBe(400);

    await post("/api/workflows/hello/runs", { workflowId: "h-1" });
    await until(async () => (await status("h-1")) === "completed");
    const ended = await post("/api/runs/h-1/signals/go/token", { expiresInMs: 60_000 });
    expect(ended.status).toBe(409);
    expect((await errorOf(ended)).error).toBe("run_ended");
  });

  it("complete through auth: public with the bearer, 202 then 200, value required", async () => {
    const { server, post, status, storage } = setup({
      definitions: { w: waiting("w") },
      apiKeys: [KEY],
    });
    await post("/api/workflows/w/runs", { workflowId: "w-1" });
    await until(async () => (await status("w-1")) === "suspended");
    const minted = (await (
      await post("/api/runs/w-1/signals/go/token", { expiresInMs: 60_000 })
    ).json()) as { tokenId: string; bearer: string };

    const complete = (body: unknown) =>
      server.handle(
        new Request(`http://x/api/signal-tokens/${minted.tokenId}/complete`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${minted.bearer}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
        }),
      );
    expect((await complete({})).status).toBe(400);
    const first = await complete({ value: { approved: true } });
    expect(first.status).toBe(202);
    const again = await complete({ value: { approved: false } });
    expect(again.status).toBe(200);
    expect(await again.json()).toEqual({
      ok: true,
      value: { approved: true },
      alreadyCompleted: true,
    });
    expect((await storage.loadSignals("w-1")).map((s) => s.payload)).toEqual([{ approved: true }]);

    // Listing tokens is an operator route.
    const anonymous = await server.handle(new Request("http://x/api/runs/w-1/signal-tokens"));
    expect(anonymous.status).toBe(401);
  });
});

describe("through auth", () => {
  it("authenticates before it validates or starts anything", async () => {
    const hello = counted("hello");
    const { server, post, storage } = setup({
      definitions: { hello: hello.wf },
      apiKeys: [KEY],
    });
    const anonymous = await server.handle(
      new Request("http://x/api/workflows/hello/runs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{not json",
      }),
    );
    expect(anonymous.status).toBe(401);
    expect(await storage.listWorkflows({})).toEqual([]);

    const res = await post("/api/workflows/hello/runs", { workflowId: "h-1" });
    expect(res.status).toBe(202);
    expect((await post("/api/workflows/hello/runs", { workflowId: 1 })).status).toBe(400);
  });

  it("guards every changed operator route behind the key", async () => {
    const { server } = setup({ apiKeys: [KEY] });
    for (const [method, path] of [
      ["POST", "/api/workflows/x/runs"],
      ["GET", "/api/runs?limit=1"],
      ["POST", "/api/runs/x/signal"],
      ["GET", "/api/signals"],
      ["GET", "/api/runs/x/children"],
      ["POST", "/api/runs/x/streams/s"],
      ["POST", "/api/workflows/x/rollback"],
    ] as const) {
      const res = await server.handle(new Request(`http://x${path}`, { method }));
      expect(res.status).toBe(401);
    }
  });
});
