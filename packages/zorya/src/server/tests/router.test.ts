// ---------------------------------------------------------------------------
// Router crash barrier, URL decoding, the API-path static fallback and the
// shared parse helpers — exercised through `ZoryaServer.handle`.
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows } from "../services/workflows/local-workflows.ts";
import { ZoryaServer, type Logger } from "../server.ts";
import { HttpError, Router, isoOrNull, parseIntParam } from "../router.ts";

class RecordingLogger implements Logger {
  readonly errors: unknown[][] = [];
  log(): void {}
  warn(): void {}
  error(message: string, ...args: unknown[]): void {
    this.errors.push([message, ...args]);
  }
}

/** Storage whose reads fail like a dropped database connection. */
class BrokenStorage extends InMemoryWorkflowStorage {
  override async loadWorkflow(): Promise<never> {
    throw new Error("ECONNRESET /secret/path/db.sock");
  }
}

function makeServer(params: {
  storage?: InMemoryWorkflowStorage;
  logger?: Logger;
  uiDir?: string;
}): ZoryaServer {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  return new ZoryaServer({
    insecure: true,
    workflows,
    ...(params.logger !== undefined && { logger: params.logger }),
    ...(params.uiDir !== undefined && { uiDir: params.uiDir }),
  });
}

describe("Router crash barrier", () => {
  it("answers a malformed %-escape in a path param with a JSON 400", async () => {
    const server = makeServer({});
    const res = await server.handle(new Request("http://x/api/runs/%E0%A4%A"));
    expect(res.status).toBe(400);
    expect(res.headers.get("content-type")).toBe("application/json");
    expect(await res.json()).toEqual({ error: "bad_path", message: "Malformed URL encoding" });
  });

  it("maps a throwing handler to a JSON 500 with a request id and no internals", async () => {
    const logger = new RecordingLogger();
    const server = makeServer({ storage: new BrokenStorage(), logger });
    const res = await server.handle(new Request("http://x/api/runs/wf-1"));
    expect(res.status).toBe(500);
    expect(res.headers.get("content-type")).toBe("application/json");
    const text = await res.text();
    expect(text).not.toContain("ECONNRESET");
    expect(text).not.toContain("/secret/path");
    expect(text).not.toContain("at ");
    const body = JSON.parse(text) as { error: string; requestId: string };
    expect(body.error).toBe("internal_error");
    expect(body.requestId).toMatch(/^[0-9a-f-]{36}$/);
    // The detail is logged under the same request id.
    expect(logger.errors).toHaveLength(1);
    expect(String(logger.errors[0]![0])).toContain(body.requestId);
    expect(logger.errors[0]![1]).toBeInstanceOf(Error);
  });

  it("maps HttpError to its status and code", async () => {
    const router = new Router({ logger: new RecordingLogger() }).get("/teapot", () => {
      throw new HttpError({ status: 418, code: "teapot", message: "short and stout" });
    });
    const res = await router.handle(new Request("http://x/teapot"));
    expect(res.status).toBe(418);
    expect(await res.json()).toEqual({ error: "teapot", message: "short and stout" });
  });

  it("survives a logger that throws", async () => {
    const router = new Router({
      logger: {
        error: () => {
          throw new Error("logger down");
        },
      },
    }).get("/boom", () => {
      throw new Error("boom");
    });
    const res = await router.handle(new Request("http://x/boom"));
    expect(res.status).toBe(500);
  });

  it("returns a JSON 404 for unknown routes", async () => {
    const server = makeServer({});
    const res = await server.handle(new Request("http://x/api/nope"));
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "not_found" });
  });
});

describe("static fallback with uiDir", () => {
  let dir: string;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "zorya-ui-"));
    writeFileSync(join(dir, "index.html"), "<!doctype html><title>zorya</title>");
  });
  afterAll(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("never serves the SPA for unknown /api, /rpc or /webhooks paths", async () => {
    const server = makeServer({ uiDir: dir });
    for (const path of ["/api/advertisements", "/api/nope/deeper", "/rpc/worker", "/webhooks/x"]) {
      const res = await server.handle(new Request(`http://x${path}`, { method: "GET" }));
      expect(res.status).toBe(404);
      expect(res.headers.get("content-type")).toBe("application/json");
      expect(await res.json()).toEqual({ error: "not_found" });
    }
  });

  it("still serves index.html for client-side routes", async () => {
    const server = makeServer({ uiDir: dir });
    const res = await server.handle(new Request("http://x/runs/wf-1"));
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("<title>zorya</title>");
  });
});

describe("method mismatch and validation issues", () => {
  it("answers 405 with Allow when only the method is wrong", async () => {
    const router = new Router({ logger: new RecordingLogger() })
      .get("/things/:id", () => Response.json({}))
      .delete("/things/:id", () => new Response(null, { status: 204 }));
    const res = await router.handle(new Request("http://x/things/1", { method: "PUT" }));
    expect(res.status).toBe(405);
    expect(res.headers.get("allow")).toBe("GET, DELETE");
    expect(((await res.json()) as { error: string }).error).toBe("method_not_allowed");
    expect((await router.handle(new Request("http://x/nothing"))).status).toBe(404);
  });

  it("returns an HttpError's issues in the envelope", async () => {
    const router = new Router({ logger: new RecordingLogger() }).post("/v", () => {
      throw new HttpError({
        status: 400,
        code: "invalid_request",
        message: "n: Expected number",
        issues: [{ path: "n", message: "Expected number" }],
      });
    });
    const res = await router.handle(new Request("http://x/v", { method: "POST" }));
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({
      error: "invalid_request",
      message: "n: Expected number",
      issues: [{ path: "n", message: "Expected number" }],
    });
  });
});

describe("parse helpers", () => {
  it("parseIntParam clamps to its bounds and ignores garbage", () => {
    expect(parseIntParam({ raw: null, min: 1, max: 10 })).toBeUndefined();
    expect(parseIntParam({ raw: "", min: 1, max: 10 })).toBeUndefined();
    expect(parseIntParam({ raw: "abc", min: 1, max: 10 })).toBeUndefined();
    expect(parseIntParam({ raw: "5", min: 1, max: 10 })).toBe(5);
    expect(parseIntParam({ raw: "-3", min: 1, max: 10 })).toBe(1);
    expect(parseIntParam({ raw: "1e400", min: 1, max: 10 })).toBe(1);
    expect(parseIntParam({ raw: "99999999999999999999", min: 1, max: 10 })).toBe(10);
  });

  it("isoOrNull tolerates missing and invalid dates", () => {
    expect(isoOrNull(null)).toBeNull();
    expect(isoOrNull(undefined)).toBeNull();
    expect(isoOrNull(new Date(Number.NaN))).toBeNull();
    expect(isoOrNull(new Date(0))).toBe("1970-01-01T00:00:00.000Z");
  });
});
