// ---------------------------------------------------------------------------
// The dashboard client against the server's error envelope and the moved
// trigger route.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import { ApiError, api } from "../client.ts";

const realFetch = globalThis.fetch;
const realStorage = (globalThis as { localStorage?: unknown }).localStorage;

function stub(handler: (req: Request) => Response): Request[] {
  const seen: Request[] = [];
  (globalThis as { localStorage?: unknown }).localStorage = { getItem: () => null };
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const req = new Request(new URL(String(input), "http://ui.test"), init);
    seen.push(req);
    return handler(req);
  }) as typeof fetch;
  return seen;
}

afterEach(() => {
  globalThis.fetch = realFetch;
  (globalThis as { localStorage?: unknown }).localStorage = realStorage;
});

describe("ApiError", () => {
  it("parses the error envelope into code, detail and issues", () => {
    const err = new ApiError(
      400,
      JSON.stringify({
        error: "invalid_request",
        message: "workflowId: Expected string",
        issues: [{ path: "workflowId", message: "Expected string" }],
      }),
    );
    expect(err.code).toBe("invalid_request");
    expect(err.detail).toBe("workflowId: Expected string");
    expect(err.issues).toEqual([{ path: "workflowId", message: "Expected string" }]);
    expect(err.message).toBe("HTTP 400: workflowId: Expected string");
  });

  it("keeps a non-JSON body as the message", () => {
    const err = new ApiError(502, "Bad Gateway");
    expect(err.code).toBeUndefined();
    expect(err.message).toBe("HTTP 502: Bad Gateway");
  });
});

describe("api", () => {
  it("triggers through POST /api/workflows/:name/runs and reads the 202 body", async () => {
    const seen = stub(() => Response.json({ workflowId: "r-1" }, { status: 202 }));
    const res = await api.triggerWorkflow("mark-success", { input: { a: 1 } });
    expect(res).toEqual({ workflowId: "r-1" });
    expect(seen[0]!.method).toBe("POST");
    expect(new URL(seen[0]!.url).pathname).toBe("/api/workflows/mark-success/runs");
  });

  it("sends inbox decisions with ifAbsent and surfaces a 409 as an ApiError code", async () => {
    const seen = stub(() =>
      Response.json({ error: "signal_already_delivered", message: "taken" }, { status: 409 }),
    );
    const failure = await api.sendSignal("w-1", "go", { approved: true }).catch((e) => e);
    expect(failure).toBeInstanceOf(ApiError);
    expect((failure as ApiError).code).toBe("signal_already_delivered");
    expect(await seen[0]!.json()).toEqual({
      signalName: "go",
      payload: { approved: true },
      ifAbsent: true,
    });
  });

  it("resolves a 204 delete to undefined", async () => {
    stub(() => new Response(null, { status: 204 }));
    expect(await api.deleteSchedule("s1")).toBeUndefined();
  });
});

describe("req retries", () => {
  it("retries a GET through a 503 and a network failure, then reads the answer", async () => {
    const answers: Array<() => Response> = [
      () => new Response("restarting", { status: 503 }),
      () => {
        throw new TypeError("Failed to fetch");
      },
      () => Response.json({ ok: true }),
    ];
    const seen = stub(() => answers.shift()!());
    expect(await api.getHealth()).toEqual({ ok: true });
    expect(seen.map((r) => r.method)).toEqual(["GET", "GET", "GET"]);
  });

  it("gives up on a GET after three attempts with the last answer", async () => {
    const seen = stub(() => new Response("Bad Gateway", { status: 502 }));
    const failure = await api.getHealth().catch((e) => e);
    expect(failure).toBeInstanceOf(ApiError);
    expect((failure as ApiError).status).toBe(502);
    expect(seen).toHaveLength(3);
  });

  it("does not retry a GET answered with a non-gateway error", async () => {
    const seen = stub(() => new Response("nope", { status: 500 }));
    expect(await api.getHealth().catch((e) => (e as ApiError).status)).toBe(500);
    expect(seen).toHaveLength(1);
  });

  it("never replays a mutation", async () => {
    const seen = stub(() => new Response("restarting", { status: 503 }));
    const failure = await api.triggerWorkflow("wf", { input: {} }).catch((e) => e);
    expect((failure as ApiError).status).toBe(503);
    expect(seen).toHaveLength(1);
  });
});
