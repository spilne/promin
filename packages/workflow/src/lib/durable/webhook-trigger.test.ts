import { describe, it, expect, beforeEach } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { webhookTrigger } from "./webhook-trigger.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface OrderEvent {
  orderId: string;
  total: number;
}

function buildOrderWorkflow(storage: InMemoryWorkflowStorage) {
  return workflow<OrderEvent>({ name: "order-flow", storage })
    .step("load", ({ input }) => Pipeline.succeed(input))
    .step("process", ({ prev }) => Pipeline.succeed(`processed-${prev.orderId}`))
    .build();
}

async function sha256Hex(secret: string, body: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
  const bytes = new Uint8Array(sig);
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

function postJson(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request("http://test.local/webhook", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("webhookTrigger", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  describe("happy path", () => {
    it("starts workflow and returns 202 with workflowId (fire-and-forget default)", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
      });

      const res = await handler(postJson({ orderId: "ord-1", total: 100 }));
      expect(res.status).toBe(202);
      expect(await res.json()).toEqual({ workflowId: "ord-1" });
    });

    it("returns 200 + result when wait: true", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        wait: true,
      });

      const res = await handler(postJson({ orderId: "ord-2", total: 50 }));
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ workflowId: "ord-2", result: "processed-ord-2" });
    });
  });

  describe("idempotency", () => {
    it("returns 409 on duplicate workflowId", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        wait: true,
      });

      await handler(postJson({ orderId: "dup-1", total: 1 }));

      const second = await handler(postJson({ orderId: "dup-1", total: 2 }));
      expect(second.status).toBe(409);
      const body = await second.json();
      expect(body.workflowId).toBe("dup-1");
    });
  });

  describe("HMAC validation", () => {
    const SECRET = "super-secret";

    it("accepts request with valid signature (bare hex)", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        hmac: { secret: SECRET },
      });

      const body = { orderId: "hmac-1", total: 10 };
      const rawBody = JSON.stringify(body);
      const sig = await sha256Hex(SECRET, rawBody);

      const res = await handler(postJson(body, { "x-signature-256": sig }));
      expect(res.status).toBe(202);
    });

    it("accepts GitHub-style prefixed signature via hmac.prefix", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        hmac: { secret: SECRET, header: "x-hub-signature-256", prefix: "sha256=" },
      });

      const body = { orderId: "hmac-2", total: 10 };
      const rawBody = JSON.stringify(body);
      const sig = `sha256=${await sha256Hex(SECRET, rawBody)}`;

      const res = await handler(postJson(body, { "x-hub-signature-256": sig }));
      expect(res.status).toBe(202);
    });

    it("rejects request with bad signature → 401", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        hmac: { secret: SECRET },
      });

      const res = await handler(
        postJson({ orderId: "hmac-bad", total: 10 }, { "x-signature-256": "deadbeef" }),
      );
      expect(res.status).toBe(401);
    });

    it("rejects request with missing signature header → 401", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        hmac: { secret: SECRET },
      });

      const res = await handler(postJson({ orderId: "hmac-missing", total: 10 }));
      expect(res.status).toBe(401);
    });

    it("supports per-request secret resolver (multi-tenant)", async () => {
      const wf = buildOrderWorkflow(storage);
      const secrets: Record<string, string> = { "tenant-a": "sec-a", "tenant-b": "sec-b" };
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        hmac: {
          secret: (req) => secrets[req.headers.get("x-tenant")!]!,
        },
      });

      const body = { orderId: "multi-a", total: 1 };
      const rawBody = JSON.stringify(body);
      const sig = await sha256Hex("sec-a", rawBody);

      const res = await handler(postJson(body, { "x-tenant": "tenant-a", "x-signature-256": sig }));
      expect(res.status).toBe(202);
    });
  });

  describe("custom verify()", () => {
    it("accepts when verify returns true", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        verify: (req) => req.headers.get("x-custom-token") === "expected",
      });

      const res = await handler(
        postJson({ orderId: "verify-ok", total: 1 }, { "x-custom-token": "expected" }),
      );
      expect(res.status).toBe(202);
    });

    it("rejects when verify returns false → 401", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as OrderEvent).orderId,
        input: (r) => r.body as OrderEvent,
        verify: () => false,
      });

      const res = await handler(postJson({ orderId: "verify-no", total: 1 }));
      expect(res.status).toBe(401);
    });

    it("throws at construction when both hmac and verify supplied", () => {
      const wf = buildOrderWorkflow(storage);
      expect(() =>
        webhookTrigger({
          workflow: wf,
          workflowId: () => "x",
          input: () => ({}) as OrderEvent,
          hmac: { secret: "s" },
          verify: () => true,
        }),
      ).toThrow(/either `hmac` OR `verify`/);
    });
  });

  describe("malformed requests", () => {
    it("returns 400 on invalid JSON body", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: (r) => (r.body as any)?.orderId ?? "unknown",
        input: (r) => r.body as OrderEvent,
      });

      const res = await handler(
        new Request("http://test.local/webhook", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{not json",
        }),
      );
      expect(res.status).toBe(400);
    });

    it("returns 500 with onError override when extractor throws", async () => {
      const wf = buildOrderWorkflow(storage);
      const handler = webhookTrigger({
        workflow: wf,
        workflowId: () => {
          throw new Error("missing id");
        },
        input: (r) => r.body as OrderEvent,
        onError: (err) =>
          new Response(JSON.stringify({ custom: String(err) }), {
            status: 422,
            headers: { "content-type": "application/json" },
          }),
      });

      const res = await handler(postJson({ orderId: "x", total: 1 }));
      expect(res.status).toBe(422);
      const body = await res.json();
      expect(body.custom).toContain("missing id");
    });
  });
});
