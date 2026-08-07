// ---------------------------------------------------------------------------
// webhookTrigger — framework-agnostic HTTP handler that starts a workflow.
//
// Returns a Web Fetch API-compatible handler: `(Request) => Promise<Response>`.
// Works directly with Bun.serve, Deno.serve, Hono, Cloudflare Workers, etc.
// Express users wrap it (see README/examples).
//
// Design goals:
// - Framework-agnostic (standard Request/Response; no Express coupling)
// - Idempotent by caller-chosen workflowId (duplicate → 409)
// - Optional HMAC validation (sha256 hex, common webhook shape) OR a full
//   `verify` escape hatch for Stripe/custom formats
// - Fire-and-forget by default (202); `wait: true` for sync-until-complete (200)
// ---------------------------------------------------------------------------

import type { Workflow } from "./durable-pipeline.ts";
import type { WorkflowRunner } from "./workflow-runner.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Parsed view of a webhook request passed to `workflowId`/`input`/`verify`. */
export interface WebhookRequest {
  /** Original Request object — for headers/url access when the defaults aren't enough. */
  readonly raw: Request;
  readonly headers: Headers;
  /** JSON-parsed body. `null` if body couldn't be parsed as JSON. */
  readonly body: unknown;
  /** Raw body text — used for signature validation where every byte matters. */
  readonly rawBody: string;
}

export interface WebhookHmacConfig {
  /** Shared secret. For multi-tenant use a function that resolves per-request. */
  readonly secret: string | ((req: WebhookRequest) => string | Promise<string>);
  /** Header carrying the signature. Default: `"x-signature-256"`. */
  readonly header?: string;
  /** HMAC algorithm. Default: `"sha256"`. */
  readonly algorithm?: "sha256" | "sha1";
  /**
   * Optional prefix to strip from the header value before comparing, e.g.
   * GitHub's `x-hub-signature-256: sha256=<hex>` → set `prefix: "sha256="`.
   */
  readonly prefix?: string;
}

export interface WebhookTriggerConfig<Input, Output> {
  /** Workflow to start when a valid webhook arrives. */
  readonly workflow: Workflow<Input, Output>;
  /** Runner that drives `runner.run({ workflow, ... })` when a request arrives. */
  readonly runner: WorkflowRunner;
  /**
   * Storage used for dedup lookup (is this workflowId already present?).
   * Typically the same storage the runner is configured with.
   */
  readonly storage: WorkflowStorage;
  /**
   * Extract a deterministic workflowId from the request. Enables idempotency:
   * re-posting the same event (same id) returns 409 instead of starting twice.
   * Typically derived from the payload (`req.body.orderId`, header ids, etc.).
   */
  readonly workflowId: (req: WebhookRequest) => string | Promise<string>;
  /** Extract workflow input from the request. */
  readonly input: (req: WebhookRequest) => Input | Promise<Input>;
  /**
   * Built-in HMAC validation for the common `sha256(rawBody, secret)` webhook
   * shape. For Stripe/custom formats with timestamps, use `verify` instead.
   */
  readonly hmac?: WebhookHmacConfig;
  /**
   * Custom signature verification — mutually exclusive with `hmac`. Full
   * control; return true to accept, false to reject with 401. Receives the
   * raw body so any payload-based validation works (Stripe's `t=…,v1=…`
   * scheme, nonce checks, etc.).
   */
  readonly verify?: (req: WebhookRequest, rawBody: string) => boolean | Promise<boolean>;
  /**
   * Wait for the workflow to complete before responding. Default: false.
   * - false → return 202 immediately with `{ workflowId }` (fire-and-forget)
   * - true  → await result; 200 with `{ workflowId, result }` or 500 on failure
   */
  readonly wait?: boolean;
  /** Override the generic 500 response for internal errors. */
  readonly onError?: (error: unknown) => Response;
}

export type WebhookHandler = (request: Request) => Promise<Response>;

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Create a webhook handler that starts a workflow when POSTed to.
 *
 * @example Bun
 * ```ts
 * Bun.serve({
 *   port: 3000,
 *   fetch: webhookTrigger({
 *     workflow: orderWorkflow,
 *     workflowId: (req) => req.body.orderId,
 *     input: (req) => req.body,
 *     hmac: { secret: process.env.WEBHOOK_SECRET!, prefix: "sha256=" },
 *   }),
 * });
 * ```
 *
 * @example Hono
 * ```ts
 * app.post("/webhook/orders", (c) => handler(c.req.raw));
 * ```
 *
 * @example Custom (Stripe) verification
 * ```ts
 * webhookTrigger({
 *   workflow,
 *   workflowId,
 *   input,
 *   verify: async (req, raw) => {
 *     const sig = req.headers.get("stripe-signature");
 *     return stripe.webhooks.constructEvent(raw, sig, SECRET) !== null;
 *   },
 * });
 * ```
 */
export function webhookTrigger<Input, Output>(
  config: WebhookTriggerConfig<Input, Output>,
): WebhookHandler {
  if (config.hmac && config.verify) {
    throw new Error("webhookTrigger: specify either `hmac` OR `verify`, not both");
  }

  return async (request: Request): Promise<Response> => {
    try {
      // ---- Parse ----
      const rawBody = await request.text();
      let body: unknown = null;
      if (rawBody.length > 0) {
        try {
          body = JSON.parse(rawBody);
        } catch {
          // Invalid JSON is fatal — the workflowId/input extractors expect JSON.
          return jsonResponse(400, { error: "invalid JSON body" });
        }
      }
      const webhookReq: WebhookRequest = {
        raw: request,
        headers: request.headers,
        body,
        rawBody,
      };

      // ---- Verify signature ----
      if (config.hmac) {
        const ok = await verifyHmac(webhookReq, config.hmac);
        if (!ok) return jsonResponse(401, { error: "invalid signature" });
      } else if (config.verify) {
        const ok = await config.verify(webhookReq, rawBody);
        if (!ok) return jsonResponse(401, { error: "invalid signature" });
      }

      // ---- Extract id + input ----
      const workflowId = await config.workflowId(webhookReq);
      const input = await config.input(webhookReq);

      // ---- Dedup check ----
      const existing = await config.storage.loadWorkflow(workflowId);
      if (existing) {
        return jsonResponse(409, { error: "duplicate workflowId", workflowId });
      }

      // ---- Start ----
      if (config.wait) {
        const result = await config.runner.run({ workflow: config.workflow, workflowId, input });
        return jsonResponse(200, { workflowId, result });
      }

      // Fire-and-forget: kick off the run, swallow rejections at the edge so
      // we don't trigger unhandled-rejection warnings. The workflow's own
      // error handling (retries, DLQ, hooks) still applies.
      void config.runner.run({ workflow: config.workflow, workflowId, input }).catch(() => {});
      return jsonResponse(202, { workflowId });
    } catch (error) {
      if (config.onError) return config.onError(error);
      return jsonResponse(500, {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };
}

// ---------------------------------------------------------------------------
// HMAC
// ---------------------------------------------------------------------------

async function verifyHmac(req: WebhookRequest, hmac: WebhookHmacConfig): Promise<boolean> {
  const header = hmac.header ?? "x-signature-256";
  const algorithm = hmac.algorithm ?? "sha256";
  const provided = req.headers.get(header);
  if (!provided) return false;

  const cleaned =
    hmac.prefix && provided.startsWith(hmac.prefix) ? provided.slice(hmac.prefix.length) : provided;

  const secret = typeof hmac.secret === "function" ? await hmac.secret(req) : hmac.secret;
  const expected = await computeHmac(algorithm, secret, req.rawBody);
  return constantTimeEquals(cleaned, expected);
}

async function computeHmac(
  algorithm: "sha256" | "sha1",
  secret: string,
  message: string,
): Promise<string> {
  const algoName = algorithm === "sha256" ? "SHA-256" : "SHA-1";
  const keyData = new TextEncoder().encode(secret);
  const msgData = new TextEncoder().encode(message);
  const key = await crypto.subtle.importKey(
    "raw",
    keyData,
    { name: "HMAC", hash: algoName },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, msgData);
  return toHex(new Uint8Array(sig));
}

function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

/** Constant-time comparison so timing side-channels can't leak signature bytes. */
function constantTimeEquals(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return diff === 0;
}

// ---------------------------------------------------------------------------
// Response helper
// ---------------------------------------------------------------------------

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
