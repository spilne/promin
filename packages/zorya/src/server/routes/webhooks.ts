// ---------------------------------------------------------------------------
// Webhook ingress — generic per-source verifier + agent dispatch.
//
// Route shape:
//   POST /webhooks/:source/:agentId?namespaceId=...&resourceId=...
//
// Per-source verifiers (Slack / GitHub / Stripe / custom) live on the
// host's WebhooksConfig — each declares a secret + signature scheme so
// the framework rejects spoofed events before they reach an agent.
//
// Phase 1 cut (this file): one signature scheme — `sha256-hex` (GitHub
// pattern: `X-Hub-Signature-256: sha256=<hmac>`). Stripe-style
// timestamp+multi-version signatures and Slack's v0 timestamp prefix
// are tracked as Phase 2 follow-ups; the framework leaves room for
// new schemes via the discriminator on `WebhookSourceConfig.scheme`.
//
// Replay protection: in-memory bounded cache keyed by (source,
// deliveryId). A real multi-replica deployment wants a shared dedup
// store — that's Phase 2. For single-process this is fine and stops
// duplicate webhooks during retry storms.
// ---------------------------------------------------------------------------

import { createHmac, timingSafeEqual } from "node:crypto";
import type { AgentRegistry, RegisteredAgent } from "@promin/agent";
import { json, jsonError } from "../router.ts";
import type { NamespaceService } from "../services/namespaces.ts";
import { resolveRequiredNamespaceId } from "./namespace-validation.ts";

export type WebhookSignatureScheme = "sha256-hex";

export interface WebhookSourceConfig {
  /** Shared secret for HMAC verification (raw bytes or utf8 string). */
  readonly secret: string;
  /** Header to read the signature from. Default: 'X-Hub-Signature-256'. */
  readonly signatureHeader?: string;
  /** Header carrying a unique delivery id for replay dedup. Default: 'X-Delivery-Id'. */
  readonly deliveryIdHeader?: string;
  /** Signature scheme. Phase 1: only 'sha256-hex'. */
  readonly scheme?: WebhookSignatureScheme;
}

export interface WebhookGatewayDeps {
  readonly registry: AgentRegistry;
  /**
   * Resolve a recipe to a live Agent. Same shape as AgentGatewayDeps.resolve.
   * The webhook route doesn't go through the agent gateway directly — it
   * hits the same resolver-then-invoke pattern.
   */
  readonly resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Promise<{
    invoke: (input: { task: string }) => Promise<{ text: Promise<string> }>;
    withScope: (s: { namespaceId: string; resourceId?: string }) => {
      invoke: (input: { task: string }) => Promise<{ text: Promise<string> }>;
    };
  }>;
  /** Per-source verifier configs, keyed by URL `:source` segment. */
  readonly sources: Readonly<Record<string, WebhookSourceConfig>>;
  readonly namespaces?: NamespaceService;
  /**
   * Optional dedup store override. Default: in-memory bounded LRU
   * keyed by `${source}:${deliveryId}`. Multi-replica deployments
   * should pass a shared store (Postgres, Redis); this is Phase 2.
   */
  readonly dedup?: WebhookDedupStore;
}

export interface WebhookDedupStore {
  /** Returns true when this id has already been seen (don't re-dispatch). */
  has(key: string): Promise<boolean>;
  mark(key: string): Promise<void>;
}

/**
 * Default in-memory dedup with bounded LRU (5000 entries, ~5min lifetime
 * at 1 webhook/sec). Multi-replica deployments must override.
 */
export class InMemoryWebhookDedupStore implements WebhookDedupStore {
  private readonly seen = new Map<string, number>();
  private readonly maxEntries: number;
  constructor(config: { maxEntries?: number } = {}) {
    this.maxEntries = config.maxEntries ?? 5_000;
  }
  async has(key: string): Promise<boolean> {
    return this.seen.has(key);
  }
  async mark(key: string): Promise<void> {
    if (this.seen.size >= this.maxEntries) {
      // Naive eviction: drop the oldest entry. LRU would be better but
      // not worth the per-write cost for Phase 1.
      const oldest = this.seen.keys().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.set(key, Date.now());
  }
}

export interface WebhookAcceptedResponse {
  readonly accepted: true;
  readonly source: string;
  readonly agentId: string;
}

export interface WebhookReplayResponse {
  readonly accepted: true;
  readonly replay: true;
  readonly source: string;
  readonly deliveryId: string;
}

export function ingestWebhook(deps: WebhookGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const source = params.source;
    const agentId = params.agentId;
    if (!source) return jsonError(400, "missing_source");
    if (!agentId) return jsonError(400, "missing_agentId");

    const sourceConfig = deps.sources[source];
    if (!sourceConfig) {
      return jsonError(404, "unknown_source", `No webhook verifier registered for "${source}".`);
    }

    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId");
    if (!namespaceId) return jsonError(400, "missing_namespaceId");
    const resourceId = url.searchParams.get("resourceId") ?? undefined;

    // Signature verification — read raw body first; the HMAC is over
    // the bytes received, NOT the parsed JSON. JSON re-serialization
    // would whitespace-shift the input and invalidate the signature.
    const rawBody = await req.text();
    const sigHeader = sourceConfig.signatureHeader ?? "X-Hub-Signature-256";
    const provided = req.headers.get(sigHeader);
    if (!provided) {
      return jsonError(401, "missing_signature", `Header "${sigHeader}" is required.`);
    }
    if (!verifySha256Hex(sourceConfig.secret, rawBody, provided)) {
      return jsonError(401, "signature_mismatch");
    }

    // Replay dedup — agnostic to source's signature scheme. When
    // delivery-id header is set, idempotently accept retries.
    const deliveryHeader = sourceConfig.deliveryIdHeader ?? "X-Delivery-Id";
    const deliveryId = req.headers.get(deliveryHeader);
    const dedup = deps.dedup ?? defaultDedup;
    if (deliveryId) {
      const dedupKey = `${source}:${deliveryId}`;
      if (await dedup.has(dedupKey)) {
        const replayResp: WebhookReplayResponse = {
          accepted: true,
          replay: true,
          source,
          deliveryId,
        };
        return json(200, replayResp);
      }
      await dedup.mark(dedupKey);
    }

    // Body parse — best-effort JSON; webhooks that ship form-encoded
    // or other shapes will need per-source parse handlers (Phase 2).
    let payload: unknown;
    try {
      payload = rawBody.length > 0 ? JSON.parse(rawBody) : null;
    } catch {
      return jsonError(422, "invalid_json", "Webhook body is not valid JSON.");
    }

    // Resolve agent + invoke. Task framing is intentionally minimal —
    // the agent's system prompt should pin "you receive webhook events
    // and act on them"; the structured payload is appended to the task
    // so the LLM has the data without per-source schema awareness.
    const recipe = await deps.registry.get(agentId);
    if (!recipe) {
      return jsonError(404, "agent_not_found", `Agent "${agentId}" is not registered.`);
    }

    const namespace = await resolveRequiredNamespaceId(deps.namespaces, namespaceId);
    if ("response" in namespace) return namespace.response;

    try {
      const resolved = await deps.resolve(recipe, {
        namespaceId: namespace.namespaceId,
        ...(resourceId !== undefined && { resourceId }),
      });
      const scoped = resolved.withScope({
        namespaceId: namespace.namespaceId,
        ...(resourceId !== undefined && { resourceId }),
      });
      const task = formatWebhookTask(source, payload);
      // Fire-and-await the invoke so any throw lands in our error path.
      // Webhook clients typically don't wait for the agent's text reply
      // (that would block their retry timer), so we just confirm dispatch.
      const out = await scoped.invoke({ task });
      // Drain the text promise in the background — prevents an unhandled
      // rejection later if the agent loop fails after this route returned.
      out.text.catch(() => {});
    } catch (err) {
      return jsonError(500, "dispatch_failed", err instanceof Error ? err.message : String(err));
    }

    const resp: WebhookAcceptedResponse = { accepted: true, source, agentId };
    return json(200, resp);
  };
}

const defaultDedup = new InMemoryWebhookDedupStore();

function verifySha256Hex(secret: string, rawBody: string, provided: string): boolean {
  // GitHub format: "sha256=<hex>". Strip the prefix when present.
  const hex = provided.startsWith("sha256=") ? provided.slice(7) : provided;
  const expected = createHmac("sha256", secret).update(rawBody).digest("hex");
  // timingSafeEqual requires equal-length buffers; a length mismatch is
  // sufficient evidence to reject, no need to expose timing on the prefix.
  if (hex.length !== expected.length) return false;
  try {
    return timingSafeEqual(Buffer.from(hex, "hex"), Buffer.from(expected, "hex"));
  } catch {
    return false;
  }
}

function formatWebhookTask(source: string, payload: unknown): string {
  // Compact JSON — agents don't need pretty formatting and it eats
  // tokens. Truncate at a reasonable cap so a runaway payload doesn't
  // blow the context window before the agent even starts reasoning.
  const json = JSON.stringify(payload);
  const cap = 8_000;
  const body = json.length > cap ? `${json.slice(0, cap)}…[truncated]` : json;
  return `Incoming webhook from ${source}:\n${body}`;
}
