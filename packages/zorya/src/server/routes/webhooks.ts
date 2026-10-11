// ---------------------------------------------------------------------------
// Webhook ingress — signed delivery → durable dedup → agent dispatch.
//
// Route shape:
//   POST /webhooks/:source/:agentId?namespaceId=...&resourceId=...
//
// Signature (`zorya-v1`):
//   X-Zorya-Timestamp: <unix ms>
//   X-Zorya-Delivery:  <unique delivery id>
//   X-Zorya-Signature: v1=<hex HMAC-SHA256(secret, signing payload)>
//
//   The signing payload binds everything that decides where and when the
//   event lands — `v1`, timestamp, delivery id, source, agent id,
//   namespace and resource, one per line — followed by the raw body
//   bytes. A captured delivery therefore can't be replayed against another
//   namespace, resource or agent, nor re-sent after the tolerance window
//   (default 5 min, judged on the server's WallClock). `signWebhookDelivery`
//   builds the header for senders. A source may also pin `namespaceId` /
//   `resourceId` in its config; a request naming a different one is
//   rejected.
//
// Dedup and durability:
//   Deliveries are claimed in a `WebhookDeliveryStore` keyed by
//   (source, agent, namespace, resource, delivery id) — by default the
//   server's WorkflowStorage, so it survives restarts and is shared by
//   every instance on the same backend. The claim records the delivery's
//   scope and task before the agent runs. The agent turn runs under a run
//   id derived from the delivery (and its attempt), and the delivery is
//   marked done only after the turn finished. A dispatcher that dies
//   mid-turn leaves the delivery unfinished: `WebhookDeliveryRecovery` (or
//   the sender's retry) claims it once the dead claim lapses and runs it
//   again under the same run id, so the turn continues from its journal —
//   never dropped, never run twice. A failed dispatch releases the claim,
//   so the sender's retry is processed (as the next attempt) instead of
//   answered as a replay.
//
// Status codes:
//   200 accepted (or `replay: true` for an already-processed delivery)
//   400 malformed request / scope        401 signature, timestamp
//   404 unknown source or agent          409 same delivery in flight
//   410 agent disabled                   422 body is not JSON
//   500 dispatch failed (retryable; body carries only a request id)
// ---------------------------------------------------------------------------

import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { AgentRegistry, RegisteredAgent } from "@promin/agent";
import {
  SystemWallClock,
  type FenceToken,
  type NamespaceRegistry,
  type TimerHandle,
  type WallClock,
  type WorkflowStorage,
} from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { internalError } from "./agent-gateway-kit.ts";

export type WebhookSignatureScheme = "zorya-v1";

export interface WebhookSourceConfig {
  /** Shared HMAC secret. */
  readonly secret: string;
  /** Pin every delivery of this source to one namespace. */
  readonly namespaceId?: string;
  /** Pin every delivery of this source to one resource. */
  readonly resourceId?: string;
  /** Signature header. Default: `X-Zorya-Signature`. */
  readonly signatureHeader?: string;
  /** Timestamp header (unix ms). Default: `X-Zorya-Timestamp`. */
  readonly timestampHeader?: string;
  /** Delivery id header. Default: `X-Zorya-Delivery`. */
  readonly deliveryIdHeader?: string;
  /** Accepted clock skew / replay window in ms. Default: 5 minutes. */
  readonly toleranceMs?: number;
}

const DEFAULT_SIGNATURE_HEADER = "X-Zorya-Signature";
const DEFAULT_TIMESTAMP_HEADER = "X-Zorya-Timestamp";
const DEFAULT_DELIVERY_HEADER = "X-Zorya-Delivery";
const DEFAULT_TOLERANCE_MS = 5 * 60 * 1_000;
const MAX_DELIVERY_ID_LENGTH = 200;

/** Where and when a delivery lands — everything the signature binds. */
export interface WebhookDeliveryScope {
  readonly source: string;
  readonly agentId: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly deliveryId: string;
  /** Unix ms. */
  readonly timestamp: number;
}

/** The exact bytes `zorya-v1` signs: a scope header, then the raw body. */
export function webhookSigningPayload(params: {
  readonly scope: WebhookDeliveryScope;
  readonly body: Uint8Array;
}): Uint8Array {
  const { scope, body } = params;
  const header = new TextEncoder().encode(
    [
      "v1",
      String(scope.timestamp),
      scope.deliveryId,
      scope.source,
      scope.agentId,
      scope.namespaceId,
      scope.resourceId ?? "",
      "",
    ].join("\n"),
  );
  const out = new Uint8Array(header.length + body.length);
  out.set(header, 0);
  out.set(body, header.length);
  return out;
}

/** The `X-Zorya-Signature` header value for a delivery. */
export function signWebhookDelivery(params: {
  readonly secret: string;
  readonly scope: WebhookDeliveryScope;
  readonly body: Uint8Array | string;
}): string {
  const body =
    typeof params.body === "string" ? new TextEncoder().encode(params.body) : params.body;
  const mac = createHmac("sha256", params.secret)
    .update(webhookSigningPayload({ scope: params.scope, body }))
    .digest("hex");
  return `v1=${mac}`;
}

// ---------------------------------------------------------------------------
// Delivery store
// ---------------------------------------------------------------------------

/** A recorded delivery: where it lands and the task it hands the agent. */
export interface WebhookDelivery {
  readonly key: string;
  readonly scope: WebhookDeliveryScope;
  readonly task: string;
}

/** Outcome of claiming a delivery. */
export type WebhookClaim =
  | {
      readonly outcome: "claimed";
      readonly token?: FenceToken;
      /**
       * Dispatch attempt of this delivery, from 1. A claim that takes over
       * an interrupted dispatch keeps its attempt (the agent run continues
       * from its journal); one after a failed dispatch starts the next.
       */
      readonly attempt: number;
    }
  | { readonly outcome: "duplicate" }
  | { readonly outcome: "in_flight" };

/**
 * Exactly-once bookkeeping for webhook deliveries. `claim` is atomic: of
 * concurrent claims for one key, at most one gets `claimed`. The claim
 * records the delivery (scope and task) durably before the agent runs, and
 * expires after `leaseMs` unless renewed, so a dispatcher that dies
 * mid-run leaves a delivery that `listUnfinished` reports and a later
 * claim (the sender's retry, or `WebhookDeliveryRecovery`) takes over.
 */
export interface WebhookDeliveryStore {
  claim(params: {
    readonly delivery: WebhookDelivery;
    readonly leaseMs: number;
  }): Promise<WebhookClaim>;
  /** Extend a held claim by `leaseMs` from now. */
  renew(params: {
    readonly key: string;
    readonly token?: FenceToken;
    readonly leaseMs: number;
  }): Promise<void>;
  /** The delivery was processed: later claims answer `duplicate`. */
  complete(params: { readonly key: string; readonly token?: FenceToken }): Promise<void>;
  /** The dispatch failed: drop the claim so a retry can claim again. */
  fail(params: {
    readonly key: string;
    readonly token?: FenceToken;
    readonly error: string;
  }): Promise<void>;
  /**
   * Deliveries claimed but neither completed nor failed — dispatches in
   * flight, or interrupted by a crash. Claiming one answers `in_flight`
   * while its dispatcher still holds the claim.
   */
  listUnfinished(params: {
    readonly limit: number;
    /** Skip this many (paging). Default 0. */
    readonly offset?: number;
  }): Promise<WebhookDelivery[]>;
}

/** Workflow name of the run rows `WorkflowStorageWebhookDeliveryStore` keeps. */
export const WEBHOOK_DELIVERY_WORKFLOW = "zorya.webhook-delivery";

/** What a delivery run row stores as its input. */
interface DeliveryRowInput {
  readonly key: string;
  readonly source: string;
  readonly agentId: string;
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly deliveryId: string;
  readonly timestamp: number;
  readonly task: string;
  readonly attempt: number;
}

/**
 * Durable delivery store on the server's `WorkflowStorage`: each delivery
 * is a run row (`zorya.webhook-delivery`, run source `webhook`) holding the
 * delivery's scope and task, and the claim is that run's lock. The lock's
 * expiry is decided by the storage backend, so every instance sharing the
 * backend agrees on it; a completed row answers `duplicate` across
 * restarts and instances; a failed one is restarted by the next claim.
 */
export class WorkflowStorageWebhookDeliveryStore implements WebhookDeliveryStore {
  private readonly storage: WorkflowStorage;

  constructor(config: { readonly storage: WorkflowStorage }) {
    this.storage = config.storage;
  }

  async claim(params: { delivery: WebhookDelivery; leaseMs: number }): Promise<WebhookClaim> {
    const { delivery } = params;
    const workflowId = deliveryWorkflowId(delivery.key);
    const locked = await this.storage.tryLockAndLoad({
      workflowId,
      lockDurationMs: params.leaseMs,
    });
    if (!locked.locked) return { outcome: "in_flight" };
    const guard = locked.token !== undefined ? { fenceToken: locked.token } : undefined;
    const release = async (): Promise<WebhookClaim> => {
      await this.storage.releaseLock({ workflowId, ...(guard && { guard }) });
      return { outcome: "duplicate" };
    };
    let state = locked.state;
    let attempt = 1;
    if (!state) {
      const input: DeliveryRowInput = {
        key: delivery.key,
        source: delivery.scope.source,
        agentId: delivery.scope.agentId,
        namespaceId: delivery.scope.namespaceId,
        ...(delivery.scope.resourceId !== undefined && { resourceId: delivery.scope.resourceId }),
        deliveryId: delivery.scope.deliveryId,
        timestamp: delivery.scope.timestamp,
        task: delivery.task,
        attempt,
      };
      const created = await this.storage.createWorkflow({
        workflowId,
        workflowName: WEBHOOK_DELIVERY_WORKFLOW,
        input,
        runSource: "webhook",
        runSourceId: delivery.scope.source,
      });
      if (!created.created) state = created.existing;
    }
    if (state) {
      if (state.status === "completed") return release();
      const stored = state.input as DeliveryRowInput;
      // A failed dispatch starts the next attempt; one whose dispatcher
      // died mid-flight (its lock lapsed) continues the same attempt.
      attempt = state.status === "failed" ? stored.attempt + 1 : stored.attempt;
      await this.storage.startFreshRun({
        workflowId,
        ...(attempt !== stored.attempt && { nextInput: { value: { ...stored, attempt } } }),
        ...(guard && { guard }),
      });
    }
    // Park the row as suspended while the dispatch runs: no workflow
    // definition backs it, so orphan recovery (which adopts pending /
    // running rows) must leave it alone; `listUnfinished` finds it.
    await this.storage.suspendWorkflow({
      workflowId,
      stepName: "dispatch",
      stepUpdate: { status: "running" },
      ...(guard && { guard }),
    });
    return {
      outcome: "claimed",
      attempt,
      ...(locked.token !== undefined && { token: locked.token }),
    };
  }

  async renew(params: { key: string; token?: FenceToken; leaseMs: number }): Promise<void> {
    await this.storage.heartbeat({
      workflowId: deliveryWorkflowId(params.key),
      lockDurationMs: params.leaseMs,
      ...(params.token !== undefined && { guard: { fenceToken: params.token } }),
    });
  }

  async complete(params: { key: string; token?: FenceToken }): Promise<void> {
    const workflowId = deliveryWorkflowId(params.key);
    const guard = params.token !== undefined ? { guard: { fenceToken: params.token } } : {};
    await this.storage.completeWorkflow({ workflowId, result: { dispatched: true }, ...guard });
    await this.storage.releaseLock({ workflowId, ...guard });
  }

  async fail(params: { key: string; token?: FenceToken; error: string }): Promise<void> {
    const workflowId = deliveryWorkflowId(params.key);
    const guard = params.token !== undefined ? { guard: { fenceToken: params.token } } : {};
    await this.storage.failWorkflow({ workflowId, error: params.error, ...guard });
    await this.storage.releaseLock({ workflowId, ...guard });
  }

  async listUnfinished(params: { limit: number; offset?: number }): Promise<WebhookDelivery[]> {
    const rows = await this.storage.listWorkflows({
      status: "suspended",
      name: WEBHOOK_DELIVERY_WORKFLOW,
      limit: params.limit,
      ...(params.offset !== undefined && { offset: params.offset }),
      orderBy: "createdAt",
      orderDir: "asc",
    });
    return rows.map((row) => {
      const input = row.input as DeliveryRowInput;
      return {
        key: input.key,
        task: input.task,
        scope: {
          source: input.source,
          agentId: input.agentId,
          namespaceId: input.namespaceId,
          ...(input.resourceId !== undefined && { resourceId: input.resourceId }),
          deliveryId: input.deliveryId,
          timestamp: input.timestamp,
        },
      };
    });
  }
}

function deliveryWorkflowId(key: string): string {
  return `webhook-delivery:${createHash("sha256").update(key).digest("hex").slice(0, 40)}`;
}

/**
 * Run id of a delivery's agent turn: one per (delivery, attempt). A
 * dispatch that takes over an interrupted one lands on the same run, which
 * continues from its journal (a finished run answers from storage), so a
 * crash never runs the model or a tool twice for one delivery.
 */
export function webhookRunId(params: { readonly key: string; readonly attempt: number }): string {
  const digest = createHash("sha256")
    .update(JSON.stringify([params.key, params.attempt]))
    .digest("hex");
  return `webhook-run-${digest.slice(0, 40)}`;
}

/**
 * Single-process delivery store. Claims and the processed-delivery memory
 * live on the injected clock; processed deliveries are remembered for
 * `retainMs` (default 7 days, bounded by `maxEntries`). Not shared across
 * instances or restarts — use `WorkflowStorageWebhookDeliveryStore` there.
 */
export class InMemoryWebhookDeliveryStore implements WebhookDeliveryStore {
  private readonly clock: WallClock;
  private readonly retainMs: number;
  private readonly maxEntries: number;
  private readonly rows = new Map<
    string,
    {
      readonly state: "in_flight" | "done" | "failed";
      readonly expiresAt: number;
      readonly delivery: WebhookDelivery;
      readonly attempt: number;
    }
  >();

  constructor(
    config: {
      readonly clock?: WallClock;
      readonly retainMs?: number;
      readonly maxEntries?: number;
    } = {},
  ) {
    this.clock = config.clock ?? SystemWallClock;
    this.retainMs = config.retainMs ?? 7 * 24 * 60 * 60 * 1_000;
    this.maxEntries = config.maxEntries ?? 50_000;
  }

  async claim(params: { delivery: WebhookDelivery; leaseMs: number }): Promise<WebhookClaim> {
    const { delivery } = params;
    const now = this.clock.currentTimeMs();
    const row = this.rows.get(delivery.key);
    if (row && row.state !== "failed" && row.expiresAt > now) {
      return row.state === "done" ? { outcome: "duplicate" } : { outcome: "in_flight" };
    }
    // A lapsed in-flight claim continues its attempt; a failed one starts
    // the next; anything else (none, or a forgotten done) starts afresh.
    const attempt =
      row?.state === "in_flight" ? row.attempt : row?.state === "failed" ? row.attempt + 1 : 1;
    this.set(delivery.key, {
      state: "in_flight",
      expiresAt: now + params.leaseMs,
      delivery: row?.delivery ?? delivery,
      attempt,
    });
    return { outcome: "claimed", attempt };
  }

  async renew(params: { key: string; leaseMs: number }): Promise<void> {
    const row = this.rows.get(params.key);
    if (row?.state === "in_flight") {
      this.set(params.key, { ...row, expiresAt: this.clock.currentTimeMs() + params.leaseMs });
    }
  }

  async complete(params: { key: string }): Promise<void> {
    const row = this.rows.get(params.key);
    if (!row) return;
    this.set(params.key, {
      ...row,
      state: "done",
      expiresAt: this.clock.currentTimeMs() + this.retainMs,
    });
  }

  async fail(params: { key: string }): Promise<void> {
    const row = this.rows.get(params.key);
    if (!row) return;
    this.set(params.key, {
      ...row,
      state: "failed",
      expiresAt: this.clock.currentTimeMs() + this.retainMs,
    });
  }

  async listUnfinished(params: { limit: number; offset?: number }): Promise<WebhookDelivery[]> {
    const offset = params.offset ?? 0;
    return [...this.rows.values()]
      .filter((r) => r.state === "in_flight")
      .slice(offset, offset + params.limit)
      .map((r) => r.delivery);
  }

  private set(
    key: string,
    row: {
      state: "in_flight" | "done" | "failed";
      expiresAt: number;
      delivery: WebhookDelivery;
      attempt: number;
    },
  ): void {
    this.rows.delete(key);
    if (this.rows.size >= this.maxEntries) {
      const now = this.clock.currentTimeMs();
      for (const [k, r] of this.rows) {
        if (r.state !== "in_flight" && r.expiresAt <= now) this.rows.delete(k);
      }
      if (this.rows.size >= this.maxEntries) {
        const oldest = this.rows.keys().next().value;
        if (oldest !== undefined) this.rows.delete(oldest);
      }
    }
    this.rows.set(key, row);
  }
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

/** The agent surface the webhook dispatch uses. */
interface WebhookAgent {
  invoke: (
    input: { task: string },
    opts?: { runId?: string },
  ) => Promise<{ text: Promise<string> }>;
}

export interface WebhookGatewayDeps {
  readonly registry: AgentRegistry;
  /**
   * Resolve a recipe to a live Agent. Same shape as AgentGatewayDeps.resolve,
   * narrowed to the methods the webhook dispatch uses.
   */
  readonly resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Promise<
    WebhookAgent & {
      withScope: (s: { namespaceId: string; resourceId?: string }) => WebhookAgent;
    }
  >;
  /** Per-source verifier configs, keyed by URL `:source` segment. */
  readonly sources: Readonly<Record<string, WebhookSourceConfig>>;
  /** Delivery dedup store. The server defaults it to its WorkflowStorage. */
  readonly deliveries: WebhookDeliveryStore;
  /** Time source for the replay window and claim renewal. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /** How long a dispatch may hold its claim between renewals. Default: 2 minutes. */
  readonly claimLeaseMs?: number;
  /**
   * When set, a delivery's namespace must be registered and active (404
   * `namespace_not_found`, 409 `namespace_archived`), checked before the
   * delivery is claimed so the sender's retry runs once it is.
   */
  readonly namespaces?: NamespaceRegistry;
}

const DEFAULT_CLAIM_LEASE_MS = 2 * 60 * 1_000;

/**
 * The delivery's agent turn is still being run by another dispatcher (its
 * delivery claim lapsed, the turn's own lock did not): leave the delivery
 * unfinished, so a later claim picks it up once that dispatcher is done
 * or gone.
 */
export class WebhookDispatchBusyError extends Error {
  readonly _tag = "WebhookDispatchBusyError";
  constructor() {
    super("The delivery's agent turn is still running elsewhere.");
  }
}

/**
 * Run a claimed delivery: hand its task to the agent under the delivery's
 * deterministic run id, wait for the turn to finish, then mark the
 * delivery done — or failed, rethrowing, when the dispatch fails. The
 * claim is renewed while the turn runs. When the turn is still locked by
 * another dispatcher, the delivery stays unfinished
 * (`WebhookDispatchBusyError`).
 */
async function dispatchDelivery(params: {
  readonly deps: WebhookGatewayDeps;
  readonly delivery: WebhookDelivery;
  readonly claim: Extract<WebhookClaim, { outcome: "claimed" }>;
}): Promise<void> {
  const { deps, delivery, claim } = params;
  const clock = deps.clock ?? SystemWallClock;
  const leaseMs = deps.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS;
  const { key, scope } = delivery;
  const token = claim.token;
  const renewal = clock.setInterval(
    () => {
      deps.deliveries
        .renew({ key, leaseMs, ...(token !== undefined && { token }) })
        .catch(() => {});
    },
    Math.max(1, Math.floor(leaseMs / 2)),
  );
  renewal.unref?.();
  try {
    const recipe = await deps.registry.get(scope.agentId);
    if (!recipe) throw new Error(`agent "${scope.agentId}" is not registered`);
    const resolved = await deps.resolve(recipe, {
      namespaceId: scope.namespaceId,
      ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
    });
    const agent = resolved.withScope({
      namespaceId: scope.namespaceId,
      ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
    });
    const out = await agent.invoke(
      { task: delivery.task },
      { runId: webhookRunId({ key, attempt: claim.attempt }) },
    );
    // The delivery is done when the turn is, whatever the backend's
    // `invoke` resolves on.
    await out.text;
  } catch (err) {
    renewal.clear();
    if ((err as { _tag?: unknown } | null)?._tag === "WorkflowLockError") {
      // Not a failure: the turn runs elsewhere. Our claim lapses on its own.
      throw new WebhookDispatchBusyError();
    }
    await deps.deliveries
      .fail({
        key,
        error: err instanceof Error ? err.message : String(err),
        ...(token !== undefined && { token }),
      })
      .catch(() => {});
    throw err;
  }
  renewal.clear();
  try {
    await deps.deliveries.complete({ key, ...(token !== undefined && { token }) });
  } catch (err) {
    // The agent already ran; a later claim of this delivery lands on the
    // same finished run, which answers from storage without running again.
    console.error("[zorya] webhook delivery could not be marked done", err);
  }
}

/**
 * Finishes deliveries whose dispatcher died mid-run: every tick lists the
 * unfinished deliveries and claims each — a claim succeeds only once the
 * dead dispatcher's claim has lapsed — then dispatches it again under the
 * same run id, so the agent turn continues from its journal instead of
 * starting over. The server runs one; it ticks at start and then every
 * `intervalMs` (default 60 s).
 */
export class WebhookDeliveryRecovery {
  private readonly deps: WebhookGatewayDeps;
  private readonly intervalMs: number;
  private readonly batch: number;
  private readonly onError: (err: unknown) => void;
  private timer: TimerHandle | null = null;
  private running = false;

  constructor(
    config: WebhookGatewayDeps & {
      readonly intervalMs?: number;
      /** Deliveries examined per tick. Default 50. */
      readonly batch?: number;
      readonly onError?: (err: unknown) => void;
    },
  ) {
    this.deps = config;
    this.intervalMs = config.intervalMs ?? 60_000;
    this.batch = config.batch ?? 50;
    this.onError =
      config.onError ?? ((err) => console.error("[zorya] webhook delivery recovery failed", err));
  }

  start(): void {
    if (this.timer) return;
    const clock = this.deps.clock ?? SystemWallClock;
    void this.tick();
    this.timer = clock.setInterval(() => void this.tick(), this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    this.timer?.clear();
    this.timer = null;
  }

  /** One pass; overlapping ticks are skipped. Resolves when its dispatches end. */
  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const leaseMs = this.deps.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS;
      // Page through every unfinished delivery: live ones (still claimed by
      // their dispatcher) stay in the list, so a lapsed one may sit beyond
      // the first page.
      const lapsed: Array<{
        delivery: WebhookDelivery;
        claim: Extract<WebhookClaim, { outcome: "claimed" }>;
      }> = [];
      for (let offset = 0; ; offset += this.batch) {
        const page = await this.deps.deliveries.listUnfinished({ limit: this.batch, offset });
        for (const delivery of page) {
          const claim = await this.deps.deliveries.claim({ delivery, leaseMs });
          if (claim.outcome === "claimed") lapsed.push({ delivery, claim });
        }
        if (page.length < this.batch) break;
      }
      await Promise.all(
        lapsed.map(({ delivery, claim }) =>
          dispatchDelivery({ deps: this.deps, delivery, claim }).catch((err: unknown) => {
            if (!(err instanceof WebhookDispatchBusyError)) this.onError(err);
          }),
        ),
      );
    } catch (err) {
      this.onError(err);
    } finally {
      this.running = false;
    }
  }
}

export interface WebhookAcceptedResponse {
  readonly accepted: true;
  readonly source: string;
  readonly agentId: string;
  readonly deliveryId: string;
}

export interface WebhookReplayResponse {
  readonly accepted: true;
  readonly replay: true;
  readonly source: string;
  readonly deliveryId: string;
}

export function ingestWebhook(deps: WebhookGatewayDeps) {
  const clock = deps.clock ?? SystemWallClock;
  const leaseMs = deps.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS;
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const source = params.source;
    const agentId = params.agentId;
    if (!source) return jsonError(400, "missing_source");
    if (!agentId) return jsonError(400, "missing_agentId");

    const sourceConfig = Object.hasOwn(deps.sources, source) ? deps.sources[source] : undefined;
    if (!sourceConfig) {
      return jsonError(404, "unknown_source", `No webhook verifier registered for "${source}".`);
    }

    const url = new URL(req.url);
    const scope = resolveDeliveryScope({ url, config: sourceConfig });
    if ("error" in scope) return jsonError(400, scope.error);

    const timestampRaw = req.headers.get(sourceConfig.timestampHeader ?? DEFAULT_TIMESTAMP_HEADER);
    const deliveryId = req.headers.get(sourceConfig.deliveryIdHeader ?? DEFAULT_DELIVERY_HEADER);
    const signature = req.headers.get(sourceConfig.signatureHeader ?? DEFAULT_SIGNATURE_HEADER);
    if (!signature) return jsonError(401, "missing_signature");
    if (!timestampRaw || !/^\d{1,16}$/.test(timestampRaw)) {
      return jsonError(400, "invalid_timestamp", "A unix-ms timestamp header is required.");
    }
    if (!deliveryId || deliveryId.length > MAX_DELIVERY_ID_LENGTH) {
      return jsonError(400, "invalid_delivery_id", "A delivery id header is required.");
    }

    // HMAC over the bytes received — never a decoded / re-encoded string.
    const body = new Uint8Array(await req.arrayBuffer());
    const delivery: WebhookDeliveryScope = {
      source,
      agentId,
      namespaceId: scope.namespaceId,
      ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
      deliveryId,
      timestamp: Number(timestampRaw),
    };
    const expected = signWebhookDelivery({ secret: sourceConfig.secret, scope: delivery, body });
    if (!constantTimeEqual(signature, expected)) return jsonError(401, "signature_mismatch");
    const tolerance = sourceConfig.toleranceMs ?? DEFAULT_TOLERANCE_MS;
    if (Math.abs(clock.currentTimeMs() - delivery.timestamp) > tolerance) {
      return jsonError(401, "timestamp_out_of_window");
    }

    let payload: unknown;
    try {
      payload = body.length > 0 ? JSON.parse(new TextDecoder().decode(body)) : null;
    } catch {
      return jsonError(422, "invalid_json", "Webhook body is not valid JSON.");
    }

    // Unknown / disabled agents are answered before claiming, so the
    // sender's retry after the agent is registered or re-enabled runs.
    const recipe = await deps.registry.get(agentId);
    if (!recipe) {
      return jsonError(404, "agent_not_found", `Agent "${agentId}" is not registered.`);
    }
    if (recipe.metadata.enabled === false) {
      return jsonError(410, "agent_disabled", `Agent "${agentId}" is disabled.`);
    }
    await requireActiveNamespace({ registry: deps.namespaces, namespace: delivery.namespaceId });

    const key = JSON.stringify([
      source,
      agentId,
      delivery.namespaceId,
      delivery.resourceId ?? null,
      deliveryId,
    ]);
    // The claim records the delivery — scope and task — before the agent
    // runs, so a crash mid-run leaves it for recovery instead of losing it.
    const recorded: WebhookDelivery = {
      key,
      scope: delivery,
      task: formatWebhookTask(source, payload),
    };
    const claim = await deps.deliveries.claim({ delivery: recorded, leaseMs });
    if (claim.outcome === "duplicate") {
      return json(200, {
        accepted: true,
        replay: true,
        source,
        deliveryId,
      } satisfies WebhookReplayResponse);
    }
    if (claim.outcome === "in_flight") {
      return jsonError(409, "delivery_in_progress", "This delivery is being processed.");
    }

    try {
      await dispatchDelivery({ deps, delivery: recorded, claim });
    } catch (err) {
      if (err instanceof WebhookDispatchBusyError) {
        return jsonError(409, "delivery_in_progress", "This delivery is being processed.");
      }
      return internalError({ code: "dispatch_failed", err });
    }
    return json(200, {
      accepted: true,
      source,
      agentId,
      deliveryId,
    } satisfies WebhookAcceptedResponse);
  };
}

/** Namespace / resource from the source pin or the query string. */
function resolveDeliveryScope(params: {
  url: URL;
  config: WebhookSourceConfig;
}): { namespaceId: string; resourceId?: string } | { error: string } {
  const { url, config } = params;
  const queryNs = url.searchParams.get("namespaceId") || undefined;
  const queryRes = url.searchParams.get("resourceId") || undefined;
  if (config.namespaceId !== undefined && queryNs !== undefined && queryNs !== config.namespaceId) {
    return { error: "namespace_mismatch" };
  }
  if (config.resourceId !== undefined && queryRes !== undefined && queryRes !== config.resourceId) {
    return { error: "resource_mismatch" };
  }
  const namespaceId = config.namespaceId ?? queryNs;
  if (!namespaceId) return { error: "missing_namespaceId" };
  const resourceId = config.resourceId ?? queryRes;
  return { namespaceId, ...(resourceId !== undefined && { resourceId }) };
}

function constantTimeEqual(provided: string, expected: string): boolean {
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(expected, "utf8");
  // A length mismatch is enough to reject; it reveals nothing about the MAC.
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
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
