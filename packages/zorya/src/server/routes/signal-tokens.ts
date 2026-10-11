// ---------------------------------------------------------------------------
// Signal tokens — public-bearer authorization for `deliverSignal`.
//
// Three routes:
//   POST /api/runs/:id/signals/:name/token     — mint (auth-gated, server-side)
//   POST /api/signal-tokens/:tokenId/complete  — public consume (bearer auth)
//   GET  /api/runs/:id/signal-tokens           — dashboard listing
//
// The mint route is reachable by Zorya operators / admins; the complete
// route is the only route a third-party (Stripe webhook, email reviewer
// link, etc.) ever hits — bearer-validated, no Zorya credentials needed.
// Completion goes through `storage.completeSignalToken`, which consumes the
// token and delivers its signal in one atomic write, so the workflow
// resumes through the existing signal mechanic and a failed delivery never
// leaves a consumed token behind.
// ---------------------------------------------------------------------------

import { parseApprovalSignal } from "@promin/agent";
import type { SignalTokenRecord, WorkflowStorage } from "@promin/workflow";
import { SystemWallClock, isTerminalWorkflowStatus, type WallClock } from "@promin/workflow";
import { z } from "zod";
import { isoOrNull, json, jsonError } from "../router.ts";
import { CompleteSignalTokenRequestSchema, IdSchema, NameSchema } from "../api-types.ts";
import { parseBody, parseParams } from "../request-validation.ts";
import { requireRun } from "./run-extras.ts";
import { RunParamsSchema, signalSchemaMismatch } from "./runs.ts";
import { agentApprovalRefusal } from "./signals.ts";

/** Default cap on a minted token's validity window: 90 days. */
export const DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS = 90 * 24 * 60 * 60 * 1000;

export interface SignalTokenRoutesDeps {
  storage: WorkflowStorage;
  /**
   * Optional base URL the mint route prepends to the relative completion
   * path when building the token's `url`. Absent → token returns no URL,
   * caller composes one. Useful for tests that don't care about URLs.
   */
  publicBaseUrl?: string;
  /** Time source for minted expiry and the completion expiry check. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Largest `expiresInMs` a mint may request. Default: `DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS`. */
  maxTtlMs?: number;
}

/** Body of `POST /api/runs/:id/signals/:name/token`, with `expiresInMs` capped at `maxTtlMs`. */
export function mintTokenRequestSchema(params: { maxTtlMs: number }) {
  return z
    .object({
      /** TTL in milliseconds. Required — caps the token's validity window. */
      expiresInMs: z.number().int().min(1).max(params.maxTtlMs),
      /** Free-form labels for dashboard filtering. */
      tags: z.array(z.string().max(256)).max(64).optional(),
      /**
       * Caller-supplied dedup key. Reusing it for the same signal returns
       * the cached token; for another signal it is a 409.
       */
      idempotencyKey: z.string().min(1).max(256).nullable().optional(),
    })
    .strict();
}

export type MintTokenRequest = z.infer<ReturnType<typeof mintTokenRequestSchema>>;

const MintParamsSchema = z.object({ id: IdSchema, name: NameSchema });
const TokenParamsSchema = z.object({ tokenId: IdSchema });

export interface MintTokenResponse {
  tokenId: string;
  url: string | null;
  bearer: string;
  expiresAt: string;
  isCached: boolean;
}

/** Body of the complete route: `{ value }`, delivered as the token's signal payload. */
export type { CompleteSignalTokenRequest as CompleteTokenRequest } from "../api-types.ts";

/**
 * `202` on first delivery (the run resumes when it next consumes the
 * signal), `200` with `alreadyCompleted: true` on a re-submission with
 * the same bearer.
 */
export interface CompleteTokenResponse {
  ok: true;
  value: unknown;
  alreadyCompleted?: true;
}

/** Bearer-authed metadata fetch for the public share page. */
export interface DescribeSignalTokenResponse {
  workflowId: string;
  workflowName: string;
  signalName: string;
  /** True when the signal follows the `approve:<callId>` convention. */
  isApproval: boolean;
  /** Null when the stored expiry is unreadable; such a token is treated as expired. */
  expiresAt: string | null;
  completedAt: string | null;
  /** True when the workflow is still suspended on this signal. */
  pending: boolean;
  /**
   * JSON Schema snapshot the suspend point waited on, from
   * `ctx.validatedSignal` / `ctx.approval`. Undefined for plain
   * `ctx.signal` suspends — the share page falls back to a JSON editor
   * in that case. The schema-driven form renderer keys off presence.
   */
  jsonSchema?: unknown;
}

export interface SignalTokenDto {
  tokenId: string;
  workflowId: string;
  signalName: string;
  tags: string[];
  idempotencyKey: string | null;
  /** Null when the stored date is unreadable. */
  expiresAt: string | null;
  completedAt: string | null;
  createdAt: string | null;
}

/**
 * Mint a public-bearer signal token for `(workflowId, signalName)`.
 * Auth-gated by the operator's auth layer (this route doesn't enforce
 * its own auth — it sits behind whatever protects the rest of `/api`).
 */
export function mintSignalToken(deps: SignalTokenRoutesDeps) {
  const mintSchema = mintTokenRequestSchema({
    maxTtlMs: deps.maxTtlMs ?? DEFAULT_MAX_SIGNAL_TOKEN_TTL_MS,
  });
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id: workflowId, name: signalName } = parseParams({
      params,
      schema: MintParamsSchema,
    });
    const body = await parseBody({ req, schema: mintSchema });
    const ttl = body.expiresInMs;

    // Verify the workflow exists — failing fast at mint time prevents
    // issuing tokens against a typo'd id that no completer would ever match.
    const status = await deps.storage.loadWorkflowStatus(workflowId);
    if (!status) return jsonError(404, "workflow_not_found");
    if (isTerminalWorkflowStatus(status.status)) {
      return jsonError(409, "run_ended", `Run has already ended (status: ${status.status})`);
    }
    const agentApproval = await agentApprovalRefusal({
      storage: deps.storage,
      workflowId,
      signalName,
    });
    if (agentApproval) return agentApproval;

    const tokenId = randomToken(16);
    const bearer = randomToken(32);
    const expiresAt = new Date((deps.clock ?? SystemWallClock).currentTimeMs() + ttl);

    const { record, isCached } = await deps.storage.createSignalToken({
      tokenId,
      workflowId,
      signalName,
      bearer,
      tags: body.tags ?? [],
      idempotencyKey: body.idempotencyKey ?? null,
      expiresAt,
    });
    if (isCached && record.signalName !== signalName) {
      return jsonError(
        409,
        "idempotency_key_conflict",
        `idempotencyKey already minted a token for signal "${record.signalName}"`,
      );
    }

    const response: MintTokenResponse = {
      tokenId: record.tokenId,
      url: deps.publicBaseUrl
        ? `${deps.publicBaseUrl}/api/signal-tokens/${record.tokenId}/complete`
        : null,
      bearer: record.bearer,
      // A cached record minted before validation existed may hold an
      // unreadable expiry; report it as already expired.
      expiresAt: isoOrNull(record.expiresAt) ?? new Date(0).toISOString(),
      isCached,
    };
    return json(isCached ? 200 : 201, response);
  };
}

/**
 * Public completion endpoint — bearer-authenticated. The bearer was issued
 * by `mintSignalToken` and carried through whatever side-channel the
 * caller chose (email, webhook URL, return-from-replicate, …).
 *
 * Status semantics:
 *   202 — first delivery; the run resumes when it next consumes the signal
 *   200 — re-submission with the same bearer; same value returned
 *   400 — body is not `{ value }`, or the value fails the wait's schema
 *   401 — missing or wrong bearer
 *   404 — unknown token id, or its run no longer exists
 *   409 — the run has already ended (`run_ended`), or a concurrent
 *         completion won the token (`token_already_completed`)
 *   410 — token expired before completion (`token_expired`)
 */
export function completeSignalToken(deps: SignalTokenRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { tokenId } = parseParams({ params, schema: TokenParamsSchema });

    const bearer = extractBearer(req);
    if (!bearer) return jsonError(401, "missing_bearer");

    const token = await deps.storage.findSignalTokenById(tokenId);
    if (!token) return jsonError(404, "token_not_found");

    if (!constantTimeEqual(bearer, token.bearer)) {
      return jsonError(401, "invalid_bearer");
    }

    if (token.completedAt) {
      // Idempotent re-submission: same bearer accepted, original value returned.
      const response: CompleteTokenResponse = {
        ok: true,
        value: token.completedValue,
        alreadyCompleted: true,
      };
      return json(200, response);
    }

    const now = (deps.clock ?? SystemWallClock).now();
    if (isExpired({ expiresAt: token.expiresAt, now })) {
      return jsonError(410, "token_expired", "The signal token has expired");
    }

    const body = await parseBody({ req, schema: CompleteSignalTokenRequestSchema });

    // Schema check — when the suspend point was set up via
    // `ctx.validatedSignal` / `ctx.approval`, the StepState carries a
    // JSON Schema snapshot. Reject mismatching payloads here so a webhook
    // or share-link recipient can't smuggle the wrong shape through. Plain
    // `ctx.signal` suspends leave the schema undefined → pass-through.
    const wf = await deps.storage.loadWorkflow(token.workflowId);
    if (!wf) return jsonError(404, "workflow_not_found");
    // An ended run can't consume the signal; leave the token unclaimed.
    if (isTerminalWorkflowStatus(wf.status)) {
      return jsonError(409, "run_ended", `Run has already ended (status: ${wf.status})`);
    }
    const mismatch = signalSchemaMismatch({
      run: wf,
      signalName: token.signalName,
      payload: body.value,
    });
    if (mismatch !== undefined) return jsonError(400, "schema_mismatch", mismatch);

    // Consume the token and deliver the signal in one storage write. From
    // the signal's perspective, the bearer-completer is indistinguishable
    // from an in-app `storage.deliverSignal` caller. A write that throws
    // changed nothing: the token stays completable.
    const claim = await deps.storage.completeSignalToken({
      tokenId,
      value: body.value,
      now,
    });

    if (claim.outcome === "already_completed") {
      // Lost a race with a concurrent completer: this bearer's value was
      // not the one delivered.
      return jsonError(
        409,
        "token_already_completed",
        "A concurrent completion consumed this token first",
      );
    }

    const response: CompleteTokenResponse = { ok: true, value: body.value };
    return json(202, response);
  };
}

/**
 * Bearer-authed metadata fetch — what a public share page needs to render
 * a meaningful "approve / reject this signal" UI without exposing the rest
 * of the dashboard. Surfaces workflow name, signal name, approval-shape
 * discriminator, expiry, and whether the workflow is still suspended on
 * this signal (so an already-resolved share link can show a friendly
 * "already handled" state instead of failing on complete).
 */
export function describeSignalToken(deps: SignalTokenRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { tokenId } = parseParams({ params, schema: TokenParamsSchema });
    const bearer = extractBearer(req);
    if (!bearer) return jsonError(401, "missing_bearer");
    const token = await deps.storage.findSignalTokenById(tokenId);
    if (!token) return jsonError(404, "token_not_found");
    if (!constantTimeEqual(bearer, token.bearer)) return jsonError(401, "invalid_bearer");

    const wf = await deps.storage.loadWorkflow(token.workflowId);
    if (!wf) return jsonError(404, "workflow_not_found");

    // Pending check — the workflow could have resumed via another delivery
    // between mint and now. Surface that explicitly so the share page renders
    // an "already handled" state instead of letting the user click Approve
    // into a workflow that's already finished. Pull the schema snapshot off
    // the same step while we're walking — populates the share form when
    // the suspend used ctx.validatedSignal / ctx.approval.
    let pending = false;
    let jsonSchema: unknown;
    for (const step of Object.values(wf.steps)) {
      if (step.status === "waiting_for_signal" && step.signalName === token.signalName) {
        pending = true;
        jsonSchema = step.signalJsonSchema;
        break;
      }
    }

    const body: DescribeSignalTokenResponse = {
      workflowId: token.workflowId,
      workflowName: wf.workflowName,
      signalName: token.signalName,
      isApproval: parseApprovalSignal(token.signalName) !== null,
      expiresAt: isoOrNull(token.expiresAt),
      completedAt: isoOrNull(token.completedAt),
      pending,
      ...(jsonSchema !== undefined && { jsonSchema }),
    };
    return json(200, body);
  };
}

/**
 * List every signal token issued for one workflow — drives the dashboard
 * waitpoints/approvals view. Bearer is omitted from the response so leaking
 * the dashboard payload doesn't leak a still-valid completion credential.
 */
export function listSignalTokensForRun(deps: SignalTokenRoutesDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id: workflowId } = parseParams({ params, schema: RunParamsSchema });
    await requireRun({ storage: deps.storage, workflowId });
    const records = await deps.storage.listSignalTokensForWorkflow(workflowId);
    return json(200, { tokens: records.map(toDto) });
  };
}

function toDto(record: SignalTokenRecord): SignalTokenDto {
  return {
    tokenId: record.tokenId,
    workflowId: record.workflowId,
    signalName: record.signalName,
    tags: [...record.tags],
    idempotencyKey: record.idempotencyKey,
    expiresAt: isoOrNull(record.expiresAt),
    completedAt: isoOrNull(record.completedAt),
    createdAt: isoOrNull(record.createdAt),
  };
}

/** An unreadable expiry counts as expired, never as "valid forever". */
function isExpired(params: { expiresAt: Date; now: Date }): boolean {
  const t = params.expiresAt.getTime();
  return !Number.isFinite(t) || t <= params.now.getTime();
}

function extractBearer(req: Request): string | null {
  const auth = req.headers.get("authorization");
  if (!auth) return null;
  const m = /^Bearer\s+(.+)$/i.exec(auth.trim());
  return m?.[1] ?? null;
}

/**
 * Constant-time string comparison so a timing attack against the
 * completion endpoint can't probe bearers byte-by-byte. Only meaningful
 * when the strings are the same length; otherwise short-circuits.
 */
function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return diff === 0;
}

/**
 * Cryptographically-random URL-safe token. Uses `crypto.getRandomValues`
 * (web standard, available in Bun + Node 19+).
 */
function randomToken(byteLen: number): string {
  const bytes = new Uint8Array(byteLen);
  crypto.getRandomValues(bytes);
  // base64url encode without padding for clean URLs.
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
