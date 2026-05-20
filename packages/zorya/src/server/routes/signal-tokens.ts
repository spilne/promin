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
// On successful completion the route calls `storage.deliverSignal` so the
// workflow resumes through the existing signal mechanic.
// ---------------------------------------------------------------------------

import { parseApprovalSignal } from "@promin/agent";
import type { SignalTokenRecord, WorkflowStorage } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";

export interface SignalTokenRoutesDeps {
  storage: WorkflowStorage;
  /**
   * Optional base URL the mint route prepends to the relative completion
   * path when building the token's `url`. Absent → token returns no URL,
   * caller composes one. Useful for tests that don't care about URLs.
   */
  publicBaseUrl?: string;
}

export interface MintTokenRequest {
  /** Free-form labels for dashboard filtering. Optional. */
  tags?: string[];
  /** TTL in milliseconds. Required — caps the token's validity window. */
  expiresInMs: number;
  /** Caller-supplied dedup key. Reusing returns the cached token. Optional. */
  idempotencyKey?: string;
}

export interface MintTokenResponse {
  tokenId: string;
  url: string | null;
  bearer: string;
  expiresAt: string;
  isCached: boolean;
}

export interface CompleteTokenRequest {
  /** Payload delivered through `storage.deliverSignal`. JSON-serializable. */
  value: unknown;
}

/** Bearer-authed metadata fetch for the public share page. */
export interface DescribeSignalTokenResponse {
  workflowId: string;
  workflowName: string;
  signalName: string;
  /** True when the signal follows the `approve:<callId>` convention. */
  isApproval: boolean;
  expiresAt: string;
  completedAt: string | null;
  /** True when the workflow is still suspended on this signal. */
  pending: boolean;
}

export interface SignalTokenDto {
  tokenId: string;
  workflowId: string;
  signalName: string;
  tags: string[];
  idempotencyKey: string | null;
  expiresAt: string;
  completedAt: string | null;
  createdAt: string;
}

/**
 * Mint a public-bearer signal token for `(workflowId, signalName)`.
 * Auth-gated by the operator's auth layer (this route doesn't enforce
 * its own auth — it sits behind whatever protects the rest of `/api`).
 */
export function mintSignalToken(deps: SignalTokenRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const workflowId = params.id;
    const signalName = params.name;
    if (!workflowId) return jsonError(400, "missing_workflow_id");
    if (!signalName) return jsonError(400, "missing_signal_name");

    const body = await readJson<MintTokenRequest>(req);
    if (!body || typeof body.expiresInMs !== "number" || body.expiresInMs <= 0) {
      return jsonError(400, "missing_expires_in_ms");
    }

    // Verify the workflow exists — failing fast at mint time prevents
    // issuing tokens against a typo'd id that no completer would ever match.
    const wf = await deps.storage.loadWorkflow(workflowId);
    if (!wf) return jsonError(404, "workflow_not_found");

    const tokenId = randomToken(16);
    const bearer = randomToken(32);
    const expiresAt = new Date(Date.now() + body.expiresInMs);

    const { record, isCached } = await deps.storage.createSignalToken({
      tokenId,
      workflowId,
      signalName,
      bearer,
      tags: body.tags ?? [],
      idempotencyKey: body.idempotencyKey ?? null,
      expiresAt,
    });

    const response: MintTokenResponse = {
      tokenId: record.tokenId,
      url: deps.publicBaseUrl
        ? `${deps.publicBaseUrl}/api/signal-tokens/${record.tokenId}/complete`
        : null,
      bearer: record.bearer,
      expiresAt: record.expiresAt.toISOString(),
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
 *   201 — first delivery, signal resumed
 *   200 — re-submission with the same bearer; same value returned
 *   401 — missing or wrong bearer
 *   404 — unknown token id
 *   408 — token expired before completion
 *   410 — token already consumed by something other than this bearer
 */
export function completeSignalToken(deps: SignalTokenRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const tokenId = params.tokenId;
    if (!tokenId) return jsonError(400, "missing_token_id");

    const bearer = extractBearer(req);
    if (!bearer) return jsonError(401, "missing_bearer");

    const token = await deps.storage.findSignalTokenById(tokenId);
    if (!token) return jsonError(404, "token_not_found");

    if (!constantTimeEqual(bearer, token.bearer)) {
      return jsonError(401, "invalid_bearer");
    }

    if (token.completedAt) {
      // Idempotent re-submission: same bearer accepted, original value returned.
      return json(200, { ok: true, value: token.completedValue, alreadyCompleted: true });
    }

    const now = new Date();
    if (token.expiresAt.getTime() <= now.getTime()) {
      return jsonError(408, "token_expired");
    }

    const body = await readJson<CompleteTokenRequest>(req);
    if (!body || !("value" in body)) {
      return jsonError(400, "missing_value");
    }

    const claim = await deps.storage.markSignalTokenCompleted({
      tokenId,
      value: body.value,
      now,
    });

    if (claim.outcome === "already_completed") {
      // Lost a race with a concurrent completer (or scanner expiry — when
      // we add one). Surface as 410 since this bearer wasn't the one that
      // delivered the value. The race window is small (claim is atomic in
      // postgres + sqlite, near-atomic in redis), but the response shape
      // is meaningful for ops debugging.
      return json(410, {
        ok: false,
        error: "already_completed",
        value: claim.record.completedValue,
      });
    }

    // Resume the workflow through the existing signal mechanic. From the
    // signal's perspective, the bearer-completer is indistinguishable from
    // an in-app `storage.deliverSignal` caller.
    await deps.storage.deliverSignal(token.workflowId, token.signalName, body.value);

    return json(201, { ok: true, value: body.value });
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
    const tokenId = params.tokenId;
    if (!tokenId) return jsonError(400, "missing_token_id");
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
    // into a workflow that's already finished.
    let pending = false;
    for (const step of Object.values(wf.steps)) {
      if (step.status === "waiting_for_signal" && step.signalName === token.signalName) {
        pending = true;
        break;
      }
    }

    const body: DescribeSignalTokenResponse = {
      workflowId: token.workflowId,
      workflowName: wf.workflowName,
      signalName: token.signalName,
      isApproval: parseApprovalSignal(token.signalName) !== null,
      expiresAt: token.expiresAt.toISOString(),
      completedAt: token.completedAt ? token.completedAt.toISOString() : null,
      pending,
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
    const workflowId = params.id;
    if (!workflowId) return jsonError(400, "missing_workflow_id");
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
    expiresAt: record.expiresAt.toISOString(),
    completedAt: record.completedAt ? record.completedAt.toISOString() : null,
    createdAt: record.createdAt.toISOString(),
  };
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
