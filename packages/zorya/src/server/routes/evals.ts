// ---------------------------------------------------------------------------
// Evals API — eval suites (versioned per `versioned-resource.ts`), durable
// eval runs against agent recipe versions, run comparison and promotion
// gates.
//
//   GET    /api/evals/suites                 — active version per suite (?agentId, ?tag)
//   POST   /api/evals/suites                 — register a new suite (409 when it exists)
//   GET    /api/evals/suites/:id             — fetch (active, or ?version=)
//   PATCH  /api/evals/suites/:id             — new version / kill switch; `If-Match` required
//   GET    /api/evals/suites/:id/versions    — version history
//   POST   /api/evals/suites/:id/promote     — make a version active
//   DELETE /api/evals/suites/:id             — remove all versions (or ?version=)
//
//   POST   /api/evals/runs                   — run a suite against an agent version (202 + runId)
//   GET    /api/evals/runs                   — runs newest first (?suiteId ?agentId ?agentVersion ?status ?limit)
//   GET    /api/evals/runs/:runId            — one run with its case results (poll for progress)
//   POST   /api/evals/runs/:runId/cancel     — cancel a running run
//   GET    /api/evals/compare                — ?baseline=<runId>&candidate=<runId>: per-case diff
//
//   GET    /api/evals/gates                  — every agent's promotion gate
//   GET    /api/evals/gates/:agentId         — one gate (+ ?version=: how that version fares)
//   PUT    /api/evals/gates/:agentId         — set the gate `{ suiteId, minScore, suiteVersion?, scorerId? }`
//   DELETE /api/evals/gates/:agentId         — remove the gate
//
// Authorization (with an `authorizeScope` hook):
//   - a run acts in the caller's scope (`namespaceId`, `resourceId` default
//     `eval-<runId>`), checked like an agent invocation (403) plus an
//     active namespace; reading, cancelling and comparing runs is checked
//     against the scope each run acted in (a run the caller may not see is
//     404), and listing runs needs `?namespaceId=` it may act in;
//   - suite writes need `{ kind: "operator", resource: "eval-suite" }`,
//     gate writes `{ kind: "operator", resource: "eval-gate" }` — either
//     can weaken what a promotion must pass.
// A run reads what an agent invocation in its scope reads (namespace
// facts, knowledge bases, BYOK credentials) and spends that namespace's
// budget. Suites read datasets inline or from the dataset store; a `jsonl`
// dataset (a path on this host) is refused.
// ---------------------------------------------------------------------------

import { isRegistryInputError } from "@promin/agent";
import {
  diffRuns,
  storedRunSummary,
  type DatasetSpec,
  type EvalGate,
  type EvalRunQuery,
  type EvalRunRecord,
  type EvalRunStatus,
  type EvalSuiteInput,
  type RegisteredEvalGate,
  type RegisteredEvalSuite,
  type RunDiff,
  type StoredEvalRun,
} from "@promin/evals";
import type { NamespaceRegistry } from "@promin/workflow";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { HttpError, json, jsonError, parseIntParam, readJson } from "../router.ts";
import type { ZoryaEvals } from "../services/evals/index.ts";
import { WorkflowIdConflictError } from "../services/workflows/errors.ts";
import type { ScopeAuthorizer } from "./agent-gateway-kit.ts";
import {
  createOnly,
  deleteRoute,
  entryResponse,
  ifMatchVersions,
  listVersionsRoute,
  matchActiveVersion,
  optionalVersion,
  promoteRoute,
  registryCall,
} from "./versioned-resource.ts";

export type { RegisteredEvalGate, RegisteredEvalSuite, RunDiff, StoredEvalRun, EvalRunRecord };

export interface EvalsRouteDeps {
  readonly evals: ZoryaEvals;
  readonly namespaces?: NamespaceRegistry;
  readonly authorizeScope?: ScopeAuthorizer;
}

export interface EvalSuitesListResponse {
  readonly suites: RegisteredEvalSuite[];
  readonly nextCursor: string | null;
}
export interface EvalRunsListResponse {
  readonly runs: EvalRunRecord[];
}
export interface EvalRunStartedResponse {
  readonly runId: string;
  readonly existing?: true;
}
export interface EvalGatesListResponse {
  readonly gates: RegisteredEvalGate[];
}
export interface EvalGateResponse {
  readonly gate: RegisteredEvalGate;
  /** With `?version=`: how that agent version fares against the gate. */
  readonly verdict?: {
    readonly passed: boolean;
    readonly score: number | null;
    readonly latest: EvalRunRecord | null;
    readonly reason?: string;
  };
}

const RUN_STATUSES: ReadonlyArray<EvalRunStatus> = ["running", "completed", "failed", "cancelled"];

function suiteNotFound(id: string): Response {
  return jsonError(404, "suite_not_found", `Eval suite "${id}" not registered.`);
}

function gateNotFound(agentId: string): Response {
  return jsonError(404, "gate_not_found", `Agent "${agentId}" has no promotion gate.`);
}

// ---- Suites ----------------------------------------------------------------

export function listSuites(deps: EvalsRouteDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 1_000 });
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const agentId = url.searchParams.get("agentId") ?? undefined;
    const page = await deps.evals.suites.list({
      ...((tag !== undefined || agentId !== undefined) && {
        filter: {
          ...(tag !== undefined && { tag }),
          ...(agentId !== undefined && { agentId }),
        },
      }),
      ...(limit !== undefined && { limit }),
      ...(cursor !== undefined && { cursor }),
    });
    return json(200, {
      suites: page.items,
      nextCursor: page.nextCursor,
    } satisfies EvalSuitesListResponse);
  };
}

export function getSuite(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const suite = await deps.evals.suites.get({ id, ...(version !== undefined && { version }) });
    if (!suite) return suiteNotFound(id);
    return entryResponse({ status: 200, entry: suite });
  };
}

export function listSuiteVersions(deps: EvalsRouteDeps) {
  return listVersionsRoute({ registry: deps.evals.suites, notFound: suiteNotFound });
}

/**
 * 403 `scope_forbidden` unless the caller has operator access to
 * `resource` (when the host authorizes scopes). Suite and gate writes need
 * it: editing a suite or a gate can weaken what a promotion must pass.
 */
async function requireOperator(params: {
  readonly deps: EvalsRouteDeps;
  readonly req: Request;
  readonly resource: "eval-gate" | "eval-suite";
}): Promise<Response | undefined> {
  const authorize = params.deps.authorizeScope;
  if (!authorize) return undefined;
  const allowed = await authorize({
    req: params.req,
    scope: { kind: "operator", resource: params.resource },
  });
  return allowed
    ? undefined
    : jsonError(403, "scope_forbidden", `Changing ${params.resource}s needs operator access.`);
}

/** `handler` behind `requireOperator`. */
function operatorOnly(params: {
  readonly deps: EvalsRouteDeps;
  readonly resource: "eval-gate" | "eval-suite";
  readonly handler: (req: Request, routeParams: Record<string, string>) => Promise<Response>;
}) {
  return async (req: Request, routeParams: Record<string, string>): Promise<Response> =>
    (await requireOperator({ deps: params.deps, req, resource: params.resource })) ??
    params.handler(req, routeParams);
}

export function promoteSuite(deps: EvalsRouteDeps) {
  return operatorOnly({
    deps,
    resource: "eval-suite",
    handler: promoteRoute({ registry: deps.evals.suites, notFound: suiteNotFound }),
  });
}

export function deleteSuite(deps: EvalsRouteDeps) {
  return operatorOnly({
    deps,
    resource: "eval-suite",
    handler: deleteRoute({ registry: deps.evals.suites, notFound: suiteNotFound }),
  });
}

/** The suite fields of a request body; validation happens in the registry. */
function suiteBody(raw: Record<string, unknown>): EvalSuiteInput | Response {
  const dataset = raw.dataset as DatasetSpec | undefined;
  if (dataset?.kind === "jsonl") {
    return jsonError(
      400,
      "unsupported_dataset",
      "jsonl datasets read a file on the server; use an inline or stored dataset.",
    );
  }
  return {
    ...(raw.description !== undefined && { description: raw.description as string | null }),
    dataset: raw.dataset as DatasetSpec,
    scorers: (raw.scorers ?? []) as EvalSuiteInput["scorers"],
    ...(raw.samplesPerCase !== undefined && { samplesPerCase: raw.samplesPerCase as number }),
    ...(raw.concurrency !== undefined && { concurrency: raw.concurrency as number }),
    ...(raw.passThreshold !== undefined && { passThreshold: raw.passThreshold as number }),
    ...(raw.caseTimeoutMs !== undefined && { caseTimeoutMs: raw.caseTimeoutMs as number }),
    ...(raw.idempotentCases !== undefined && { idempotentCases: raw.idempotentCases as boolean }),
    ...(raw.allowedTools !== undefined && { allowedTools: raw.allowedTools as string[] }),
    ...(raw.metadata !== undefined && { metadata: raw.metadata as EvalSuiteInput["metadata"] }),
  };
}

/** `POST /api/evals/suites` — register a new suite (409 `suite_exists` when the id is taken). */
export function createSuite(deps: EvalsRouteDeps) {
  return async (req: Request): Promise<Response> => {
    const forbidden = await requireOperator({ deps, req, resource: "eval-suite" });
    if (forbidden) return forbidden;
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    const id = body.id;
    const suite = suiteBody(body);
    if (suite instanceof Response) return suite;
    const version = optionalVersion(body.version);
    const created = await createOnly({
      register: () =>
        deps.evals.suites.register({
          id,
          ...(version !== undefined && { version }),
          ...suite,
          expectedActive: null,
        }),
      onExists: () => jsonError(409, "suite_exists", `Eval suite "${id}" already exists.`),
    });
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

/**
 * `PATCH /api/evals/suites/:id` — `If-Match` required. A `dataset` /
 * `scorers` body becomes a new version (`publish: false` keeps it a
 * draft); `enabled` flips the kill switch.
 */
export function updateSuite(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const forbidden = await requireOperator({ deps, req, resource: "eval-suite" });
    if (forbidden) return forbidden;
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    const ifMatch = ifMatchVersions({ req, required: true });
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    const active = await deps.evals.suites.get({ id });
    if (!active) return suiteNotFound(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    let current: RegisteredEvalSuite = active;
    if (body.dataset !== undefined || body.scorers !== undefined) {
      const suite = suiteBody({
        description: active.description,
        dataset: active.dataset,
        scorers: active.scorers,
        samplesPerCase: active.samplesPerCase,
        concurrency: active.concurrency,
        passThreshold: active.passThreshold,
        caseTimeoutMs: active.caseTimeoutMs,
        idempotentCases: active.idempotentCases,
        allowedTools: active.allowedTools,
        metadata: active.metadata,
        ...body,
      });
      if (suite instanceof Response) return suite;
      const version = optionalVersion(body.version);
      current = await registryCall(() =>
        deps.evals.suites.register({
          id,
          ...(version !== undefined && { version }),
          ...suite,
          promote: body.publish !== false,
          expectedActive: active.version,
          ...(enabled !== undefined && { enabled }),
        }),
      );
    } else if (enabled !== undefined) {
      current = await registryCall(() =>
        deps.evals.suites.setEnabled({ id, enabled, expectedActive: active.version }),
      );
    }
    return entryResponse({ status: 200, entry: current });
  };
}

// ---- Runs ------------------------------------------------------------------

interface StartRunRequest {
  readonly suiteId?: unknown;
  readonly suiteVersion?: unknown;
  readonly agentId?: unknown;
  readonly agentVersion?: unknown;
  readonly namespaceId?: unknown;
  readonly resourceId?: unknown;
  readonly runId?: unknown;
}

function optionalString(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

/** `POST /api/evals/runs` — start a durable run; 202 `{ runId }` at once. */
export function startRun(deps: EvalsRouteDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<StartRunRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const suiteId = optionalString(body.suiteId);
    const agentId = optionalString(body.agentId);
    const namespaceId = optionalString(body.namespaceId);
    if (!suiteId) return jsonError(400, "missing_suite_id");
    if (!agentId) return jsonError(400, "missing_agent_id");
    if (!namespaceId) return jsonError(400, "missing_namespace_id");
    // Default: a resource of its own per run (`eval-<runId>`), so cases
    // never share memory with a real user's.
    const resourceId = optionalString(body.resourceId);
    if (deps.authorizeScope) {
      const allowed = await deps.authorizeScope({
        req,
        scope: { namespaceId, ...(resourceId !== undefined && { resourceId }), agentId },
      });
      if (!allowed) return jsonError(403, "scope_forbidden");
    }
    await requireActiveNamespace({ registry: deps.namespaces, namespace: namespaceId });
    const suiteVersion = optionalString(body.suiteVersion);
    const agentVersion = optionalString(body.agentVersion);
    const runId = optionalString(body.runId);
    let started: Awaited<ReturnType<ZoryaEvals["startRun"]>>;
    try {
      started = await deps.evals.startRun({
        suiteId,
        ...(suiteVersion !== undefined && { suiteVersion }),
        agentId,
        ...(agentVersion !== undefined && { agentVersion }),
        scope: { namespaceId, ...(resourceId !== undefined && { resourceId }) },
        ...(runId !== undefined && { runId }),
      });
    } catch (err) {
      if (err instanceof WorkflowIdConflictError) {
        return jsonError(409, "run_id_conflict", err.message);
      }
      throw err;
    }
    if ("error" in started) {
      const status =
        started.error === "suite_not_found" || started.error === "agent_not_found"
          ? 404
          : started.error === "agent_disabled"
            ? 410
            : started.error === "run_id_conflict"
              ? 409
              : 400;
      return jsonError(status, started.error, started.message);
    }
    return json(202, started satisfies EvalRunStartedResponse);
  };
}

export function listRuns(deps: EvalsRouteDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const status = url.searchParams.get("status") ?? undefined;
    if (status !== undefined && !RUN_STATUSES.includes(status as EvalRunStatus)) {
      return jsonError(400, "invalid_status");
    }
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 500 }) ?? 50;
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    // A host that authorizes scopes lists one authorized namespace at a time.
    if (deps.authorizeScope) {
      if (namespaceId === undefined) return jsonError(400, "missing_namespace_id");
      if (!(await deps.authorizeScope({ req, scope: { namespaceId } }))) {
        return jsonError(403, "scope_forbidden");
      }
    }
    const query: EvalRunQuery = {
      ...(namespaceId !== undefined && { namespaceId }),
      ...stringParam({ url, name: "suiteId", key: "suiteId" }),
      ...stringParam({ url, name: "suiteVersion", key: "suiteVersion" }),
      ...stringParam({ url, name: "agentId", key: "targetId" }),
      ...stringParam({ url, name: "agentVersion", key: "targetVersion" }),
      ...(status !== undefined && { status: status as EvalRunStatus }),
      limit,
    };
    return json(200, { runs: await deps.evals.listRuns(query) } satisfies EvalRunsListResponse);
  };
}

function stringParam(params: {
  url: URL;
  name: string;
  key: keyof EvalRunQuery;
}): Partial<EvalRunQuery> {
  const value = params.url.searchParams.get(params.name);
  return value ? { [params.key]: value } : {};
}

/**
 * The run, when the caller may see it: `authorizeScope` for the scope the
 * run acted in (its namespace, resource and agent), as for starting it.
 * A run the caller may not see answers 404 like an unknown one.
 */
async function readableRun(params: {
  readonly deps: EvalsRouteDeps;
  readonly req: Request;
  readonly runId: string;
}): Promise<StoredEvalRun | Response> {
  const { deps, req, runId } = params;
  const notFound = jsonError(404, "run_not_found", `Eval run "${runId}" not found.`);
  const run = await deps.evals.getRun(runId);
  if (!run) return notFound;
  if (deps.authorizeScope) {
    if (run.namespaceId === undefined) return notFound;
    const resourceId = run.metadata?.resourceId;
    const allowed = await deps.authorizeScope({
      req,
      scope: {
        namespaceId: run.namespaceId,
        ...(typeof resourceId === "string" && { resourceId }),
        agentId: run.targetId,
      },
    });
    if (!allowed) return notFound;
  }
  return run;
}

export function getRun(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const runId = params.runId;
    if (!runId) return jsonError(400, "missing_run_id");
    const run = await readableRun({ deps, req, runId });
    if (run instanceof Response) return run;
    return json(200, run satisfies StoredEvalRun);
  };
}

export function cancelRun(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const runId = params.runId;
    if (!runId) return jsonError(400, "missing_run_id");
    const readable = await readableRun({ deps, req, runId });
    if (readable instanceof Response) return readable;
    const outcome = await deps.evals.cancelRun(runId);
    if (outcome === "not_found") {
      return jsonError(404, "run_not_found", `Eval run "${runId}" not found.`);
    }
    if (outcome === "ended") return jsonError(409, "run_ended", `Eval run "${runId}" has ended.`);
    return json(200, await deps.evals.getRun(runId));
  };
}

/** `GET /api/evals/compare?baseline=&candidate=` — the two runs side by side, case by case. */
export function compareEvalRuns(deps: EvalsRouteDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const baselineId = url.searchParams.get("baseline");
    const candidateId = url.searchParams.get("candidate");
    if (!baselineId || !candidateId) return jsonError(400, "missing_run_ids");
    const baseline = await readableRun({ deps, req, runId: baselineId });
    if (baseline instanceof Response) return baseline;
    const candidate = await readableRun({ deps, req, runId: candidateId });
    if (candidate instanceof Response) return candidate;
    return json(
      200,
      diffRuns({
        baseline: storedRunSummary(baseline),
        candidate: storedRunSummary(candidate),
      }) satisfies RunDiff,
    );
  };
}

// ---- Gates -----------------------------------------------------------------

export function listGates(deps: EvalsRouteDeps) {
  return async (): Promise<Response> => {
    const gates: RegisteredEvalGate[] = [];
    let cursor: string | undefined;
    for (;;) {
      const page = await deps.evals.gates.list({
        limit: 500,
        ...(cursor !== undefined && { cursor }),
      });
      gates.push(...page.items);
      if (page.nextCursor === null) break;
      cursor = page.nextCursor;
    }
    return json(200, { gates } satisfies EvalGatesListResponse);
  };
}

export function getGate(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const agentId = params.agentId;
    if (!agentId) return jsonError(400, "missing_agent_id");
    const gate = await deps.evals.gates.get({ id: agentId });
    if (!gate) return gateNotFound(agentId);
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    if (version === undefined) return json(200, { gate } satisfies EvalGateResponse);
    const check = await deps.evals.checkGate({ agentId, version });
    return json(200, {
      gate,
      ...(check !== null && {
        verdict: {
          passed: check.verdict.passed,
          score: check.verdict.score,
          latest: check.verdict.latest,
          ...(check.verdict.reason !== undefined && { reason: check.verdict.reason }),
        },
      }),
    } satisfies EvalGateResponse);
  };
}

/** `PUT /api/evals/gates/:agentId` — set (or replace) the agent's gate; the new policy is active at once. */
export function putGate(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const agentId = params.agentId;
    if (!agentId) return jsonError(400, "missing_agent_id");
    const forbidden = await requireOperator({ deps, req, resource: "eval-gate" });
    if (forbidden) return forbidden;
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    const activeSuite =
      typeof body.suiteId === "string" ? await deps.evals.suites.get({ id: body.suiteId }) : null;
    if (typeof body.suiteId === "string" && !activeSuite) return suiteNotFound(body.suiteId);
    const policy = {
      suiteId: body.suiteId,
      minScore: body.minScore,
      ...(body.suiteVersion !== undefined && { suiteVersion: body.suiteVersion }),
      // Unpinned: checked against the suite's active version at check time;
      // recorded here for display only.
      ...(body.suiteVersion === undefined &&
        activeSuite && { suiteVersionAtSet: activeSuite.version }),
      ...(body.scorerId !== undefined && { scorerId: body.scorerId }),
    } as EvalGate;
    try {
      const gate = await deps.evals.gates.register({
        id: agentId,
        ...policy,
        promote: true,
        enabled: body.enabled !== false,
      });
      return json(200, { gate } satisfies EvalGateResponse);
    } catch (err) {
      if (isRegistryInputError(err)) return jsonError(400, "invalid_gate", err.message);
      throw err;
    }
  };
}

export function deleteGate(deps: EvalsRouteDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const agentId = params.agentId;
    if (!agentId) return jsonError(400, "missing_agent_id");
    const forbidden = await requireOperator({ deps, req, resource: "eval-gate" });
    if (forbidden) return forbidden;
    const removed = await deps.evals.gates.unregister({ id: agentId });
    if (!removed) return gateNotFound(agentId);
    return new Response(null, { status: 204 });
  };
}

// ---- The promotion gate the agent routes consult ----------------------------

/**
 * Decides whether an agent version may become active. Answers a `Response`
 * to refuse (409 `eval_gate_failed`, 403 for an override the caller may not
 * make), `undefined` to let the write go on. `version: null` is a version
 * the write would create (a publishing edit), which no run has measured.
 */
export type PromotionGate = (params: {
  readonly req: Request;
  readonly agentId: string;
  readonly version: string | null;
  /** The caller asked to promote past a failing gate. */
  readonly override: boolean;
}) => Promise<Response | undefined>;

/** Body of a 409 `eval_gate_failed`. */
export interface EvalGateFailedResponse {
  readonly error: "eval_gate_failed";
  readonly message: string;
  readonly gate: RegisteredEvalGate;
  readonly score: number | null;
  /** The newest completed run of the gate's suite on the version (null: none). */
  readonly latest: EvalRunRecord | null;
}

/** The `PromotionGate` backed by an evals service's gates and run history. */
export function evalPromotionGate(deps: EvalsRouteDeps): PromotionGate {
  return async ({ req, agentId, version, override }) => {
    const gate = await deps.evals.gates.get({ id: agentId });
    if (!gate || !gate.enabled) return undefined;
    if (override) {
      if (deps.authorizeScope) {
        const allowed = await deps.authorizeScope({
          req,
          scope: { kind: "operator", resource: "eval-gate" },
        });
        if (!allowed) {
          return jsonError(
            403,
            "scope_forbidden",
            "Overriding an eval gate needs operator access.",
          );
        }
      }
      return undefined;
    }
    const check = version === null ? null : await deps.evals.checkGate({ agentId, version });
    if (check?.verdict.passed === true) return undefined;
    const message =
      version === null
        ? `Agent "${agentId}" is gated on eval suite "${gate.suiteId}": save the edit as a draft (publish: false), run the suite on it, then promote it.`
        : (check?.verdict.reason ?? `Agent "${agentId}@${version}" did not pass its eval gate.`);
    return json(409, {
      error: "eval_gate_failed",
      message,
      gate,
      score: check?.verdict.score ?? null,
      latest: check?.verdict.latest ?? null,
    } satisfies EvalGateFailedResponse);
  };
}

/** Throws a 400 `invalid_override` for a non-boolean override flag. */
export function overrideFlag(raw: unknown): boolean {
  if (raw === undefined) return false;
  if (typeof raw !== "boolean") {
    throw new HttpError({
      status: 400,
      code: "invalid_override",
      message: "overrideEvalGate must be a boolean.",
    });
  }
  return raw;
}
