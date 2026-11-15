// ---------------------------------------------------------------------------
// Evals API: suites, runs, comparison and promotion gates (`routes/evals.ts`).
// The gate the agent promote route consults is wired in `mount-agents.ts`.
// ---------------------------------------------------------------------------

import type { NamespaceRegistry } from "@promin/workflow";
import type { Router } from "../router.ts";
import type { ScopeAuthorizer } from "../routes/agent-gateway-kit.ts";
import {
  cancelRun,
  compareEvalRuns,
  createSuite,
  deleteGate,
  deleteSuite,
  getGate,
  getRun,
  getSuite,
  listGates,
  listRuns,
  listSuites,
  listSuiteVersions,
  promoteSuite,
  putGate,
  startRun,
  updateSuite,
  type EvalsRouteDeps,
} from "../routes/evals.ts";
import type { ZoryaEvals } from "../services/evals/index.ts";
import {
  bodyNamespaceTarget,
  orgTarget,
  protectedAccess,
  queryNamespaceTarget,
} from "./route-security.ts";

export function mountEvals(params: {
  readonly router: Router;
  readonly evals: ZoryaEvals;
  readonly namespaces: NamespaceRegistry | undefined;
  readonly authorizeScope: ScopeAuthorizer | undefined;
}): void {
  const deps: EvalsRouteDeps = {
    evals: params.evals,
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
    ...(params.authorizeScope !== undefined && { authorizeScope: params.authorizeScope }),
  };
  const suite = orgTarget({ kind: "eval", idParam: "id" });
  const gate = orgTarget({ kind: "eval", idParam: "agentId" });
  // The approved eval vocabulary intentionally groups suite/gate administration under create;
  // `run` is reserved for executing or cancelling an evaluation.
  const storedRun = async ({
    params: route,
  }: {
    readonly params: Readonly<Record<string, string>>;
  }) => {
    const id = route.runId;
    const run = id ? await params.evals.getRun(id) : undefined;
    return {
      kind: "eval" as const,
      namespaceId: run?.namespaceId ?? "org",
      ...(id !== undefined && { id }),
    };
  };
  params.router.route({
    method: "GET",
    path: "/api/evals/suites",
    access: protectedAccess({ resource: "eval", action: "read" }),
    handler: listSuites(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/evals/suites",
    access: protectedAccess({ resource: "eval", action: "create" }),
    handler: createSuite(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/suites/:id",
    access: protectedAccess({ resource: "eval", action: "read", target: suite }),
    handler: getSuite(deps),
  });
  params.router.route({
    method: "PATCH",
    path: "/api/evals/suites/:id",
    access: protectedAccess({ resource: "eval", action: "create", target: suite }),
    handler: updateSuite(deps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/evals/suites/:id",
    access: protectedAccess({ resource: "eval", action: "create", target: suite }),
    handler: deleteSuite(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/suites/:id/versions",
    access: protectedAccess({ resource: "eval", action: "read", target: suite }),
    handler: listSuiteVersions(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/evals/suites/:id/promote",
    access: protectedAccess({ resource: "eval", action: "create", target: suite }),
    handler: promoteSuite(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/evals/runs",
    access: protectedAccess({
      resource: "eval",
      action: "run",
      target: bodyNamespaceTarget({ kind: "eval" }),
    }),
    handler: startRun(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/runs",
    access: protectedAccess({
      resource: "eval",
      action: "read",
      target: queryNamespaceTarget({ kind: "eval" }),
    }),
    handler: listRuns(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/runs/:runId",
    access: protectedAccess({ resource: "eval", action: "read", target: storedRun }),
    handler: getRun(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/evals/runs/:runId/cancel",
    access: protectedAccess({ resource: "eval", action: "run", target: storedRun }),
    handler: cancelRun(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/compare",
    access: protectedAccess({ resource: "eval", action: "read" }),
    handler: compareEvalRuns(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/gates",
    access: protectedAccess({ resource: "eval", action: "read" }),
    handler: listGates(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/evals/gates/:agentId",
    access: protectedAccess({ resource: "eval", action: "read", target: gate }),
    handler: getGate(deps),
  });
  params.router.route({
    method: "PUT",
    path: "/api/evals/gates/:agentId",
    access: protectedAccess({ resource: "eval", action: "create", target: gate }),
    handler: putGate(deps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/evals/gates/:agentId",
    access: protectedAccess({ resource: "eval", action: "create", target: gate }),
    handler: deleteGate(deps),
  });
}
