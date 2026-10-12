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
  params.router
    .get("/api/evals/suites", listSuites(deps))
    .post("/api/evals/suites", createSuite(deps))
    .get("/api/evals/suites/:id", getSuite(deps))
    .patch("/api/evals/suites/:id", updateSuite(deps))
    .delete("/api/evals/suites/:id", deleteSuite(deps))
    .get("/api/evals/suites/:id/versions", listSuiteVersions(deps))
    .post("/api/evals/suites/:id/promote", promoteSuite(deps))
    .post("/api/evals/runs", startRun(deps))
    .get("/api/evals/runs", listRuns(deps))
    .get("/api/evals/runs/:runId", getRun(deps))
    .post("/api/evals/runs/:runId/cancel", cancelRun(deps))
    .get("/api/evals/compare", compareEvalRuns(deps))
    .get("/api/evals/gates", listGates(deps))
    .get("/api/evals/gates/:agentId", getGate(deps))
    .put("/api/evals/gates/:agentId", putGate(deps))
    .delete("/api/evals/gates/:agentId", deleteGate(deps));
}
