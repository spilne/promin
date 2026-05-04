// ---------------------------------------------------------------------------
// Read-only metadata endpoints for the agent designer UI.
//
// `GET /api/agents/_catalog/models` returns the host's `ModelCatalog`
// stripped of the runtime `llm` field — JSON-safe wire shape that powers
// the designer's model dropdown. The `_catalog` URL prefix signals that
// these are metadata reads, not agent invocations (the `_` keeps them
// from colliding with a real agent id).
// ---------------------------------------------------------------------------

import type { ModelCatalog, SerializedModelCatalogItem } from "@promin/agent";
import { json } from "../router.ts";

export interface ModelsCatalogResponse {
  models: SerializedModelCatalogItem[];
}

export interface AgentCatalogDeps {
  readonly models: ModelCatalog;
}

export function listCatalogModels(deps: AgentCatalogDeps) {
  return async (): Promise<Response> => {
    const body: ModelsCatalogResponse = { models: deps.models.serialize() };
    return json(200, body);
  };
}
