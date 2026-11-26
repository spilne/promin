import type { DagRunScope, DecisionModelRef } from "../dag/types.ts";
import type { ModelCostRates } from "../metrics/types.ts";
import type { SecretsStorage } from "../secrets/types.ts";
import type { DecisionProvider } from "./types.ts";

export interface DecisionModelCatalogItem {
  readonly provider: string;
  readonly id: string;
  readonly displayName?: string;
  readonly pricing?: ModelCostRates;
  create(params: { readonly apiKey?: string }): DecisionProvider;
}

function catalogKey(params: { readonly provider: string; readonly id: string }): string {
  return `${params.provider}::${params.id}`;
}

/** In-process catalogue of decision providers keyed by provider and model id. */
export class InMemoryDecisionModelCatalog {
  private readonly items: Map<string, DecisionModelCatalogItem>;

  constructor(items: ReadonlyArray<DecisionModelCatalogItem>) {
    this.items = new Map();
    for (const item of items) {
      const key = catalogKey(item);
      if (this.items.has(key)) {
        throw new Error(
          `InMemoryDecisionModelCatalog: duplicate entry for provider "${item.provider}" and id "${item.id}"`,
        );
      }
      this.items.set(key, item);
    }
  }

  get(provider: string, id: string): DecisionModelCatalogItem | undefined {
    return this.items.get(catalogKey({ provider, id }));
  }

  list(): DecisionModelCatalogItem[] {
    return Array.from(this.items.values());
  }
}

export interface ResolvedDecisionModel {
  readonly provider: DecisionProvider;
  readonly rates?: ModelCostRates;
}

export type DecisionResolver = (params: {
  readonly model: DecisionModelRef;
  readonly scope: DagRunScope;
}) => Promise<ResolvedDecisionModel>;

export class DecisionModelRefusedError extends Error {
  readonly _tag = "DecisionModelRefusedError";

  constructor(message: string) {
    super(message);
    this.name = "DecisionModelRefusedError";
  }
}

/** Resolve a recipe's model reference, including its optional scoped credential. */
export function createCatalogDecisionResolver(params: {
  readonly catalog: InMemoryDecisionModelCatalog;
  readonly secrets?: SecretsStorage;
}): DecisionResolver {
  return async ({ model, scope }) => {
    const item = params.catalog.get(model.provider, model.id);
    if (item === undefined) {
      throw new DecisionModelRefusedError(
        `decision_model_not_found: no decision model registered as "${model.provider}/${model.id}"`,
      );
    }

    let apiKey: string | undefined;
    if (model.credentialRef !== undefined) {
      if (params.secrets === undefined) {
        throw new DecisionModelRefusedError(
          `decision_credential_missing: model "${model.provider}/${model.id}" requires credential "${model.credentialRef}", but no secrets storage is configured`,
        );
      }
      const secret = await params.secrets.resolve({
        namespaceId: scope.namespaceId,
        ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
        key: model.credentialRef,
      });
      if (secret === null) {
        throw new DecisionModelRefusedError(
          `decision_credential_missing: credential "${model.credentialRef}" was not found for model "${model.provider}/${model.id}"`,
        );
      }
      apiKey = secret.value;
    }

    return {
      provider: item.create({ ...(apiKey !== undefined && { apiKey }) }),
      ...(item.pricing !== undefined && { rates: item.pricing }),
    };
  };
}
