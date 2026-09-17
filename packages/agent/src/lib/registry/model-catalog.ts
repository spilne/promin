// ---------------------------------------------------------------------------
// `ModelCatalog` — the bridge between an `AgentBackend.model = { provider, id }`
// recipe field and a live `LLMProvider`.
//
// Why this exists: recipes are JSON-serializable data, but `LLMProvider` is
// a runtime closure (holds API keys, fetch state, streaming generators). The
// catalog stores the runtime instance keyed by `(provider, id)` so the
// resolver can look it up without the host hand-wiring every agent's LLM by
// recipe id.
//
// Why operators care: with a populated catalog, an operator can clone an
// agent recipe in the designer UI, change `backend.model.id` from
// `"claude-haiku-4-5-20251001"` to `"claude-sonnet-4-6"`, and ship — no code
// change, no redeploy. The catalog also powers the model dropdown in the
// designer (via the `serialize()` projection, which omits the runtime-only
// `llm` field).
// ---------------------------------------------------------------------------

import type { LLMProvider } from "../llm-provider.ts";

/** Capability tag — matches what the designer UI's filter chips compare against. */
export type ModelCapability = "chat" | "tools" | "vision" | "thinking" | "stream" | string;

/** Cost tier for the dropdown. UI maps these to the usual cheap/mid/spendy badges. */
export type ModelCostTier = "low" | "mid" | "high";

/**
 * One entry in the catalog. The `llm` field is runtime-only and stripped
 * before serialization to the designer UI — same pattern as tool
 * implementations vs tool names in agent recipes.
 */
export interface ModelCatalogItem {
  readonly provider: string;
  readonly id: string;
  /** Display name for UI dropdowns. Defaults to `<provider>:<id>` when omitted. */
  readonly displayName?: string;
  /** Token context window. Used by the designer to render a "fits in context" hint. */
  readonly contextLimit?: number;
  /** Capability tags. Designer's filter / availability checks read these. */
  readonly capabilities?: ReadonlyArray<ModelCapability>;
  /** Cost tier for the picker. */
  readonly costTier?: ModelCostTier;
  /** Live LLMProvider. Runtime-only — not serializable. */
  readonly llm: LLMProvider;
}

/**
 * Wire-shape of `ModelCatalogItem` exposed to the designer UI via
 * `GET /api/agents/_catalog/models`. Same fields minus `llm`. Defined as a
 * separate type so callers can typecheck "this came off the wire" vs
 * "this has a runtime LLM attached".
 */
export interface SerializedModelCatalogItem {
  readonly provider: string;
  readonly id: string;
  readonly displayName?: string;
  readonly contextLimit?: number;
  readonly capabilities?: ReadonlyArray<ModelCapability>;
  readonly costTier?: ModelCostTier;
}

/** Read-only registry of available models keyed by `(provider, id)`. */
export interface ModelCatalog {
  /** Look up by `(provider, id)`. Returns `undefined` when the pair isn't registered. */
  get(provider: string, id: string): ModelCatalogItem | undefined;
  /** Snapshot of every entry. Order is insertion order for deterministic UIs. */
  list(): ModelCatalogItem[];
  /** Snapshot stripped of `llm` for HTTP serialization to the designer UI. */
  serialize(): SerializedModelCatalogItem[];
}

function keyOf(provider: string, id: string): string {
  return `${provider}::${id}`;
}

function toSerialized(item: ModelCatalogItem): SerializedModelCatalogItem {
  // Spread + delete pattern would lose `readonly` and the inferred shape,
  // so destructure to discard `llm` and keep the rest. Conditional spreads
  // keep `undefined` fields off the wire entirely.
  const { llm: _llm, ...rest } = item;
  void _llm;
  return rest;
}

/**
 * In-process `ModelCatalog`. Constructed once at boot from a static array;
 * mutation isn't supported (re-create the catalog if you need to swap an
 * entry — recipes pin `(provider, id)`, not the catalog instance). For
 * file-driven discovery use `createFileModelCatalog`.
 *
 * Dedupe: a duplicate `(provider, id)` pair throws at construction time —
 * silently keeping last-write-wins would let two boot orderings produce
 * different LLM bindings for the same recipe, which is exactly the kind of
 * drift the catalog is supposed to prevent.
 *
 * @example
 * ```ts
 * const catalog = new InMemoryModelCatalog([
 *   { provider: "anthropic", id: "claude-sonnet-4-6", llm: anthropic("claude-sonnet-4-6") },
 *   { provider: "anthropic", id: "claude-haiku-4-5-20251001", costTier: "low",
 *     llm: anthropic("claude-haiku-4-5-20251001") },
 * ]);
 * const item = catalog.get("anthropic", "claude-sonnet-4-6");
 * ```
 */
export class InMemoryModelCatalog implements ModelCatalog {
  private readonly byKey: Map<string, ModelCatalogItem>;

  constructor(items: ReadonlyArray<ModelCatalogItem> = []) {
    this.byKey = new Map();
    for (const item of items) {
      const key = keyOf(item.provider, item.id);
      if (this.byKey.has(key)) {
        throw new Error(
          `InMemoryModelCatalog: duplicate entry for (provider="${item.provider}", id="${item.id}"). ` +
            "Each (provider, id) pair must be unique — recipes resolve by exact key.",
        );
      }
      this.byKey.set(key, item);
    }
  }

  get(provider: string, id: string): ModelCatalogItem | undefined {
    return this.byKey.get(keyOf(provider, id));
  }

  list(): ModelCatalogItem[] {
    return Array.from(this.byKey.values());
  }

  serialize(): SerializedModelCatalogItem[] {
    return this.list().map(toSerialized);
  }
}
