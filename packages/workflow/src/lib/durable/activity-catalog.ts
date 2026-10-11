// ---------------------------------------------------------------------------
// ActivityCatalog — an ActivityRegistry that also describes its activities.
//
// A visual editor needs more than a name per activity: a title and category
// for its palette, JSON Schemas for the config form and for wiring checks,
// and default config for a freshly dropped node. The catalog carries that
// metadata next to each factory, resolves references for `compileWorkflow`
// like any registry, and hands the metadata (never the factories) to the UI.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../shared/tagged-error.ts";
import type { ActivityContext, ActivityFactory, ActivityRegistry } from "./activity-registry.ts";
import type { JsonSchema } from "./workflow-schema.ts";

/** What an editor shows and checks for one activity. JSON-serializable. */
export interface ActivityDescriptor {
  /** The `activityRef` steps use. */
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  /** Palette group, e.g. `Text` or `JSON`. */
  readonly category?: string;
  readonly tags?: ReadonlyArray<string>;
  /** Host capabilities the activity needs (e.g. `postgres`), shown as badges. */
  readonly capabilities?: ReadonlyArray<string>;
  /** Shape of the step's `config`; editors build a form from it. */
  readonly configSchema?: JsonSchema;
  /** Shape of the value the activity reads as `prev`. */
  readonly inputSchema?: JsonSchema;
  /** Shape of the value it returns. A boolean output marks a branch condition. */
  readonly outputSchema?: JsonSchema;
  /** Config a newly added step starts with; merged under the step's own config. */
  readonly defaultConfig?: Readonly<Record<string, unknown>>;
}

/** An activity's metadata plus its implementation. */
export interface ActivityDefinition extends ActivityDescriptor {
  readonly factory: ActivityFactory;
}

/** An `ActivityRegistry` whose activities carry editor metadata. */
export interface ActivityCatalog extends ActivityRegistry {
  /** Every activity's metadata, ordered by category, then id. */
  describe(): ActivityDescriptor[];
  /** One activity's metadata, or `undefined`. */
  descriptor(id: string): ActivityDescriptor | undefined;
}

/**
 * Map-backed `ActivityCatalog`. `resolve` passes the activity's
 * `defaultConfig` overlaid with the step's own config to the factory.
 *
 * @example
 * ```ts
 * const catalog = new MapActivityCatalog([
 *   {
 *     id: "text.upper",
 *     title: "Uppercase",
 *     category: "Text",
 *     inputSchema: { type: "string" },
 *     outputSchema: { type: "string" },
 *     factory: () => (ctx) => succeed(String(ctx.prev).toUpperCase()),
 *   },
 * ]);
 * const wf = compileWorkflow({ schema, registry: catalog });
 * ```
 */
export class MapActivityCatalog implements ActivityCatalog {
  private readonly definitions = new Map<string, ActivityDefinition>();

  constructor(definitions: ReadonlyArray<ActivityDefinition> = []) {
    for (const definition of definitions) this.register(definition);
  }

  /** Add or replace an activity. Throws on an empty id or title. */
  register(definition: ActivityDefinition): void {
    if (!definition.id.trim()) throw new Error("MapActivityCatalog: an activity needs an id");
    if (!definition.title.trim()) {
      throw new Error(`MapActivityCatalog: activity "${definition.id}" needs a title`);
    }
    const { factory, ...descriptor } = definition;
    this.definitions.set(definition.id, { ...structuredClone(descriptor), factory });
  }

  describe(): ActivityDescriptor[] {
    return [...this.definitions.values()]
      .map(toDescriptor)
      .sort(
        (a, b) => (a.category ?? "").localeCompare(b.category ?? "") || a.id.localeCompare(b.id),
      );
  }

  descriptor(id: string): ActivityDescriptor | undefined {
    const definition = this.definitions.get(id);
    return definition ? toDescriptor(definition) : undefined;
  }

  resolve(params: {
    ref: string;
    config?: Record<string, unknown>;
  }): (ctx: ActivityContext) => Eff<unknown, Throws<TaggedError>> {
    const definition = this.definitions.get(params.ref);
    if (!definition) {
      throw new Error(
        `Activity "${params.ref}" not found in catalog. Available: ${this.list().join(", ")}`,
      );
    }
    return definition.factory({ ...definition.defaultConfig, ...params.config });
  }

  has(ref: string): boolean {
    return this.definitions.has(ref);
  }

  list(): string[] {
    return [...this.definitions.keys()].sort();
  }
}

function toDescriptor(definition: ActivityDefinition): ActivityDescriptor {
  const { factory: _factory, ...descriptor } = definition;
  return structuredClone(descriptor);
}
