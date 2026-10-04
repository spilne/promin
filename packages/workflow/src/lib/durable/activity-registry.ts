// ---------------------------------------------------------------------------
// ActivityRegistry — maps activity names to step implementations
//
// The visual editor can't embed TypeScript lambdas. Instead, steps reference
// activities by name (activityRef). The registry resolves names to functions
// at compile time.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../shared/tagged-error.ts";

// ---------------------------------------------------------------------------
// Activity types
// ---------------------------------------------------------------------------

/**
 * Context passed to an activity when executed as part of a workflow step.
 *
 * Activities receive this unified context regardless of how the step is wired
 * (linear chain, DAG dependencies, or fan-out map).
 *
 * @example
 * ```ts
 * // In a linear chain: ctx.prev is the previous step's result
 * const uppercase: ActivityFactory = () => (ctx) =>
 *   succeed(String(ctx.prev).toUpperCase());
 *
 * // In a DAG: ctx.deps has named results from dependencies
 * const combine: ActivityFactory = () => (ctx) => {
 *   const { summary, keywords } = ctx.deps as { summary: string; keywords: string[] };
 *   return succeed({ summary, keywords });
 * };
 *
 * // Access workflow input (always available)
 * const greet: ActivityFactory = () => (ctx) =>
 *   succeed(`Hello, ${(ctx.input as { name: string }).name}!`);
 * ```
 */
export interface ActivityContext {
  /** The workflow input (always available). */
  readonly input: unknown;
  /**
   * Previous step result.
   * - Linear mode: output of the previous step.
   * - DAG mode with single dep: that dependency's result.
   * - DAG mode with multiple deps: the entire `deps` object.
   */
  readonly prev: unknown;
  /** Dependency results keyed by step name (DAG mode). Empty object for linear/root steps. */
  readonly deps: Record<string, unknown>;
  /** Current workflow ID. */
  readonly workflowId: string;
  /** Execution attempt number (1 on first try, 2+ on retries). */
  readonly attempt: number;
}

/**
 * An activity factory — receives static config from the schema and returns
 * a step function that takes `ActivityContext` and returns an `Eff`.
 *
 * The two-level function allows config to be "baked in" at compile time
 * while the step function runs at execution time with runtime context.
 *
 * @example
 * ```ts
 * // Simple transform (no config)
 * const uppercase: ActivityFactory = () => (ctx) =>
 *   succeed(String(ctx.prev).toUpperCase());
 *
 * // HTTP call with config
 * const httpGet: ActivityFactory = (config) => (ctx) =>
 *   httpClient.get({ url: config?.url as string });
 *
 * // DB insert with table from config
 * const dbInsert: ActivityFactory = (config) => (ctx) =>
 *   tryPromise(() => db.insert(config?.table as string, ctx.prev), toDbError);
 *
 * // AI call with model from config
 * const aiSummarize: ActivityFactory = (config) => (ctx) =>
 *   aiClient.complete({
 *     model: (config?.model as string) ?? "gpt-4",
 *     prompt: `Summarize: ${ctx.prev}`,
 *   });
 * ```
 */
export type ActivityFactory = (
  config?: Record<string, unknown>,
) => (ctx: ActivityContext) => Eff<unknown, Throws<TaggedError>>;

// ---------------------------------------------------------------------------
// ActivityRegistry interface
// ---------------------------------------------------------------------------

/**
 * Registry that maps activity names to their implementations.
 *
 * Steps in a `WorkflowSchema` reference activities by name (`activityRef`).
 * The compiler resolves these references via the registry at compile time.
 *
 * Implement this interface for custom registry behavior (e.g., lazy loading,
 * plugin systems). For most cases, use `MapActivityRegistry`.
 *
 * @example
 * ```ts
 * // Custom registry that loads activities from a plugin directory
 * class PluginRegistry implements ActivityRegistry {
 *   resolve({ ref, config }: { ref: string; config?: Record<string, unknown> }) {
 *     const plugin = loadPlugin(ref);
 *     return plugin.createActivity(config);
 *   }
 *   has(ref: string) { return pluginExists(ref); }
 *   list() { return listPlugins(); }
 * }
 * ```
 */
export interface ActivityRegistry {
  /** Resolve an activity reference to a step function. Throws if not found. */
  resolve(params: {
    ref: string;
    config?: Record<string, unknown>;
  }): (ctx: ActivityContext) => Eff<unknown, Throws<TaggedError>>;

  /** Check if an activity reference exists in the registry. */
  has(ref: string): boolean;

  /** List all registered activity names. */
  list(): string[];
}

// ---------------------------------------------------------------------------
// MapActivityRegistry — simple Map-based implementation
// ---------------------------------------------------------------------------

/**
 * A simple Map-based activity registry. The default implementation for
 * wiring activity names to their implementations.
 *
 * Pass a record of `{ name: factory }` pairs to the constructor.
 * Activities can also be added at runtime via `register()`.
 *
 * @example
 * ```ts
 * import { succeed, tryPromise } from "@spilne/perfect-core";
 * import { MapActivityRegistry } from "@promin/workflow";
 *
 * const registry = new MapActivityRegistry({
 *   // Simple transform
 *   "transform.uppercase": () => (ctx) =>
 *     succeed(String(ctx.prev).toUpperCase()),
 *
 *   // HTTP call with config
 *   "http.get": (config) => (ctx) =>
 *     httpClient.get({ url: config?.url as string }),
 *
 *   // DB insert with table from config
 *   "db.insert": (config) => (ctx) =>
 *     tryPromise(() => db.insert(config?.table as string, ctx.prev), toDbError),
 *
 *   // Fan-out element processor
 *   "email.send": (config) => (ctx) =>
 *     tryPromise(
 *       () => mailer.send({ template: config?.template as string, to: ctx.prev }),
 *       toMailError,
 *     ),
 * });
 *
 * // Use with compileWorkflow
 * const definition = compileWorkflow({ schema, storage, registry });
 *
 * // Add activities at runtime (e.g., from plugins)
 * registry.register("custom.activity", (config) => (ctx) =>
 *   succeed({ custom: true }),
 * );
 * ```
 */
export class MapActivityRegistry implements ActivityRegistry {
  private readonly activities: Map<string, ActivityFactory>;

  constructor(activities: Record<string, ActivityFactory>) {
    this.activities = new Map(Object.entries(activities));
  }

  resolve(params: {
    ref: string;
    config?: Record<string, unknown>;
  }): (ctx: ActivityContext) => Eff<unknown, Throws<TaggedError>> {
    const { ref, config } = params;
    const factory = this.activities.get(ref);
    if (!factory) {
      throw new Error(
        `Activity "${ref}" not found in registry. Available: ${[...this.activities.keys()].join(", ")}`,
      );
    }
    return factory(config);
  }

  has(ref: string): boolean {
    return this.activities.has(ref);
  }

  list(): string[] {
    return [...this.activities.keys()];
  }

  /**
   * Register a new activity at runtime.
   *
   * @example
   * ```ts
   * registry.register({
   *   ref: "slack.send",
   *   factory: (config) => (ctx) =>
   *     tryPromise(
   *       () => slack.postMessage({ channel: config?.channel as string, text: String(ctx.prev) }),
   *       toSlackError,
   *     ),
   * });
   * ```
   */
  register(params: { ref: string; factory: ActivityFactory }): void {
    this.activities.set(params.ref, params.factory);
  }
}
