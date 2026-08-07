// ---------------------------------------------------------------------------
// WorkflowSchema — JSON IR for visual workflow editors
//
// The interchange format between a node-based UI and the runtime.
// Both the TypeScript DSL and a visual editor produce the same IR.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// JSON Schema (subset used for input/output type descriptions)
// ---------------------------------------------------------------------------

/**
 * JSON Schema subset for describing step input/output shapes.
 *
 * Used by visual editors to validate wiring between nodes and
 * auto-generate config forms. Fully serializable to JSON.
 *
 * @example
 * ```ts
 * const userSchema: JsonSchema = {
 *   type: "object",
 *   properties: {
 *     name: { type: "string" },
 *     age: { type: "number" },
 *   },
 *   required: ["name"],
 * };
 * ```
 */
export interface JsonSchema {
  type?: string;
  properties?: Record<string, JsonSchema>;
  items?: JsonSchema;
  required?: string[];
  enum?: unknown[];
  description?: string;
  default?: unknown;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// WorkflowSchema — the top-level IR
// ---------------------------------------------------------------------------

/**
 * JSON-serializable workflow definition. The interchange format between
 * a visual node-based editor (UI) and the runtime.
 *
 * A visual editor produces this JSON, which is then compiled into an
 * executable `WorkflowDefinition` via `compileWorkflow()`.
 *
 * @example
 * ```ts
 * const schema: WorkflowSchema = {
 *   version: 1,
 *   name: "analyze-website",
 *   inputSchema: { type: "object", properties: { url: { type: "string" } } },
 *   steps: [
 *     { type: "step", name: "scrape", dependsOn: [], activityRef: "http.get",
 *       config: { url: "{{input.url}}" } },
 *     { type: "step", name: "summarize", dependsOn: ["scrape"],
 *       activityRef: "ai.summarize" },
 *     { type: "step", name: "keywords", dependsOn: ["scrape"],
 *       activityRef: "ai.keywords" },
 *     { type: "step", name: "publish", dependsOn: ["summarize", "keywords"],
 *       activityRef: "db.insert", config: { table: "results" } },
 *   ],
 *   ui: {
 *     scrape: { x: 0, y: 100 },
 *     summarize: { x: 200, y: 0 },
 *     keywords: { x: 200, y: 200 },
 *     publish: { x: 400, y: 100 },
 *   },
 * };
 * ```
 */
export interface WorkflowSchema {
  /** Schema version for future migrations. */
  version: 1;
  /** Workflow name (must be unique). */
  name: string;
  /** JSON Schema describing the workflow input type. */
  inputSchema?: JsonSchema;
  /** Ordered list of step definitions forming the DAG. */
  steps: StepSchema[];
  /** Optional metadata for the UI (positions, colors, labels). */
  ui?: Record<string, NodeUiMeta>;
}

// ---------------------------------------------------------------------------
// StepSchema — individual step definitions
// ---------------------------------------------------------------------------

/**
 * A step in the workflow DAG. Either a single step or a fan-out map step.
 *
 * Use `type: "step"` for normal steps (including DAG steps with `dependsOn`).
 * Use `type: "map"` for fan-out over an array produced by another step.
 */
export type StepSchema =
  | SingleStepSchema
  | MapStepSchema
  | SleepStepSchema
  | SignalStepSchema
  | ApprovalStepSchema
  | BranchStepSchema
  | ParallelStepSchema;

/**
 * A single workflow step that executes one activity.
 *
 * Root steps (no dependencies) receive the workflow input.
 * DAG steps receive results from their dependencies.
 *
 * @example
 * ```ts
 * // Root step — receives workflow input
 * const fetchStep: SingleStepSchema = {
 *   type: "step",
 *   name: "fetch",
 *   dependsOn: [],
 *   activityRef: "http.get",
 *   config: { url: "https://api.example.com/data" },
 *   outputSchema: { type: "object", properties: { body: { type: "string" } } },
 * };
 *
 * // DAG step — depends on "fetch", receives its result
 * const processStep: SingleStepSchema = {
 *   type: "step",
 *   name: "process",
 *   dependsOn: ["fetch"],
 *   activityRef: "transform.uppercase",
 * };
 * ```
 */
export interface SingleStepSchema {
  type: "step";
  /** Unique step name (used in dependsOn references). */
  name: string;
  /** Dependencies — empty = root step, receives workflow input. */
  dependsOn: string[];
  /** Reference to the activity implementation (resolved at compile time via ActivityRegistry). */
  activityRef: string;
  /** Static config passed to the activity factory. */
  config?: Record<string, unknown>;
  /** Step-level options (retry, timeout). */
  options?: StepSchemaOptions;
  /** JSON Schema describing this step's output type (for UI wiring validation). */
  outputSchema?: JsonSchema;
}

/**
 * A fan-out map step that iterates over an array produced by another step.
 *
 * The `arrayFrom` field references a step whose output is an array.
 * The activity runs once per element, with optional concurrency control.
 *
 * @example
 * ```ts
 * const mapStep: MapStepSchema = {
 *   type: "map",
 *   name: "process-items",
 *   arrayFrom: "fetch-list",      // step that returns string[]
 *   activityRef: "transform.uppercase",
 *   options: { concurrency: 5 },
 *   outputSchema: { type: "string" },
 * };
 * ```
 */
export interface MapStepSchema {
  type: "map";
  /** Unique step name. */
  name: string;
  /** The step that produces the array to fan out over. */
  arrayFrom: string;
  /** Reference to the activity implementation. */
  activityRef: string;
  /** Static config passed to the activity factory. */
  config?: Record<string, unknown>;
  /** Step-level options (retry, timeout, concurrency). */
  options?: MapStepSchemaOptions;
  /** JSON Schema describing each element's output type. */
  outputSchema?: JsonSchema;
}

export interface SleepStepSchema {
  type: "sleep";
  name: string;
  dependsOn: string[];
  ms: number;
}

export interface SignalStepSchema {
  type: "signal";
  name: string;
  dependsOn: string[];
  signalName: string;
  timeoutMs?: number;
  outputSchema?: JsonSchema;
}

export interface ApprovalStepSchema {
  type: "approval";
  name: string;
  dependsOn: string[];
  signalName?: string;
  timeoutMs?: number;
  outputSchema?: JsonSchema;
}

export interface BranchStepSchema {
  type: "branch";
  name: string;
  dependsOn: string[];
  conditionRef: string;
  ifTrue: { activityRef: string; config?: Record<string, unknown> };
  ifFalse: { activityRef: string; config?: Record<string, unknown> };
  options?: StepSchemaOptions;
  outputSchema?: JsonSchema;
}

export interface ParallelBranchSchema {
  activityRef: string;
  config?: Record<string, unknown>;
}

export interface ParallelStepSchema {
  type: "parallel";
  name: string;
  dependsOn: string[];
  branches: Record<string, ParallelBranchSchema>;
  options?: StepSchemaOptions;
  outputSchema?: JsonSchema;
}

// ---------------------------------------------------------------------------
// Step options (serializable subset of StepOptions)
// ---------------------------------------------------------------------------

/**
 * Serializable step options for retry and timeout behavior.
 *
 * @example
 * ```ts
 * const options: StepSchemaOptions = {
 *   retry: { maxRetries: 3, baseDelayMs: 1000 },
 *   timeoutMs: 30_000,
 * };
 * ```
 */
export interface StepSchemaOptions {
  /** Retry policy — exponential backoff on failure. */
  retry?: { maxRetries?: number; baseDelayMs?: number };
  /** Maximum execution time in milliseconds. */
  timeoutMs?: number;
}

/**
 * Step options for map steps, adding concurrency control.
 *
 * @example
 * ```ts
 * const options: MapStepSchemaOptions = {
 *   concurrency: 10,
 *   retry: { maxRetries: 2 },
 *   timeoutMs: 60_000,
 * };
 * ```
 */
export interface MapStepSchemaOptions extends StepSchemaOptions {
  /** Max parallel executions for the fan-out. */
  concurrency?: number;
}

// ---------------------------------------------------------------------------
// UI metadata (not used by the runtime)
// ---------------------------------------------------------------------------

/**
 * Visual metadata for a workflow node in the editor.
 * Stored alongside the schema but ignored by the runtime.
 *
 * @example
 * ```ts
 * const meta: NodeUiMeta = {
 *   x: 200,
 *   y: 100,
 *   width: 180,
 *   color: "#4a90d9",
 *   label: "Fetch User Data",
 * };
 * ```
 */
export interface NodeUiMeta {
  /** X position on the canvas. */
  x: number;
  /** Y position on the canvas. */
  y: number;
  /** Node width in pixels. */
  width?: number;
  /** Node color (CSS color string). */
  color?: string;
  /** Display label (overrides step name). */
  label?: string;
}
