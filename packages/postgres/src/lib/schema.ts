// ---------------------------------------------------------------------------
// Drizzle schema for durable workflow storage
// ---------------------------------------------------------------------------

import {
  pgTable,
  text,
  integer,
  jsonb,
  timestamp,
  bigint,
  bigserial,
  boolean,
  check,
  doublePrecision,
  index,
  uniqueIndex,
  primaryKey,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createLookupTable, type LookupBinding } from "./lookup-table.ts";
import {
  WorkflowStatusIds,
  StepStatusIds,
  StepTypeIds,
  AttemptTypeIds,
} from "./workflow-lookups.ts";

// ---------------------------------------------------------------------------
// Lookup tables
// ---------------------------------------------------------------------------

export const workflowStatusTable = createLookupTable("wf_workflow_status");
export const stepStatusTable = createLookupTable("wf_step_status");
export const stepTypeTable = createLookupTable("wf_step_type");
export const attemptTypeTable = createLookupTable("wf_attempt_type");

// ---------------------------------------------------------------------------
// Lookup bindings — for seeding and validation
// ---------------------------------------------------------------------------

export const LOOKUP_BINDINGS: LookupBinding[] = [
  { lookup: WorkflowStatusIds, table: workflowStatusTable },
  { lookup: StepStatusIds, table: stepStatusTable },
  { lookup: StepTypeIds, table: stepTypeTable },
  { lookup: AttemptTypeIds, table: attemptTypeTable },
];

// ---------------------------------------------------------------------------
// Core tables
// ---------------------------------------------------------------------------

export const workflows = pgTable(
  "wf_workflows",
  {
    workflowId: text("workflow_id").primaryKey(),
    workflowName: text("workflow_name").notNull(),
    workflowType: text("workflow_type"),
    namespace: text("namespace"),
    version: text("version"),
    run: integer("run").notNull().default(1),
    statusId: integer("status_id").notNull().default(WorkflowStatusIds.id.pending),
    input: jsonb("input").notNull(),
    metadata: jsonb("metadata"),
    result: jsonb("result"),
    error: text("error"),
    // Structured reason attached when the workflow ended via a `.tripwire()`
    // step. Present only for `status_id = tripwire (6)`; null otherwise.
    tripwire: jsonb("tripwire"),
    // Per-call idempotency key — caller-supplied dedup token that resolves
    // to this workflow_id while unexpired. Lets auto-minted ids dedup
    // without the caller knowing them ahead of time.
    idempotencyKey: text("idempotency_key"),
    idempotencyExpiresAt: timestamp("idempotency_expires_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [
    index("wf_workflows_name_status_idx").on(t.workflowName, t.statusId),
    index("wf_workflows_status_idx").on(t.statusId),
    index("wf_workflows_type_idx").on(t.workflowType),
    index("wf_workflows_namespace_idx").on(t.namespace),
    uniqueIndex("wf_workflows_idempotency_key_idx")
      .on(sql`COALESCE(${t.namespace}, '')`, t.workflowName, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
  ],
);

export const workflowRuns = pgTable(
  "wf_workflow_runs",
  {
    workflowId: text("workflow_id")
      .notNull()
      .references(() => workflows.workflowId, { onDelete: "cascade" }),
    run: integer("run").notNull(),
    statusId: integer("status_id").notNull(),
    result: jsonb("result"),
    error: text("error"),
    // Tripwire reason preserved from the archived run. Null unless the
    // archived run ended via .tripwire().
    tripwire: jsonb("tripwire"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [primaryKey({ columns: [t.workflowId, t.run] })],
);

export const workflowSteps = pgTable(
  "wf_workflow_steps",
  {
    workflowId: text("workflow_id")
      .notNull()
      .references(() => workflows.workflowId, { onDelete: "cascade" }),
    stepName: text("step_name").notNull(),
    run: integer("run").notNull().default(1),
    statusId: integer("status_id").notNull().default(StepStatusIds.id.pending),
    stepTypeId: integer("step_type_id").notNull().default(StepTypeIds.id.single),
    dependsOn: jsonb("depends_on").$type<string[]>().notNull().default([]),
    result: jsonb("result"),
    error: text("error"),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    durationMs: bigint("duration_ms", { mode: "number" }),
    attempt: integer("attempt").notNull().default(0),
    wakeAt: timestamp("wake_at", { withTimezone: true }),
    signalName: text("signal_name"),
    signalTimeoutAt: timestamp("signal_timeout_at", { withTimezone: true }),
    // JSON Schema snapshot the suspend point waited on. Written by
    // `ctx.validatedSignal` / `ctx.approval` from `sig.schema.jsonSchema`;
    // read by the server's signal-delivery path to validate inbound
    // payloads before calling `deliverSignal`. Null for plain
    // `ctx.signal()` callers (pass-through, no validation).
    signalJsonSchema: jsonb("signal_json_schema"),
    // Step-kind-specific audit data (e.g. `.match()` writes the chosen
    // case). Opaque JSON; queryable with `metadata->>'<key>'`. Null for
    // steps that don't produce audit data.
    metadata: jsonb("metadata"),
  },
  (t) => [primaryKey({ columns: [t.workflowId, t.stepName, t.run] })],
);

export const workflowStepTasks = pgTable(
  "wf_workflow_step_tasks",
  {
    workflowId: text("workflow_id").notNull(),
    stepName: text("step_name").notNull(),
    run: integer("run").notNull().default(1),
    taskIndex: integer("task_index").notNull(),
    statusId: integer("status_id").notNull().default(StepStatusIds.id.pending),
    input: jsonb("input"),
    result: jsonb("result"),
    error: text("error"),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    attempt: integer("attempt").notNull().default(0),
  },
  (t) => [primaryKey({ columns: [t.workflowId, t.stepName, t.run, t.taskIndex] })],
);

export const workflowSignals = pgTable(
  "wf_workflow_signals",
  {
    workflowId: text("workflow_id")
      .notNull()
      .references(() => workflows.workflowId, { onDelete: "cascade" }),
    signalName: text("signal_name").notNull(),
    payload: jsonb("payload").notNull(),
    deliveredAt: timestamp("delivered_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index("wf_signals_workflow_idx").on(t.workflowId),
    uniqueIndex("wf_signals_workflow_signal_idx").on(t.workflowId, t.signalName),
  ],
);

export const workflowLocks = pgTable("wf_workflow_locks", {
  workflowId: text("workflow_id").primaryKey(),
  lockedAt: timestamp("locked_at", { withTimezone: true }).notNull().defaultNow(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
  lockedBy: text("locked_by"),
  /**
   * Monotonic fence token. Bumped on every successful `tryLock`, carried by
   * subsequent mutating calls, validated against this row before the write
   * commits. Lets a new holder safely take over after the previous one's
   * lock expired without risk of the stale holder committing late writes.
   */
  fenceToken: bigserial("fence_token", { mode: "number" }).notNull(),
});

export const stepQueue = pgTable(
  "wf_step_queue",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    workflowId: text("workflow_id").notNull(),
    stepName: text("step_name").notNull(),
    namespace: text("namespace"),
    // Capabilities this task requires (empty = any worker). Workers claim
    // tasks where `needs <@ capabilities`. Replaces the old `queue` column.
    needs: text("needs").array().notNull().default([]),
    priority: integer("priority").notNull().default(5),
    input: jsonb("input"),
    prevResults: jsonb("prev_results"),
    attempt: integer("attempt").notNull().default(1),
    status: text("status").notNull().default("pending"),
    result: jsonb("result"),
    error: text("error"),
    durationMs: bigint("duration_ms", { mode: "number" }),
    claimedBy: text("claimed_by"),
    claimedAt: timestamp("claimed_at", { withTimezone: true }),
    claimToken: text("claim_token"),
    heartbeatAt: timestamp("heartbeat_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    version: text("version"),
    // Search-attribute payload — parity with wf_workflows.metadata. Never
    // read by the platform for control flow; consumer-namespaced. No GIN
    // index added speculatively; lift to one when a query path becomes hot.
    metadata: jsonb("metadata"),
    // Per-task concurrency keys — caps how many tasks with the same
    // `(concurrency_scope, concurrency_key)` can be `running` at once.
    // `concurrency_limit` is the cap; null on any of the three disables
    // enforcement for this task.
    concurrencyKey: text("concurrency_key"),
    concurrencyScope: text("concurrency_scope"),
    concurrencyLimit: integer("concurrency_limit"),
  },
  (t) => [
    index("wf_step_queue_dequeue_idx").on(t.status, t.priority, t.createdAt),
    index("wf_step_queue_workflow_idx").on(t.workflowId),
    index("wf_step_queue_namespace_idx").on(t.namespace),
    // Partial unique index — enqueue dedupes on (workflow_id, step_name)
    // while a prior task is still pending or running. Terminal rows stay
    // outside the predicate so step retries + startFreshRun() keep
    // working. `ensureTable` and migration 0019 both create this.
    uniqueIndex("wf_step_queue_active_uniq")
      .on(t.workflowId, t.stepName)
      .where(sql`${t.status} IN ('pending', 'running')`),
    // GIN index powers `needs <@ capabilities` subset filter on claim.
    // Partial on status='pending' — claim only reads pending rows.
    index("wf_step_queue_needs_idx")
      .using("gin", t.needs)
      .where(sql`${t.status} = 'pending'`),
    // Hot path for the concurrency-count subquery during claim.
    index("wf_step_queue_concurrency_running_idx")
      .on(t.concurrencyScope, t.concurrencyKey)
      .where(sql`${t.status} = 'running' AND ${t.concurrencyKey} IS NOT NULL`),
  ],
);

export const stepAttempts = pgTable(
  "wf_step_attempts",
  {
    workflowId: text("workflow_id").notNull(),
    stepName: text("step_name").notNull(),
    run: integer("run").notNull().default(1),
    attempt: integer("attempt").notNull(),
    attemptTypeId: integer("attempt_type_id").notNull(),
    statusId: integer("status_id").notNull(),
    result: jsonb("result"),
    error: text("error"),
    durationMs: bigint("duration_ms", { mode: "number" }),
    startedAt: timestamp("started_at", { withTimezone: true }).notNull(),
    completedAt: timestamp("completed_at", { withTimezone: true }).notNull(),
    workerId: text("worker_id"),
  },
  (t) => [
    primaryKey({ columns: [t.workflowId, t.stepName, t.run, t.attempt, t.attemptTypeId] }),
    index("wf_step_attempts_workflow_idx").on(t.workflowId),
  ],
);

// Activity journal for .journaled() steps — one row per ctx.activity /
// ctx.sleep / ctx.signal invocation. stepType/phase/wakeAt support
// durable sleep and signal suspension alongside completed activity entries.
export const activityJournal = pgTable(
  "wf_activity_journal",
  {
    workflowId: text("workflow_id").notNull(),
    stepName: text("step_name").notNull(),
    activityIndex: integer("activity_index").notNull(),
    // `branch_path` is the position of this entry inside a `ctx.parallel`
    // tree. `""` means "at top-level in the body" — the default, matching
    // every pre-parallel workflow. Parallel branches encode as `"0"`,
    // `"1.0"`, `"2.3"` for nested structure. Included in the PK so
    // concurrent branches at the same `activity_index` don't collide.
    branchPath: text("branch_path").notNull().default(""),
    activityName: text("activity_name").notNull(),
    stepType: text("step_type").notNull().default("activity"),
    phase: text("phase").notNull().default("completed"),
    // Canonicalized-+-hashed fingerprint of the activity input, opt-in via
    // `ActivityOptions.payloadHash` or pipeline-level `payloadHash: true`.
    // NULL when hashing was never requested.
    payloadHash: text("payload_hash"),
    wakeAt: timestamp("wake_at", { withTimezone: true }),
    exit: jsonb("exit"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workflowId, t.stepName, t.activityIndex, t.branchPath] }),
    index("wf_activity_journal_step_idx").on(t.workflowId, t.stepName),
  ],
);

// ---------------------------------------------------------------------------
// Workflow definition registry — stores serialized WorkflowDAG entries
// so coordinators can resolve definitions from a shared store.
// ---------------------------------------------------------------------------

export const workflowRegistry = pgTable(
  "wf_workflow_registry",
  {
    name: text("name").notNull(),
    version: text("version").notNull(),
    dagJson: jsonb("dag_json").notNull(),
    idempotency: jsonb("idempotency"),
    // Lifecycle status — `inactive` (registered but not the chosen one),
    // `active` (current target for `findActive(name)`), `archived`
    // (rolled-back / drained). Partial unique index enforces
    // at-most-one-active per name.
    status: text("status").notNull().default("inactive"),
    activeAt: timestamp("active_at", { withTimezone: true }),
    archivedAt: timestamp("archived_at", { withTimezone: true }),
    contentHash: text("content_hash"),
    registeredAt: timestamp("registered_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.name, t.version] }),
    index("wf_workflow_registry_name_idx").on(t.name),
    uniqueIndex("wf_workflow_registry_active_uniq")
      .on(t.name)
      .where(sql`${t.status} = 'active'`),
    uniqueIndex("wf_workflow_registry_content_hash_uniq")
      .on(t.name, t.contentHash)
      .where(sql`${t.contentHash} IS NOT NULL`),
  ],
);

// ---------------------------------------------------------------------------
// Worker registry — persisted view of active workers across the fleet so a
// horizontally-scaled Zorya (or any other observer) sees the same worker
// set from every instance. Workers heartbeat into the same table; readers
// (ops dashboards, capability-based dispatchers) query it. InMemory version
// stays available for single-process deployments.
// ---------------------------------------------------------------------------

export const workerRegistry = pgTable(
  "wf_worker_registry",
  {
    workerId: text("worker_id").primaryKey(),
    status: text("status").notNull().default("active"), // active | draining | dead | retired
    capabilities: text("capabilities").array().notNull().default([]),
    concurrency: integer("concurrency").notNull().default(1),
    metadata: jsonb("metadata"),
    startedAt: timestamp("started_at", { withTimezone: true }).notNull().defaultNow(),
    lastHeartbeatAt: timestamp("last_heartbeat_at", { withTimezone: true }).notNull().defaultNow(),
    // Set by deregister() on a graceful stop; the row is kept for the
    // retention window, then reaped by gc(). NULL for non-retired rows.
    retiredAt: timestamp("retired_at", { withTimezone: true }),
  },
  (t) => [
    // Hot path: detectDead scans stale heartbeats among non-dead rows.
    index("wf_worker_registry_heartbeat_idx").on(t.lastHeartbeatAt),
    // Capability-aware lookups ('which workers can do summarize?').
    index("wf_worker_registry_caps_idx").using("gin", t.capabilities),
    // Status-filtered list() queries.
    index("wf_worker_registry_status_idx").on(t.status),
  ],
);

// ---------------------------------------------------------------------------
// Workflow advertisements — workers publish their workflow definitions
// (name, version, DAG steps, optional sample input) on connect.
// Postgres-backed so multi-replica deployments share one catalog;
// readers populate dashboards + dispatch lookups via `distinct()`.
// ---------------------------------------------------------------------------

export const workflowAdvertisements = pgTable(
  "wf_workflow_advertisements",
  {
    workerId: text("worker_id").notNull(),
    workflowName: text("workflow_name").notNull(),
    // Nullable version is part of the conceptual key; we expose it via a
    // partial unique index pair below (the row PK is a composite).
    version: text("version"),
    // JSON payload of the AdvertisedWorkflow.steps array — kept as jsonb
    // for flexible querying and forward-compat (steps gain new fields).
    steps: jsonb("steps").notNull(),
    sampleInput: jsonb("sample_input"),
    advertisedAt: timestamp("advertised_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    // Composite uniqueness: a worker can advertise one (name, version)
    // pair at a time. Two indexes because Postgres treats NULL as
    // distinct in unique constraints — we want NULL versions to also
    // dedupe per (worker, name).
    uniqueIndex("wf_workflow_advertisements_worker_name_version_idx")
      .on(t.workerId, t.workflowName, t.version)
      .where(sql`${t.version} IS NOT NULL`),
    uniqueIndex("wf_workflow_advertisements_worker_name_nullver_idx")
      .on(t.workerId, t.workflowName)
      .where(sql`${t.version} IS NULL`),
    // Dashboard's "show every workflow" + dispatch's "find by name"
    // both pivot on (workflow_name, version).
    index("wf_workflow_advertisements_workflow_idx").on(t.workflowName, t.version),
  ],
);

// ---------------------------------------------------------------------------
// Workflow start queue — pending start-workflow requests that a worker
// must claim. Used by the queued / split deployment shape: dashboard
// triggers create the storage row + push a start onto this queue;
// workflow-mode workers poll, claim, and execute. Postgres-backed for
// multi-replica deployments where the dashboard server doesn't run
// workflows in-process.
// ---------------------------------------------------------------------------

export const workflowStarts = pgTable(
  "wf_workflow_starts",
  {
    id: text("id").primaryKey(),
    workflowId: text("workflow_id").notNull(),
    workflowName: text("workflow_name").notNull(),
    namespace: text("namespace"),
    version: text("version"),
    input: jsonb("input").notNull(),
    metadata: jsonb("metadata"),
    enqueuedAt: timestamp("enqueued_at", { withTimezone: true }).notNull().defaultNow(),
    claimedAt: timestamp("claimed_at", { withTimezone: true }),
    claimedBy: text("claimed_by"),
    // status: 'pending' (waiting for a worker) | 'claimed' (in flight).
    // Completion deletes the row to keep the queue tight.
    status: text("status").notNull().default("pending"),
  },
  (t) => [
    // Hot path: workers claim by (workflow_name, enqueued_at) over the
    // pending partition only. Partial index keeps the scan tight even
    // when the queue accumulates inflight + completed history.
    index("wf_workflow_starts_pending_idx")
      .on(t.workflowName, t.enqueuedAt)
      .where(sql`${t.status} = 'pending'`),
    // Stale-claim sweeper: find rows past the reclaim window.
    index("wf_workflow_starts_claimed_idx")
      .on(t.claimedAt)
      .where(sql`${t.status} = 'claimed'`),
  ],
);

// ---------------------------------------------------------------------------
// State machine tables
// ---------------------------------------------------------------------------

export const machines = pgTable(
  "sm_machines",
  {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    machineType: text("machine_type"),
    namespace: text("namespace"),
    current: text("current_state").notNull(),
    context: jsonb("context").notNull(),
    version: text("version"),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index("sm_machines_name_idx").on(t.name),
    index("sm_machines_current_idx").on(t.current),
    index("sm_machines_type_idx").on(t.machineType),
    index("sm_machines_namespace_idx").on(t.namespace),
  ],
);

export const machineEvents = pgTable(
  "sm_machine_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    machineId: text("machine_id")
      .notNull()
      .references(() => machines.id, { onDelete: "cascade" }),
    event: text("event").notNull(),
    fromState: text("from_state").notNull(),
    toState: text("to_state").notNull(),
    context: jsonb("context").notNull(),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index("sm_machine_events_machine_idx").on(t.machineId)],
);

// ---------------------------------------------------------------------------
// Public-bearer signal tokens — authorization sidecar for deliverSignal.
//
// A signal token grants one-shot delivery rights to a public completer for
// a specific (workflow_id, signal_name). The completion route validates
// the bearer, then calls `storage.deliverSignal(workflow_id, signal_name,
// value)` to resume the workflow through the existing signal mechanic —
// no new suspend semantics.
// ---------------------------------------------------------------------------

export const signalTokens = pgTable(
  "wf_signal_tokens",
  {
    tokenId: text("token_id").primaryKey(),
    workflowId: text("workflow_id").notNull(),
    signalName: text("signal_name").notNull(),
    bearer: text("bearer").notNull(),
    tags: text("tags")
      .array()
      .notNull()
      .default(sql`'{}'::text[]`),
    idempotencyKey: text("idempotency_key"),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    completedValue: jsonb("completed_value"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("wf_signal_tokens_idempotency_key_idx")
      .on(t.workflowId, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
    index("wf_signal_tokens_workflow_idx").on(t.workflowId),
    index("wf_signal_tokens_expired_idx")
      .on(t.expiresAt)
      .where(sql`${t.completedAt} IS NULL`),
  ],
);

// ---------------------------------------------------------------------------
// Generic typed streams — bidirectional append-only channels per workflow.
// Output (workflow → subscribers) and input (subscribers → workflow) share
// this table; direction is convention at the API layer.
// ---------------------------------------------------------------------------

export const workflowStreams = pgTable(
  "wf_streams",
  {
    workflowId: text("workflow_id").notNull(),
    streamId: text("stream_id").notNull(),
    chunkIndex: integer("chunk_index").notNull(),
    payload: jsonb("payload").notNull(),
    appendedBy: text("appended_by").notNull().default("workflow"),
    appendedAt: timestamp("appended_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workflowId, t.streamId, t.chunkIndex] }),
    index("wf_streams_workflow_idx").on(t.workflowId),
    index("wf_streams_workflow_stream_idx").on(t.workflowId, t.streamId, t.chunkIndex),
  ],
);

// ---------------------------------------------------------------------------
// Agent registry — versioned recipe store. Mirrors the shape of
// SqliteAgentRegistry: one row per (id, version) tuple. Backend and
// metadata are stored as JSONB so new backend variants land without
// schema migrations. The `id` column is `agent_id` to keep the column
// names readable when joined against future agent_* tables.
// ---------------------------------------------------------------------------

export const agentRegistry = pgTable(
  "agent_registry",
  {
    agentId: text("agent_id").notNull(),
    version: text("version").notNull(),
    backendType: text("backend_type").notNull(),
    backend: jsonb("backend").notNull(),
    metadata: jsonb("metadata").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.agentId, t.version] }),
    // get(id) without version — pick latest by updated_at
    index("agent_registry_id_updated_idx").on(t.agentId, t.updatedAt),
    // list({ backendType }) filter
    index("agent_registry_backend_type_idx").on(t.backendType),
  ],
);

// ---------------------------------------------------------------------------
// Role registry — versioned behavioral-bundle store. Sibling of
// agent_registry: one row per (role_id, version). The behavioral
// `definition` (persona prompt + tools + skills + capabilities) and the
// `metadata` (description / tags / suggestedSecrets) are JSONB so the shapes
// evolve without schema migrations. An agent binds a role; see
// ROLE_AGENT_MODEL.
// ---------------------------------------------------------------------------

export const roleRegistry = pgTable(
  "role_registry",
  {
    roleId: text("role_id").notNull(),
    version: text("version").notNull(),
    definition: jsonb("definition").notNull(),
    metadata: jsonb("metadata").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.roleId, t.version] }),
    // get(id) without version — pick latest by updated_at
    index("role_registry_id_updated_idx").on(t.roleId, t.updatedAt),
  ],
);

// ---------------------------------------------------------------------------
// Skill registry — versioned instruction-block store. Sibling of
// agent_registry: one row per (skill_id, version). Content fields
// (description / when_to_use / body) are columns; metadata (capabilities /
// tags / enabled) is a JSONB blob.
// ---------------------------------------------------------------------------

export const skillRegistry = pgTable(
  "skill_registry",
  {
    skillId: text("skill_id").notNull(),
    version: text("version").notNull(),
    description: text("description").notNull(),
    whenToUse: text("when_to_use").notNull(),
    body: text("body").notNull(),
    metadata: jsonb("metadata").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.skillId, t.version] }),
    // get(id) without version — pick latest by updated_at
    index("skill_registry_id_updated_idx").on(t.skillId, t.updatedAt),
  ],
);

// ---------------------------------------------------------------------------
// Fragment store — durable storage for operator-authored prompt fragments.
// Sibling of skill_registry. File-scanned fragments live in memory only and
// are NOT persisted here (the .md file is the source of truth).
// ---------------------------------------------------------------------------

export const fragmentStore = pgTable("fragment_store", {
  key: text("key").primaryKey(),
  content: text("content").notNull(),
  updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
});

// ---------------------------------------------------------------------------
// Zorya namespace entity — authoritative namespace lifecycle/policy registry.
// This is intentionally separate from agent_namespace, which stores memory
// configuration for a namespace.
// ---------------------------------------------------------------------------

export const zoryaNamespace = pgTable(
  "zorya_namespace",
  {
    id: text("id").primaryKey(),
    displayName: text("display_name").notNull(),
    description: text("description"),
    status: text("status").notNull().default("active"),
    capabilities: jsonb("capabilities")
      .notNull()
      .default(sql`'{}'::jsonb`),
    metadata: jsonb("metadata")
      .notNull()
      .default(sql`'{}'::jsonb`),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    check("zorya_namespace_status_check", sql`${t.status} IN ('active', 'archived')`),
    index("zorya_namespace_status_display_idx").on(t.status, t.displayName, t.id),
  ],
);

// ---------------------------------------------------------------------------
// Agent memory — three-scope cascade × four-tier model. Mirrors
// SqliteMemoryStore's 6 tables. Single-table-per-tier with a `scope`
// column keeps queries simple while supporting per-scope indexes.
// All timestamps are millisecond unix epochs (BIGINT) for clock-parity
// with the SQLite implementation, so the same conformance suite passes.
// ---------------------------------------------------------------------------

export const agentNamespace = pgTable("agent_namespace", {
  namespaceId: text("namespace_id").primaryKey(),
  staticRules: text("static_rules"),
  workingMemory: text("working_memory"),
  inheritFromParent: boolean("inherit_from_parent").notNull().default(true),
  metadata: jsonb("metadata"),
  createdAt: bigint("created_at", { mode: "number" }).notNull(),
  updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
});

export const agentResource = pgTable(
  "agent_resource",
  {
    namespaceId: text("namespace_id").notNull(),
    resourceId: text("resource_id").notNull(),
    staticRules: text("static_rules"),
    workingMemory: text("working_memory"),
    inheritFromParent: boolean("inherit_from_parent").notNull().default(true),
    metadata: jsonb("metadata"),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [primaryKey({ columns: [t.namespaceId, t.resourceId] })],
);

export const agentThread = pgTable(
  "agent_thread",
  {
    namespaceId: text("namespace_id").notNull(),
    threadId: text("thread_id").notNull(),
    resourceId: text("resource_id"),
    title: text("title"),
    workingMemory: text("working_memory"),
    inheritFromParent: boolean("inherit_from_parent").notNull().default(true),
    metadata: jsonb("metadata"),
    archivedAt: bigint("archived_at", { mode: "number" }),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.namespaceId, t.threadId] }),
    index("agent_thread_resource_idx").on(t.namespaceId, t.resourceId),
  ],
);

export const agentFact = pgTable(
  "agent_fact",
  {
    id: text("id").primaryKey(),
    scope: text("scope").notNull(), // 'namespace' | 'resource' | 'thread'
    namespaceId: text("namespace_id").notNull(),
    resourceId: text("resource_id"),
    threadId: text("thread_id"),
    factText: text("text").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    index("agent_fact_ns_idx").on(t.scope, t.namespaceId, t.createdAt),
    index("agent_fact_res_idx").on(t.scope, t.namespaceId, t.resourceId, t.createdAt),
    index("agent_fact_thr_idx").on(t.scope, t.namespaceId, t.threadId, t.createdAt),
  ],
);

export const agentEpisode = pgTable(
  "agent_episode",
  {
    id: text("id").primaryKey(),
    scope: text("scope").notNull(),
    namespaceId: text("namespace_id").notNull(),
    resourceId: text("resource_id"),
    threadId: text("thread_id"),
    summary: text("summary").notNull(),
    outcome: text("outcome"),
    salience: doublePrecision("salience").notNull().default(0.5),
    embedding: jsonb("embedding"),
    sourceThreadId: text("source_thread_id"),
    sourceMsgFromSeq: integer("source_msg_from_seq"),
    sourceMsgToSeq: integer("source_msg_to_seq"),
    occurredAt: bigint("occurred_at", { mode: "number" }).notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    metadata: jsonb("metadata"),
  },
  (t) => [
    index("agent_episode_ns_idx").on(t.scope, t.namespaceId, t.salience),
    index("agent_episode_res_idx").on(t.scope, t.namespaceId, t.resourceId, t.salience),
    index("agent_episode_thr_idx").on(t.scope, t.namespaceId, t.threadId, t.createdAt),
  ],
);

export const agentMessage = pgTable(
  "agent_message",
  {
    namespaceId: text("namespace_id").notNull(),
    threadId: text("thread_id").notNull(),
    seq: integer("seq").notNull(),
    payload: jsonb("payload").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
  },
  (t) => [primaryKey({ columns: [t.namespaceId, t.threadId, t.seq] })],
);

// ---------------------------------------------------------------------------
// Agent thread lease — coordination primitive that prevents two replicas
// from running a turn on the same (namespace, thread) concurrently. One
// row per leased thread; PK is the lease key. `expires_at` defines the
// failover window — a worker that crashes mid-turn loses its lease at
// that timestamp, after which any other worker can steal it.
// ---------------------------------------------------------------------------

export const agentThreadLease = pgTable(
  "agent_thread_lease",
  {
    namespaceId: text("namespace_id").notNull(),
    threadId: text("thread_id").notNull(),
    leaseId: text("lease_id").notNull(),
    ownerId: text("owner_id").notNull(),
    acquiredAt: bigint("acquired_at", { mode: "number" }).notNull(),
    expiresAt: bigint("expires_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.namespaceId, t.threadId] }),
    // Hot-path index for stealing expired leases on the next acquire.
    index("agent_thread_lease_expires_idx").on(t.expiresAt),
  ],
);

// ---------------------------------------------------------------------------
// Secrets — scoped credential vault. Three scopes share one table; the
// kind column discriminates 'global' / 'namespace' / 'resource'. Values
// are stored encrypted (AES-256-GCM, scrypt-stretched passphrase). The
// PK lets the same key name coexist at different scopes.
//
// Scope null-coalescing convention:
//   global       — namespace_id = '', resource_id = ''
//   namespace    — namespace_id = <ns>, resource_id = ''
//   resource     — namespace_id = <ns>, resource_id = <res>
//
// Empty strings (not NULLs) so the PK + UNIQUE indexes treat them as
// regular values (PG NULLs aren't equal to themselves under UNIQUE,
// which would let duplicate global-scope rows slip in).
// ---------------------------------------------------------------------------

export const agentSecret = pgTable(
  "agent_secret",
  {
    scopeKind: text("scope_kind").notNull(), // 'global' | 'namespace' | 'resource'
    namespaceId: text("namespace_id").notNull().default(""),
    resourceId: text("resource_id").notNull().default(""),
    secretKey: text("secret_key").notNull(),
    /** Initialization vector (12 bytes hex). */
    iv: text("iv").notNull(),
    /** AES-GCM auth tag (16 bytes hex). */
    authTag: text("auth_tag").notNull(),
    /** Ciphertext (hex). */
    ciphertext: text("ciphertext").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.scopeKind, t.namespaceId, t.resourceId, t.secretKey] }),
    // Per-scope listing index — `list({ scope })` queries by the
    // (kind, ns, res) prefix, ordered however the caller wants.
    index("agent_secret_scope_idx").on(t.scopeKind, t.namespaceId, t.resourceId),
  ],
);

// ---------------------------------------------------------------------------
// Agent audit log — append-only record of elevated (cross-scope) tool
// calls. Every elevated tool must call `ctx.audit()` per invocation;
// each such call lands here as one row a security review can read.
//
// `recorded_at` is owned by the database clock (server-side NOW()), not
// the caller — an audit trail must not be back-datable by a skewed or
// hostile client. Stored as epoch-ms BIGINT for parity with the rest of
// the agent tables. `agent_id` / `target` / `meta` are nullable: a tool
// can run outside a recipe, and action detail is tool-defined.
// ---------------------------------------------------------------------------

export const agentAuditLog = pgTable(
  "agent_audit_log",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    namespaceId: text("namespace_id").notNull(),
    resourceId: text("resource_id").notNull(),
    /** Recipe id of the agent the tool ran under, when known. */
    agentId: text("agent_id"),
    toolName: text("tool_name").notNull(),
    action: text("action").notNull(),
    /** Optional target of the action — an id, a path, a scope key. */
    target: text("target"),
    /** Optional structured detail, tool-defined. */
    meta: jsonb("meta"),
    /** Epoch ms, assigned by the database clock at insert time. */
    recordedAt: bigint("recorded_at", { mode: "number" }).notNull(),
  },
  (t) => [
    // Read path: list by namespace within a time range, newest first.
    index("agent_audit_log_ns_time_idx").on(t.namespaceId, t.recordedAt),
  ],
);

// ---------------------------------------------------------------------------
// Agent tool history — durable audit trail of which tools the host has
// exposed over time. The live AgentToolCatalog is ephemeral; this is the
// opt-in persistence layer over it (AgentToolCatalogHistory snapshots the
// catalog and upserts here).
//
// One row per (name, source_kind, source_detail, schema_hash) tuple — a
// tool re-observed with the same schema bumps last_seen_at; a parameter
// change flips schema_hash and lands as a new row. Rows are never
// deleted: a vanished tool simply stops having last_seen_at advanced.
// ---------------------------------------------------------------------------

export const agentToolHistory = pgTable(
  "agent_tool_history",
  {
    name: text("name").notNull(),
    sourceKind: text("source_kind").notNull(), // 'in-process' | 'file' | 'mcp'
    sourceDetail: text("source_detail").notNull().default(""),
    /** SHA-256 of the canonical JSON Schema of the tool's parameters. */
    schemaHash: text("schema_hash").notNull(),
    description: text("description").notNull(),
    /** Epoch ms the tuple was first recorded. */
    firstSeenAt: bigint("first_seen_at", { mode: "number" }).notNull(),
    /** Epoch ms the tuple was most recently observed. */
    lastSeenAt: bigint("last_seen_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [t.name, t.sourceKind, t.sourceDetail, t.schemaHash],
    }),
    // Read path: history for a tool name, most recently seen first.
    index("agent_tool_history_name_idx").on(t.name, t.lastSeenAt),
  ],
);

// ---------------------------------------------------------------------------
// Agent instance — a long-lived per-(agent, namespace, owner) instance of a
// recipe. The id (`namespace::recipe::owner`) doubles as the resourceId for
// the memory cascade, so each instance gets its own working memory + facts.
// This table is the thin index; the memory store does the heavy lifting.
// ---------------------------------------------------------------------------

export const agentInstance = pgTable(
  "agent_instance",
  {
    /** Deterministic `${namespaceId}::${registeredAgentId}::${ownerId}`. */
    id: text("id").primaryKey(),
    registeredAgentId: text("registered_agent_id").notNull(),
    namespaceId: text("namespace_id").notNull(),
    ownerId: text("owner_id").notNull(),
    displayName: text("display_name"),
    metadata: jsonb("metadata").notNull().default({}),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
  },
  (t) => [
    // list({ namespaceId, ownerId }) — "what agents has this owner instantiated".
    index("agent_instance_ns_owner_idx").on(t.namespaceId, t.ownerId),
    // list({ registeredAgentId }) — all instances of one recipe.
    index("agent_instance_agent_idx").on(t.registeredAgentId),
  ],
);

// ---------------------------------------------------------------------------
// Agent DAG registry — operator-authored multi-agent execution graphs,
// version-keyed (one row per (id, version), like the agent registry). The
// graph itself (nodes / edges / entry / terminals / metadata) lives in the
// `body` jsonb blob; `created_at` is preserved across version re-writes.
// ---------------------------------------------------------------------------

export const agentDag = pgTable(
  "agent_dag",
  {
    id: text("id").notNull(),
    version: text("version").notNull(),
    /** AgenticDagRecipe minus the identity columns — nodes/edges/entry/... */
    body: jsonb("body").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
    updatedAt: bigint("updated_at", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.id, t.version] }),
    // get(id) without version + list() pick the latest by updated_at.
    index("agent_dag_id_updated_idx").on(t.id, t.updatedAt),
  ],
);

// ---------------------------------------------------------------------------
// Eval run store — persistent history of completed evaluation runs. One row
// per run, keyed on the deterministic composeRunId; the full EvalRunSummary
// rides in the `summary` jsonb blob, identity columns are projected out for
// indexed filtering.
// ---------------------------------------------------------------------------

export const evalRun = pgTable(
  "eval_run",
  {
    runId: text("run_id").primaryKey(),
    targetId: text("target_id").notNull(),
    targetVersion: text("target_version"),
    datasetId: text("dataset_id").notNull(),
    ranAt: bigint("ran_at", { mode: "number" }).notNull(),
    /** The full EvalRunSummary. */
    summary: jsonb("summary").notNull(),
    savedAt: bigint("saved_at", { mode: "number" }).notNull(),
  },
  (t) => [
    index("eval_run_target_ran_idx").on(t.targetId, t.ranAt),
    index("eval_run_dataset_idx").on(t.datasetId),
  ],
);

// ---------------------------------------------------------------------------
// Eval dataset store — named, replaceable eval case lists. The case array
// rides in the `cases` jsonb blob.
// ---------------------------------------------------------------------------

export const evalDataset = pgTable("eval_dataset", {
  datasetId: text("dataset_id").primaryKey(),
  cases: jsonb("cases").notNull(),
});
