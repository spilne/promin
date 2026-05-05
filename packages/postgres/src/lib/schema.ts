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
      .on(t.workflowName, t.idempotencyKey)
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
    registeredAt: timestamp("registered_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.name, t.version] }),
    index("wf_workflow_registry_name_idx").on(t.name),
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
    status: text("status").notNull().default("active"), // active | draining | dead
    capabilities: text("capabilities").array().notNull().default([]),
    concurrency: integer("concurrency").notNull().default(1),
    metadata: jsonb("metadata"),
    startedAt: timestamp("started_at", { withTimezone: true }).notNull().defaultNow(),
    lastHeartbeatAt: timestamp("last_heartbeat_at", { withTimezone: true }).notNull().defaultNow(),
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
