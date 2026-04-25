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
