-- Backfill schema objects that landed in `schema.ts` without a matching
-- migration. Each one was added alongside its feature code but the
-- drizzle/ folder was never updated, so a database built purely from the
-- migration chain (CI, fresh deploys) lacked columns and tables the
-- runtime reads and writes.
--
-- Everything here is `IF NOT EXISTS` — databases bootstrapped through
-- `ensureTable()` (demos, some test suites) already carry these objects
-- and must stay untouched.

-- ---------------------------------------------------------------------------
-- wf_workflow_steps.signal_json_schema
-- ---------------------------------------------------------------------------
-- JSON Schema snapshot the suspend point waited on. Written by
-- `ctx.validatedSignal` / `ctx.approval` from `sig.schema.jsonSchema`;
-- read by the server's signal-delivery path to validate inbound payloads
-- before calling `deliverSignal`. Null for plain `ctx.signal()` callers.

ALTER TABLE wf_workflow_steps ADD COLUMN IF NOT EXISTS signal_json_schema JSONB;

-- ---------------------------------------------------------------------------
-- wf_workflows_idempotency_key_idx — namespace scoping
-- ---------------------------------------------------------------------------
-- 0028 created this index on (workflow_name, idempotency_key), so two
-- namespaces could not reuse the same key for the same workflow name —
-- the second namespace's insert hit the unique index and `createWorkflow`
-- then failed to resolve the conflict to a row it could attach to.
-- `COALESCE(namespace, '')` leads the tuple so unnamespaced rows still
-- share one slot. Widening a unique index never introduces a conflict, so
-- the rebuild is safe on populated tables.

DROP INDEX IF EXISTS wf_workflows_idempotency_key_idx;

CREATE UNIQUE INDEX IF NOT EXISTS wf_workflows_idempotency_key_idx
  ON wf_workflows (COALESCE(namespace, ''), workflow_name, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- ---------------------------------------------------------------------------
-- wf_step_queue.claim_token
-- ---------------------------------------------------------------------------
-- Per-claim fencing token minted by `claim()` and carried back by
-- complete / fail / heartbeat. A worker whose lease was reclaimed holds a
-- stale token and its late write matches no row, so it cannot clobber the
-- task the new owner is running.

ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS claim_token TEXT;

-- ---------------------------------------------------------------------------
-- wf_workflow_locks.fence_token
-- ---------------------------------------------------------------------------
-- Monotonic fence token, bumped on every successful `tryLock` via
-- `nextval(pg_get_serial_sequence('wf_workflow_locks', 'fence_token'))`
-- and validated against this row before a guarded write commits. Lets a
-- new holder take over after the previous lease expired without the stale
-- holder committing late writes. BIGSERIAL so the owning sequence exists
-- for `pg_get_serial_sequence` to resolve.

ALTER TABLE wf_workflow_locks ADD COLUMN IF NOT EXISTS fence_token BIGSERIAL NOT NULL;

-- ---------------------------------------------------------------------------
-- wf_workflow_advertisements
-- ---------------------------------------------------------------------------
-- Worker-advertised workflow catalog (PgWorkflowAdvertisementRegistry).
-- Workers `upsert` their definitions on connect; the dashboard reads
-- `distinct()`. Postgres-backed so multi-replica deployments share one
-- consolidated catalog across restarts.

CREATE TABLE IF NOT EXISTS wf_workflow_advertisements (
  worker_id     TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  version       TEXT,
  steps         JSONB NOT NULL,
  sample_input  JSONB,
  advertised_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Two partial unique indexes rather than one constraint: Postgres treats
-- NULL as distinct in unique indexes, and a NULL version must still
-- dedupe per (worker, name).
CREATE UNIQUE INDEX IF NOT EXISTS wf_workflow_advertisements_worker_name_version_idx
  ON wf_workflow_advertisements (worker_id, workflow_name, version)
  WHERE version IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS wf_workflow_advertisements_worker_name_nullver_idx
  ON wf_workflow_advertisements (worker_id, workflow_name)
  WHERE version IS NULL;

-- Dashboard's "show every workflow" and dispatch's "find by name" both
-- pivot on (workflow_name, version).
CREATE INDEX IF NOT EXISTS wf_workflow_advertisements_workflow_idx
  ON wf_workflow_advertisements (workflow_name, version);

-- ---------------------------------------------------------------------------
-- wf_workflow_starts
-- ---------------------------------------------------------------------------
-- Pending start-workflow requests for the queued / split deployment shape
-- (PgWorkflowStartQueue). Dashboard triggers enqueue here; workflow-mode
-- workers claim with `FOR UPDATE SKIP LOCKED`. Completion deletes the row.

CREATE TABLE IF NOT EXISTS wf_workflow_starts (
  id            TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  version       TEXT,
  input         JSONB NOT NULL,
  metadata      JSONB,
  enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claimed_at    TIMESTAMPTZ,
  claimed_by    TEXT,
  status        TEXT NOT NULL DEFAULT 'pending'
);

-- Hot path: workers claim by (workflow_name, enqueued_at) over the
-- pending partition only.
CREATE INDEX IF NOT EXISTS wf_workflow_starts_pending_idx
  ON wf_workflow_starts (workflow_name, enqueued_at)
  WHERE status = 'pending';

-- Stale-claim sweeper: find rows past the reclaim window.
CREATE INDEX IF NOT EXISTS wf_workflow_starts_claimed_idx
  ON wf_workflow_starts (claimed_at)
  WHERE status = 'claimed';
