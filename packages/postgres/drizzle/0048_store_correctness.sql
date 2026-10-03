-- Workflow store correctness: journal step types, run lineage columns,
-- dependent-table foreign keys, row-lock table for state machines, and the
-- index set the dashboard / purge / journal hot paths actually use.

-- ---------------------------------------------------------------------------
-- wf_activity_journal.step_type — allow 'child'
-- ---------------------------------------------------------------------------
-- `ctx.child` journals its pending/completed row with step_type 'child';
-- the CHECK from 0016 rejected it, so every child workflow call failed on
-- Postgres. The list mirrors `JOURNAL_STEP_TYPES` in @promin/workflow.

ALTER TABLE wf_activity_journal
  DROP CONSTRAINT IF EXISTS wf_activity_journal_step_type_check;
ALTER TABLE wf_activity_journal
  ADD CONSTRAINT wf_activity_journal_step_type_check
  CHECK (step_type IN ('activity', 'sleep', 'signal', 'compensation', 'child'));

-- ---------------------------------------------------------------------------
-- wf_workflows — parent and run-source lineage
-- ---------------------------------------------------------------------------
-- `createWorkflow` accepts parentWorkflowId / runSource / runSourceId and
-- `listWorkflows` filters on them; without columns they were silently
-- dropped, so `?runSource=schedule` returned every run and cascade cancel
-- had nothing to walk. run_source holds the small-int `RUN_SOURCE_CODES`.

ALTER TABLE wf_workflows
  ADD COLUMN IF NOT EXISTS parent_workflow_id TEXT,
  ADD COLUMN IF NOT EXISTS run_source SMALLINT,
  ADD COLUMN IF NOT EXISTS run_source_id TEXT;

CREATE INDEX IF NOT EXISTS wf_workflows_parent_idx
  ON wf_workflows (parent_workflow_id)
  WHERE parent_workflow_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS wf_workflows_run_source_idx
  ON wf_workflows (run_source, run_source_id)
  WHERE run_source IS NOT NULL;

-- ---------------------------------------------------------------------------
-- wf_workflows — dashboard and purge indexes
-- ---------------------------------------------------------------------------
-- The default `listWorkflows` order is `started_at DESC NULLS LAST`, with or
-- without a namespace scope; purge scans terminal rows (completed 2,
-- failed 3, tripwire 6) by completed_at. The namespace-leading composite
-- also serves plain namespace lookups, so the single-column index goes.

CREATE INDEX IF NOT EXISTS wf_workflows_started_at_idx
  ON wf_workflows (started_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS wf_workflows_namespace_started_at_idx
  ON wf_workflows (namespace, started_at DESC NULLS LAST);

DROP INDEX IF EXISTS wf_workflows_namespace_idx;

CREATE INDEX IF NOT EXISTS wf_workflows_purge_idx
  ON wf_workflows (completed_at)
  WHERE status_id IN (2, 3, 6);

-- ---------------------------------------------------------------------------
-- Redundant indexes — each duplicates a primary key or a unique index prefix
-- ---------------------------------------------------------------------------

DROP INDEX IF EXISTS wf_streams_workflow_idx;          -- prefix of the PK
DROP INDEX IF EXISTS wf_streams_workflow_stream_idx;   -- same columns as the PK
DROP INDEX IF EXISTS wf_activity_journal_step_idx;     -- prefix of the PK
DROP INDEX IF EXISTS wf_signals_workflow_idx;          -- prefix of the unique index

-- ---------------------------------------------------------------------------
-- Foreign keys for wf_streams and wf_signal_tokens
-- ---------------------------------------------------------------------------
-- Purging a workflow cascaded to its journal, steps and signals but left
-- stream chunks and signal tokens behind forever. NOT VALID enforces the
-- constraint for new rows without scanning (or rejecting) rows already
-- orphaned by earlier purges.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'wf_streams_workflow_id_fkey'
  ) THEN
    ALTER TABLE wf_streams
      ADD CONSTRAINT wf_streams_workflow_id_fkey
      FOREIGN KEY (workflow_id) REFERENCES wf_workflows (workflow_id)
      ON DELETE CASCADE NOT VALID;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'wf_signal_tokens_workflow_id_fkey'
  ) THEN
    ALTER TABLE wf_signal_tokens
      ADD CONSTRAINT wf_signal_tokens_workflow_id_fkey
      FOREIGN KEY (workflow_id) REFERENCES wf_workflows (workflow_id)
      ON DELETE CASCADE NOT VALID;
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- sm_machine_locks — row locks for PgStateMachineStorage
-- ---------------------------------------------------------------------------
-- Session advisory locks issued through a connection pool are re-entrant
-- per connection and their unlock lands on whichever connection the pool
-- hands out, so they neither excluded concurrent senders nor released
-- reliably. A lease row with a server-side expiry does both.

CREATE TABLE IF NOT EXISTS sm_machine_locks (
  machine_id TEXT PRIMARY KEY,
  locked_by  TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
