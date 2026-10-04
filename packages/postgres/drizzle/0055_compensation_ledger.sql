-- Durable compensation: the per-step ledger and recovery of runs that were
-- rolling back.
--
-- A run that fails after its retries enters status `compensating` (5)
-- before its first rollback, and records each step's rollback on the step
-- row as it settles. A run whose driver stops mid-rollback is found
-- `compensating`; its next driver finishes the rollback, skipping the steps
-- the ledger already lists, instead of re-running the workflow.

-- ---------------------------------------------------------------------------
-- wf_workflow_steps: compensation ledger
-- ---------------------------------------------------------------------------

ALTER TABLE wf_workflow_steps ADD COLUMN IF NOT EXISTS compensation_status TEXT;
ALTER TABLE wf_workflow_steps ADD COLUMN IF NOT EXISTS compensation_error TEXT;
ALTER TABLE wf_workflow_steps ADD COLUMN IF NOT EXISTS compensated_at TIMESTAMPTZ;

ALTER TABLE wf_workflow_steps
  DROP CONSTRAINT IF EXISTS wf_workflow_steps_compensation_status_check;
ALTER TABLE wf_workflow_steps
  ADD CONSTRAINT wf_workflow_steps_compensation_status_check
  CHECK (compensation_status IN ('compensated', 'compensation_failed'));

-- ---------------------------------------------------------------------------
-- Recovery index on wf_workflows
-- ---------------------------------------------------------------------------
-- `listOrphanedRuns` now also returns `compensating` (5) runs nobody holds,
-- so their rollback is finished by whoever adopts them.

DROP INDEX IF EXISTS wf_workflows_active_idx;
CREATE INDEX IF NOT EXISTS wf_workflows_active_idx
  ON wf_workflows (workflow_id)
  WHERE status_id IN (0, 1, 5);
