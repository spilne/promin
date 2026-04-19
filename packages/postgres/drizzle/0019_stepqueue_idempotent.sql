-- promin-k6mk: make StepQueue.enqueue idempotent while a prior task for the
-- same step is still pending or running.
--
-- Key is (workflow_id, step_name). Matches the schema's existing identity
-- contract — every other table's PK treats workflow_id as globally unique
-- (wf_workflows.workflow_id is PRIMARY KEY on its own, and derived tables
-- like wf_workflow_steps key off (workflow_id, step_name, run) without
-- namespace). Dedupe in the step queue follows the same convention.
--
-- Partial rather than plain: terminal rows (completed / failed) stay
-- outside the predicate so step-level retry + startFreshRun()'s
-- per-step re-execution keep working.

CREATE UNIQUE INDEX IF NOT EXISTS wf_step_queue_active_uniq
  ON wf_step_queue (workflow_id, step_name)
  WHERE status IN ('pending', 'running');
