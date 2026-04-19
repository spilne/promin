-- promin-k6mk: make StepQueue.enqueue idempotent while a prior task for the
-- same logical step is still pending or running.
--
-- Key is (COALESCE(namespace, ''), workflow_id, step_name) — namespace is
-- nullable and Postgres treats NULLs as distinct in plain unique indexes,
-- so we COALESCE it to '' to force NULL collisions into one bucket. This
-- also matches how `claim` filters by namespace: two different tenants
-- with the same workflowId+stepName are intentionally isolated, and the
-- dedupe must respect that boundary.
--
-- Partial (WHERE status IN ...) rather than plain: terminal rows
-- (completed / failed) are deliberately allowed to coexist with a fresh
-- pending row — step-level retry and startFreshRun() both need that.
--
-- ON CONFLICT inference in PgStepQueue.enqueue references this index by
-- its full expression list + matching WHERE predicate.

CREATE UNIQUE INDEX IF NOT EXISTS wf_step_queue_active_uniq
  ON wf_step_queue ((COALESCE(namespace, '')), workflow_id, step_name)
  WHERE status IN ('pending', 'running');
