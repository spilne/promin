-- Per-task concurrency keys on the step queue.
--
-- Each pending task can carry an optional `(concurrency_scope,
-- concurrency_key, concurrency_limit)` tuple. When set, `claim()` only
-- claims the task if fewer than `concurrency_limit` tasks with the same
-- `(scope, key)` are currently `running`. Lets multi-tenant workloads
-- cap per-tenant in-flight runs without one tenant starving others.
--
-- Scope conventions:
--   workflow_name              — workflow-level concurrency (caps any
--                                step in a workflow with this name)
--   workflow_name::step_name   — step-level concurrency (caps just one
--                                step within the workflow)
--
-- Key is the user-evaluated string: e.g. payload.tenantId.
-- Limit is the cap; null disables enforcement for this task.

ALTER TABLE wf_step_queue
  ADD COLUMN IF NOT EXISTS concurrency_key TEXT,
  ADD COLUMN IF NOT EXISTS concurrency_scope TEXT,
  ADD COLUMN IF NOT EXISTS concurrency_limit INTEGER;

-- Hot-path index for the dispatch count subquery — `claim()` reads "how
-- many tasks with this (scope, key) are currently running?" once per
-- candidate task. Partial on running + non-null fields keeps it tight.
CREATE INDEX IF NOT EXISTS wf_step_queue_concurrency_running_idx
  ON wf_step_queue (concurrency_scope, concurrency_key)
  WHERE status = 'running' AND concurrency_key IS NOT NULL;
