-- Scheduler refactor: add namespace + nextRun + tickCount.
-- Backend now plugs into the generic DurableScheduler shell in @promin/workflow,
-- which uses (namespace, next_run) for findDue and a per-row tick_count.

ALTER TABLE wf_schedules
  ADD COLUMN IF NOT EXISTS namespace TEXT,
  ADD COLUMN IF NOT EXISTS next_run TIMESTAMP WITH TIME ZONE,
  ADD COLUMN IF NOT EXISTS tick_count BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS wf_schedules_due_idx
  ON wf_schedules (namespace, next_run)
  WHERE enabled = true AND next_run IS NOT NULL;
