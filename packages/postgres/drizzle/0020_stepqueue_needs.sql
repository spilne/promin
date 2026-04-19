-- Routing moves from queue names to capability sets.
-- Replaces `queue TEXT` with `needs TEXT[]` so tasks carry the capabilities
-- they require and workers claim via subset containment (`needs <@ caps`).
--
-- Hard break (no production data yet). A real rollout would need a
-- backfill; this just drops the column and adds the new one.

ALTER TABLE wf_step_queue DROP COLUMN IF EXISTS queue;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS needs TEXT[] NOT NULL DEFAULT '{}';

-- GIN index powers `needs <@ caps` subset queries at claim time. Partial
-- on status='pending' because claim only reads pending rows and terminal
-- rows can accumulate; no point indexing them.
CREATE INDEX IF NOT EXISTS wf_step_queue_needs_idx
  ON wf_step_queue USING GIN (needs)
  WHERE status = 'pending';

-- The old dequeue index referenced `queue`. Recreate without it — FIFO
-- within priority stays useful for the claim ORDER BY.
DROP INDEX IF EXISTS wf_step_queue_dequeue_idx;
CREATE INDEX IF NOT EXISTS wf_step_queue_dequeue_idx
  ON wf_step_queue (status, priority, created_at);
