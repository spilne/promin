-- Durable audit trail: which worker handled each step attempt.
-- wf_step_queue.claimed_by holds this while the task runs, but queue rows
-- get truncated by retention and never join with the attempts table.
-- Persist the worker id alongside the attempt so ops queries can ask
-- "which worker handled the failed retry of order-123's charge?"
-- well after the queue row is gone.

ALTER TABLE wf_step_attempts ADD COLUMN IF NOT EXISTS worker_id TEXT;
