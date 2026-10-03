-- Step queue contract v2: delivery counting, claim-order / reclaim / purge
-- indexes.

-- ---------------------------------------------------------------------------
-- wf_step_queue.deliveries
-- ---------------------------------------------------------------------------
-- Claims not given back with release(). requeueStuck dead-letters a task
-- (status 'failed') once it reaches the queue's maxDeliveries, so a task that
-- crashes every worker it lands on stops being redelivered. `attempt` stays
-- the runner's attempt number, forwarded on enqueue.

ALTER TABLE wf_step_queue
  ADD COLUMN IF NOT EXISTS deliveries INTEGER NOT NULL DEFAULT 0;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- Claim order (priority DESC, FIFO). The claim walks this index and stops
-- after `limit` matching rows instead of sorting every pending task.
CREATE INDEX IF NOT EXISTS wf_step_queue_pending_order_idx
  ON wf_step_queue (priority DESC, created_at ASC, id ASC)
  WHERE status = 'pending';

-- requeueStuck({ mode: "worker" }): a dead worker's running tasks.
CREATE INDEX IF NOT EXISTS wf_step_queue_running_claimed_by_idx
  ON wf_step_queue (claimed_by)
  WHERE status = 'running';

-- purge(): terminal tasks by completion time.
CREATE INDEX IF NOT EXISTS wf_step_queue_terminal_completed_idx
  ON wf_step_queue (completed_at)
  WHERE status IN ('completed', 'failed');
