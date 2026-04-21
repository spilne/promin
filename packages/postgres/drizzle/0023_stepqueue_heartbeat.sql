-- Add heartbeat_at to wf_step_queue so workers can extend their running lease.
-- requeueStuck uses COALESCE(heartbeat_at, claimed_at) as the last-activity
-- timestamp, so a heartbeating worker is never reclaimed as stale.

ALTER TABLE wf_step_queue
  ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;
