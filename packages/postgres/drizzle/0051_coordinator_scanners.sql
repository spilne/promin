-- Coordinator recovery, scanners at scale and start-queue fencing.

-- ---------------------------------------------------------------------------
-- Scanner indexes on wf_workflow_steps
-- ---------------------------------------------------------------------------
-- The sleep and signal scanners used to list every suspended run with its
-- full step JSON and test each step in JS. `listDueTimers` and
-- `listSignalWakeups` now ask for the due ones directly.

-- Sleeping steps (status 6) by wake time.
CREATE INDEX IF NOT EXISTS wf_workflow_steps_wake_at_idx
  ON wf_workflow_steps (wake_at)
  WHERE status_id = 6;

-- Signal waits (status 7) with a timeout, by deadline.
CREATE INDEX IF NOT EXISTS wf_workflow_steps_signal_timeout_idx
  ON wf_workflow_steps (signal_timeout_at)
  WHERE status_id = 7 AND signal_timeout_at IS NOT NULL;

-- Signal waits (status 7), joined to wf_workflow_signals on the signal name.
CREATE INDEX IF NOT EXISTS wf_workflow_steps_waiting_signal_idx
  ON wf_workflow_steps (workflow_id, signal_name)
  WHERE status_id = 7;

-- ---------------------------------------------------------------------------
-- Coordinator recovery index on wf_workflows
-- ---------------------------------------------------------------------------
-- `listOrphanedRuns` pages through pending (0) / running (1) runs in
-- workflow-id order and keeps the ones whose lock is free or expired.

CREATE INDEX IF NOT EXISTS wf_workflows_active_idx
  ON wf_workflows (workflow_id)
  WHERE status_id IN (0, 1);

-- ---------------------------------------------------------------------------
-- wf_workflow_starts: claim token + heartbeat
-- ---------------------------------------------------------------------------
-- Starts were reclaimed a fixed time after the claim, so any run longer than
-- that was started a second time, and `complete(id)` had no token, so a
-- stale claimant could delete the start another worker had just re-claimed.
-- Claims now carry a token; `heartbeat` keeps a claim alive and `complete`
-- only deletes the row for the current token.

ALTER TABLE wf_workflow_starts ADD COLUMN IF NOT EXISTS claim_token TEXT;
ALTER TABLE wf_workflow_starts ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

UPDATE wf_workflow_starts
  SET heartbeat_at = claimed_at
  WHERE status = 'claimed' AND heartbeat_at IS NULL;

DROP INDEX IF EXISTS wf_workflow_starts_claimed_idx;

CREATE INDEX IF NOT EXISTS wf_workflow_starts_heartbeat_idx
  ON wf_workflow_starts (heartbeat_at)
  WHERE status = 'claimed';
