-- Composite indexes for status-filtered dashboard metrics and run lists.
-- The standalone status index cannot satisfy the accompanying time sort.
CREATE INDEX IF NOT EXISTS wf_workflows_status_started_at_idx
  ON wf_workflows (status_id, started_at DESC);

CREATE INDEX IF NOT EXISTS wf_workflows_status_completed_at_idx
  ON wf_workflows (status_id, completed_at DESC);
