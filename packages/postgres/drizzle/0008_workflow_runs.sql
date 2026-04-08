-- Archive table for workflow run snapshots (preserved across startFreshRun)
CREATE TABLE IF NOT EXISTS wf_workflow_runs (
  workflow_id TEXT NOT NULL REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE,
  run INTEGER NOT NULL,
  status_id INTEGER NOT NULL,
  result JSONB,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workflow_id, run)
);
