-- Add pending workflow status and started_at timestamp
-- Workflows now start as 'pending' and transition to 'running' when first step executes.

ALTER TABLE wf_workflows ADD COLUMN started_at TIMESTAMPTZ;
ALTER TABLE wf_workflow_runs ADD COLUMN started_at TIMESTAMPTZ;

-- Add pending status to lookup (id=0)
INSERT INTO wf_workflow_status (id, name) VALUES (0, 'pending') ON CONFLICT (name) DO NOTHING;

-- Change default status from running (1) to pending (0)
ALTER TABLE wf_workflows ALTER COLUMN status_id SET DEFAULT 0;

-- Existing running workflows remain running — no data migration needed.
