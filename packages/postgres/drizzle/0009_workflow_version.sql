-- Add version column to wf_workflows for workflow versioning (Phase 1)
ALTER TABLE wf_workflows ADD COLUMN version TEXT;
