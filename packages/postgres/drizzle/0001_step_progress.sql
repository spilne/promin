-- Columns earlier changes added to the baseline in place (a database that
-- applied the older baseline lacks them) and the step progress column. Every
-- statement is idempotent, so a database that already has them upgrades
-- cleanly and a fresh one is unchanged.
ALTER TABLE wf_workflows ADD COLUMN IF NOT EXISTS compensation_origin text;
ALTER TABLE wf_workflows ADD COLUMN IF NOT EXISTS step_progress jsonb;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS group_name text;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS group_max_concurrent integer;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS group_max_starts integer;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS group_window_ms integer;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS fairness_key text;
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS heartbeat_details jsonb;
CREATE INDEX IF NOT EXISTS wf_step_queue_group_started_idx ON wf_step_queue (group_name, claimed_at) WHERE (group_name IS NOT NULL);
