-- Keyset indexes behind the timer / wake scanners. Each scanner page walks
-- these in (workflow_id, step_name) order and stops at its LIMIT, so a page
-- costs its own rows instead of every due step after the cursor. Every
-- statement is idempotent, so a database that already has them upgrades
-- cleanly and a fresh one is unchanged.
CREATE INDEX IF NOT EXISTS wf_workflow_steps_sleep_keyset_idx ON wf_workflow_steps (workflow_id, step_name, wake_at, run) WHERE (status_id = 6);
CREATE INDEX IF NOT EXISTS wf_workflow_steps_signal_timeout_keyset_idx ON wf_workflow_steps (workflow_id, step_name, signal_timeout_at, run) WHERE ((status_id = 7) AND (signal_timeout_at IS NOT NULL));
CREATE INDEX IF NOT EXISTS wf_workflows_suspended_idx ON wf_workflows (workflow_id) WHERE (status_id = 4);
