-- The retention sweep (`RetentionSweeper`) pages ended runs oldest first by
-- (completed_at, workflow_id): a keyset scan this partial index serves
-- without a sort. Idempotent, so a database that already has it upgrades
-- cleanly and a fresh one is unchanged. On a very large table, create it
-- first with CREATE INDEX CONCURRENTLY outside the migration; this statement
-- then finds it and does nothing.
CREATE INDEX IF NOT EXISTS wf_workflows_retention_idx ON wf_workflows (completed_at, workflow_id) WHERE (status_id = ANY (ARRAY[2, 3, 6]));
