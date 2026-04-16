-- Activity journal for .journaled() steps (promin-0kt Phase 1).
-- One row per activity invocation within a journaled step. On replay, the
-- engine consults this table to skip already-executed activities. Keyed by
-- (workflow_id, step_name, activity_index) so per-step journals are isolated
-- and appending is idempotent on the primary key.

CREATE TABLE IF NOT EXISTS wf_activity_journal (
  workflow_id     TEXT NOT NULL,
  step_name       TEXT NOT NULL,
  activity_index  INTEGER NOT NULL,
  activity_name   TEXT NOT NULL,
  exit            JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workflow_id, step_name, activity_index),
  FOREIGN KEY (workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS wf_activity_journal_step_idx
  ON wf_activity_journal (workflow_id, step_name);
