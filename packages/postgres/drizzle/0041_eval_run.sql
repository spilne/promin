-- Eval run store — persistent history of completed evaluation runs. One
-- row per run, keyed on the deterministic composeRunId; the full
-- EvalRunSummary rides in the summary jsonb blob. Identity columns are
-- projected out so EvalRunStore.list can filter on indexes.

CREATE TABLE IF NOT EXISTS eval_run (
  run_id         TEXT   NOT NULL,
  target_id      TEXT   NOT NULL,
  target_version TEXT,
  dataset_id     TEXT   NOT NULL,
  ran_at         BIGINT NOT NULL,
  summary        JSONB  NOT NULL,
  saved_at       BIGINT NOT NULL,
  PRIMARY KEY (run_id)
);

CREATE INDEX IF NOT EXISTS eval_run_target_ran_idx ON eval_run (target_id, ran_at);
CREATE INDEX IF NOT EXISTS eval_run_dataset_idx ON eval_run (dataset_id);
