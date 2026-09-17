-- Eval dataset store — named, replaceable eval case lists. The case array
-- rides in the cases jsonb blob; saving a dataset replaces it wholesale.

CREATE TABLE IF NOT EXISTS eval_dataset (
  dataset_id TEXT  NOT NULL,
  cases      JSONB NOT NULL,
  PRIMARY KEY (dataset_id)
);
