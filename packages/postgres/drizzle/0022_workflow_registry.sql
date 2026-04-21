-- promin-r93v: workflow definition registry — stores serialized WorkflowDAG
-- entries so coordinators can resolve definitions from a shared Postgres store
-- instead of requiring the definition to be in-process.
--
-- Each row is a (name, version) pair. Only one version is "active" (latest)
-- per name; all versions are kept for drain support.

CREATE TABLE IF NOT EXISTS wf_workflow_registry (
  name         TEXT        NOT NULL,
  version      TEXT        NOT NULL,
  dag_json     JSONB       NOT NULL,
  idempotency  JSONB,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (name, version)
);

CREATE INDEX IF NOT EXISTS wf_workflow_registry_name_idx ON wf_workflow_registry (name);
