-- Agent DAG registry — operator-authored multi-agent execution graphs,
-- version-keyed (one row per (id, version), like the agent registry). The
-- graph (nodes / edges / entry / terminals / metadata) lives in the body
-- jsonb blob; created_at is preserved across version re-writes.

CREATE TABLE IF NOT EXISTS agent_dag (
  id         TEXT   NOT NULL,
  version    TEXT   NOT NULL,
  body       JSONB  NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (id, version)
);

-- get(id) without version + list() pick the latest by updated_at.
CREATE INDEX IF NOT EXISTS agent_dag_id_updated_idx
  ON agent_dag (id, updated_at);
