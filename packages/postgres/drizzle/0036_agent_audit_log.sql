-- Agent audit log — append-only record of elevated (cross-scope) tool
-- calls. Every elevated tool must call ctx.audit() per invocation; each
-- such call lands here as one row a security review can read.
--
-- recorded_at is owned by the database clock (server-side NOW()), not
-- the caller — an audit trail must not be back-datable by a skewed or
-- hostile client. Stored as epoch-ms BIGINT for parity with the other
-- agent tables. agent_id / target / meta are nullable: a tool can run
-- outside a recipe, and action detail is tool-defined.

CREATE TABLE IF NOT EXISTS agent_audit_log (
  id            BIGSERIAL PRIMARY KEY,
  namespace_id  TEXT NOT NULL,
  resource_id   TEXT NOT NULL,
  agent_id      TEXT,
  tool_name     TEXT NOT NULL,
  action        TEXT NOT NULL,
  target        TEXT,
  meta          JSONB,
  recorded_at   BIGINT NOT NULL
);

-- Read path: list by namespace within a time range, newest first.
CREATE INDEX IF NOT EXISTS agent_audit_log_ns_time_idx
  ON agent_audit_log (namespace_id, recorded_at);
