-- Agent tool history — durable audit trail of which tools the host has
-- exposed over time. The live AgentToolCatalog is ephemeral; this is the
-- opt-in persistence layer over it (AgentToolCatalogHistory snapshots the
-- catalog and upserts here).
--
-- One row per (name, source_kind, source_detail, schema_hash) tuple — a
-- tool re-observed with the same schema bumps last_seen_at; a parameter
-- change flips schema_hash and lands as a new row. Rows are never
-- deleted: a vanished tool simply stops having last_seen_at advanced.

CREATE TABLE IF NOT EXISTS agent_tool_history (
  name           TEXT NOT NULL,
  source_kind    TEXT NOT NULL,
  source_detail  TEXT NOT NULL DEFAULT '',
  schema_hash    TEXT NOT NULL,
  description    TEXT NOT NULL,
  first_seen_at  BIGINT NOT NULL,
  last_seen_at   BIGINT NOT NULL,
  PRIMARY KEY (name, source_kind, source_detail, schema_hash)
);

-- Read path: history for a tool name, most recently seen first.
CREATE INDEX IF NOT EXISTS agent_tool_history_name_idx
  ON agent_tool_history (name, last_seen_at);
