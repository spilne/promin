-- Agent registry — versioned recipe store. Mirrors the shape of
-- SqliteAgentRegistry: one row per (agent_id, version) tuple. Backend
-- and metadata are stored as JSONB so new backend variants (cursor,
-- remote, local) land without schema migrations.
--
-- Indexes:
--   - PK (agent_id, version) — exact-version lookup, version listing
--   - (agent_id, updated_at) — get(id) without version returns the
--     most-recently-updated row
--   - (backend_type) — list({ backendType }) filter

CREATE TABLE IF NOT EXISTS agent_registry (
  agent_id     TEXT NOT NULL,
  version      TEXT NOT NULL,
  backend_type TEXT NOT NULL,
  backend      JSONB NOT NULL,
  metadata     JSONB NOT NULL,
  created_at   BIGINT NOT NULL,
  updated_at   BIGINT NOT NULL,
  PRIMARY KEY (agent_id, version)
);

CREATE INDEX IF NOT EXISTS agent_registry_id_updated_idx
  ON agent_registry (agent_id, updated_at);

CREATE INDEX IF NOT EXISTS agent_registry_backend_type_idx
  ON agent_registry (backend_type);
