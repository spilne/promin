-- Role registry — versioned behavioral-bundle store. Sibling of
-- agent_registry: one row per (role_id, version). The behavioral
-- `definition` (persona prompt + tools + skills + capabilities) and the
-- `metadata` (description / tags / suggested_secrets) are JSONB so the
-- shapes evolve without migrations.
--
-- Indexes:
--   - PK (role_id, version) — exact-version lookup, version listing
--   - (role_id, updated_at) — get(id) without version returns the
--     most-recently-updated row

CREATE TABLE IF NOT EXISTS role_registry (
  role_id    TEXT NOT NULL,
  version    TEXT NOT NULL,
  definition JSONB NOT NULL,
  metadata   JSONB NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (role_id, version)
);

CREATE INDEX IF NOT EXISTS role_registry_id_updated_idx
  ON role_registry (role_id, updated_at);
