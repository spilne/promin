-- Skill registry — versioned instruction-block store. Sibling of
-- agent_registry: one row per (skill_id, version). Content fields
-- (description / when_to_use / body) are columns; metadata (capabilities /
-- tags / enabled) is JSONB so it evolves without migrations.
--
-- Indexes:
--   - PK (skill_id, version) — exact-version lookup, version listing
--   - (skill_id, updated_at) — get(id) without version returns the
--     most-recently-updated row

CREATE TABLE IF NOT EXISTS skill_registry (
  skill_id    TEXT NOT NULL,
  version     TEXT NOT NULL,
  description TEXT NOT NULL,
  when_to_use TEXT NOT NULL,
  body        TEXT NOT NULL,
  metadata    JSONB NOT NULL,
  created_at  BIGINT NOT NULL,
  updated_at  BIGINT NOT NULL,
  PRIMARY KEY (skill_id, version)
);

CREATE INDEX IF NOT EXISTS skill_registry_id_updated_idx
  ON skill_registry (skill_id, updated_at);
