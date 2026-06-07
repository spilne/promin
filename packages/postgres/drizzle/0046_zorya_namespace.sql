-- Zorya namespace entity — authoritative lifecycle/policy registry.
-- Separate from agent_namespace, which stores memory configuration for a
-- namespace and is not the source of truth for namespace existence.

CREATE TABLE IF NOT EXISTS zorya_namespace (
  id           TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  description  TEXT,
  status       TEXT NOT NULL DEFAULT 'active',
  capabilities JSONB NOT NULL,
  metadata     JSONB NOT NULL,
  created_at   BIGINT NOT NULL,
  updated_at   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS zorya_namespace_status_display_idx
  ON zorya_namespace (status, display_name, id);
