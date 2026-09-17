-- Fragment store — durable storage for operator-authored prompt fragments.
-- Sibling of skill_registry. File-scanned fragments live in the in-memory
-- registry only and are NOT persisted here (the .md file is the source of
-- truth — persisting them would create stale rows after file removal).

CREATE TABLE IF NOT EXISTS fragment_store (
  key        TEXT PRIMARY KEY,
  content    TEXT NOT NULL,
  updated_at BIGINT NOT NULL
);
