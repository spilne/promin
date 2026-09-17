-- Agent memory store — three-scope cascade × four-tier model. Mirrors
-- the SqliteMemoryStore schema so the same conformance suite passes
-- against both backends.
--
-- All timestamps are millisecond unix epochs (BIGINT), matching the
-- SQLite implementation. Boolean inheritance flags are real booleans
-- here (vs INTEGER 0/1 in SQLite). JSON payloads use JSONB for indexable
-- containment queries when we need them.

-- Namespace scope: top-level shared rules / working memory / metadata.
CREATE TABLE IF NOT EXISTS agent_namespace (
  namespace_id        TEXT NOT NULL PRIMARY KEY,
  static_rules        TEXT,
  working_memory      TEXT,
  inherit_from_parent BOOLEAN NOT NULL DEFAULT TRUE,
  metadata            JSONB,
  created_at          BIGINT NOT NULL,
  updated_at          BIGINT NOT NULL
);

-- Resource scope: per-(namespace, user) bucket.
CREATE TABLE IF NOT EXISTS agent_resource (
  namespace_id        TEXT NOT NULL,
  resource_id         TEXT NOT NULL,
  static_rules        TEXT,
  working_memory      TEXT,
  inherit_from_parent BOOLEAN NOT NULL DEFAULT TRUE,
  metadata            JSONB,
  created_at          BIGINT NOT NULL,
  updated_at          BIGINT NOT NULL,
  PRIMARY KEY (namespace_id, resource_id)
);

-- Thread scope: conversational state.
CREATE TABLE IF NOT EXISTS agent_thread (
  namespace_id        TEXT NOT NULL,
  thread_id           TEXT NOT NULL,
  resource_id         TEXT,
  title               TEXT,
  working_memory      TEXT,
  inherit_from_parent BOOLEAN NOT NULL DEFAULT TRUE,
  metadata            JSONB,
  archived_at         BIGINT,
  created_at          BIGINT NOT NULL,
  updated_at          BIGINT NOT NULL,
  PRIMARY KEY (namespace_id, thread_id)
);

CREATE INDEX IF NOT EXISTS agent_thread_resource_idx
  ON agent_thread (namespace_id, resource_id);

-- Facts: distilled key/value-ish strings, one row per fact, scope-tagged.
-- Single table for all three scopes so writes are uniform; partial
-- indexes per scope keep the read path tight.
CREATE TABLE IF NOT EXISTS agent_fact (
  id           TEXT NOT NULL PRIMARY KEY,
  scope        TEXT NOT NULL,    -- 'namespace' | 'resource' | 'thread'
  namespace_id TEXT NOT NULL,
  resource_id  TEXT,
  thread_id    TEXT,
  text         TEXT NOT NULL,
  created_at   BIGINT NOT NULL,
  updated_at   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS agent_fact_ns_idx
  ON agent_fact (scope, namespace_id, created_at);

CREATE INDEX IF NOT EXISTS agent_fact_res_idx
  ON agent_fact (scope, namespace_id, resource_id, created_at);

CREATE INDEX IF NOT EXISTS agent_fact_thr_idx
  ON agent_fact (scope, namespace_id, thread_id, created_at);

-- Episodes: distilled "what happened" entries with salience + optional
-- embedding. Per-thread rollups support compaction; per-resource
-- episodes power cross-thread recall.
CREATE TABLE IF NOT EXISTS agent_episode (
  id                  TEXT NOT NULL PRIMARY KEY,
  scope               TEXT NOT NULL,
  namespace_id        TEXT NOT NULL,
  resource_id         TEXT,
  thread_id           TEXT,
  summary             TEXT NOT NULL,
  outcome             TEXT,
  salience            DOUBLE PRECISION NOT NULL DEFAULT 0.5,
  embedding           JSONB,
  source_thread_id    TEXT,
  source_msg_from_seq INTEGER,
  source_msg_to_seq   INTEGER,
  occurred_at         BIGINT NOT NULL,
  created_at          BIGINT NOT NULL,
  metadata            JSONB
);

CREATE INDEX IF NOT EXISTS agent_episode_ns_idx
  ON agent_episode (scope, namespace_id, salience);

CREATE INDEX IF NOT EXISTS agent_episode_res_idx
  ON agent_episode (scope, namespace_id, resource_id, salience);

CREATE INDEX IF NOT EXISTS agent_episode_thr_idx
  ON agent_episode (scope, namespace_id, thread_id, created_at);

-- Messages: append-only transcript per thread.
CREATE TABLE IF NOT EXISTS agent_message (
  namespace_id TEXT NOT NULL,
  thread_id    TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  payload      JSONB NOT NULL,
  created_at   BIGINT NOT NULL,
  PRIMARY KEY (namespace_id, thread_id, seq)
);
