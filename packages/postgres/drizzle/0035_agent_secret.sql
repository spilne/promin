-- Scoped secrets vault — three scopes (global / namespace / resource)
-- share one table, scope_kind discriminates. Values stored encrypted
-- (AES-256-GCM with scrypt-stretched passphrase) so a database leak
-- alone doesn't expose secrets — host needs the passphrase too.
--
-- Scope null-coalescing: empty strings stand in for null so the PK +
-- UNIQUE constraint treats absent fields as ordinary equality. PG's
-- NULLs-are-not-equal-to-NULLs semantics under UNIQUE would otherwise
-- let duplicate global-scope rows slip in.

CREATE TABLE IF NOT EXISTS agent_secret (
  scope_kind   TEXT NOT NULL,
  namespace_id TEXT NOT NULL DEFAULT '',
  resource_id  TEXT NOT NULL DEFAULT '',
  secret_key   TEXT NOT NULL,
  iv           TEXT NOT NULL,
  auth_tag     TEXT NOT NULL,
  ciphertext   TEXT NOT NULL,
  created_at   BIGINT NOT NULL,
  updated_at   BIGINT NOT NULL,
  PRIMARY KEY (scope_kind, namespace_id, resource_id, secret_key)
);

CREATE INDEX IF NOT EXISTS agent_secret_scope_idx
  ON agent_secret (scope_kind, namespace_id, resource_id);
