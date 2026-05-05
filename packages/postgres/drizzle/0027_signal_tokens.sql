-- Public-bearer signal tokens.
--
-- Authorization sidecar for `storage.deliverSignal`. A signal token grants
-- one-shot delivery rights to a public completer for a specific
-- (workflow_id, signal_name). Issued by the dashboard, by an admin tool,
-- or from inside a journaled body via `ctx.signalToken(name, opts)`.
--
-- The token doesn't change the signal mechanic — `ctx.signal` and
-- `.waitForSignal()` continue to suspend on the same `wf_workflow_signals`
-- table. The completion route validates the bearer, then calls
-- `deliverSignal(workflow_id, signal_name, value)` to resume the workflow
-- through the existing path.
--
-- `bearer` is stored plaintext (short-lived, bounded by `expires_at`,
-- single-use). The risk window is the token's TTL after which the row
-- can no longer be used for delivery.

CREATE TABLE IF NOT EXISTS wf_signal_tokens (
  token_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  signal_name TEXT NOT NULL,
  bearer TEXT NOT NULL,
  tags TEXT[] NOT NULL DEFAULT '{}',
  -- Optional caller-supplied dedup key. When set, repeating
  -- (workflow_id, idempotency_key) returns the existing row instead of
  -- allocating a new token.
  idempotency_key TEXT,
  expires_at TIMESTAMPTZ NOT NULL,
  -- Set when the token is consumed via the public completion endpoint.
  -- Idempotent: re-submitting the same bearer returns the original value.
  completed_at TIMESTAMPTZ,
  completed_value JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency-key replay lookup. Partial: only rows with a key contend.
CREATE UNIQUE INDEX IF NOT EXISTS wf_signal_tokens_idempotency_key_idx
  ON wf_signal_tokens (workflow_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Workflow-scoped listing for `runs.retrieve(workflowId).signalTokens[]`.
CREATE INDEX IF NOT EXISTS wf_signal_tokens_workflow_idx
  ON wf_signal_tokens (workflow_id);

-- Optional cleanup pass — find expired-but-uncompleted tokens to prune.
CREATE INDEX IF NOT EXISTS wf_signal_tokens_expired_idx
  ON wf_signal_tokens (expires_at)
  WHERE completed_at IS NULL;

-- Tag filtering for the dashboard. GIN index on TEXT[] handles
-- `tags && ARRAY[...]` and `tags @> ARRAY[...]` efficiently.
CREATE INDEX IF NOT EXISTS wf_signal_tokens_tags_gin_idx
  ON wf_signal_tokens USING GIN (tags);
