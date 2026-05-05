-- Per-call idempotency key for `runner.run({ idempotencyKey, idempotencyKeyTTL })`.
--
-- Decouples the dedup key from the workflow_id. With the auto-mint path
-- (Zorya's `workflows.trigger()` mints workflow_id via crypto.randomUUID()
-- when the caller omits it), repeated trigger() calls produce different
-- ids — there's no way to dedup via workflow_id alone. The idempotency_key
-- column gives callers a stable string that resolves to a workflow_id so
-- repeat calls land on the same run while the key is unexpired.
--
-- The existing `workflow_id`-based dedup keeps working unchanged — this
-- column is a secondary lookup path, not a replacement.

ALTER TABLE wf_workflows
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
  ADD COLUMN IF NOT EXISTS idempotency_expires_at TIMESTAMPTZ;

-- Partial unique index — only rows with a non-null key contend. Lets
-- distinct workflow_names share key namespaces (a user-level
-- "stripe-evt-42" doesn't collide with an admin-level one). Expired
-- rows still occupy the index; the lookup query filters by `expires_at >
-- NOW()` so they don't match — and on conflict, `createWorkflow` rejects
-- the new insert and the runner falls through to attaching to the
-- existing (possibly expired-key) row, which then runs through the
-- workflow's own `idempotency.onExpiry` policy.
CREATE UNIQUE INDEX IF NOT EXISTS wf_workflows_idempotency_key_idx
  ON wf_workflows (workflow_name, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
