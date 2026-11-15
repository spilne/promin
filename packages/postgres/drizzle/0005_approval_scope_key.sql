ALTER TABLE agent_pending_approval
  ADD COLUMN IF NOT EXISTS decided_by_principal jsonb;

ALTER TABLE agent_pending_approval
  DROP CONSTRAINT IF EXISTS agent_pending_approval_pkey;

ALTER TABLE agent_pending_approval
  DROP CONSTRAINT IF EXISTS agent_pending_approval_scope_key;
ALTER TABLE agent_pending_approval
  ADD CONSTRAINT agent_pending_approval_scope_key
  UNIQUE NULLS NOT DISTINCT (namespace_id, agent_id, resource_id, thread_id, tool_call_id);

DROP INDEX IF EXISTS agent_pending_approval_ns_status_idx;
CREATE INDEX agent_pending_approval_ns_status_idx
  ON agent_pending_approval
  (namespace_id, agent_id, resource_id, status, requested_at);

DROP INDEX IF EXISTS agent_pending_approval_thread_status_idx;
CREATE INDEX agent_pending_approval_thread_status_idx
  ON agent_pending_approval
  (namespace_id, agent_id, resource_id, thread_id, status);

DROP INDEX IF EXISTS agent_pending_approval_thread_open_idx;
CREATE INDEX agent_pending_approval_thread_open_idx
  ON agent_pending_approval
  (namespace_id, agent_id, resource_id, thread_id, delivered_at);
