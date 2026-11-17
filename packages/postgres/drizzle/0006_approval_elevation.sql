ALTER TABLE agent_pending_approval
  ADD COLUMN IF NOT EXISTS elevated boolean;
ALTER TABLE agent_pending_approval
  ADD COLUMN IF NOT EXISTS request_context jsonb;
