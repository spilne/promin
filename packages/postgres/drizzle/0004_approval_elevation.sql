ALTER TABLE agent_pending_approval
  ADD COLUMN IF NOT EXISTS elevated boolean NOT NULL DEFAULT false;
