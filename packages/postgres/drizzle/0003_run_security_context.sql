-- Stable actor/effective references used to reauthorize durable execution.
-- Nullable so runs created before this migration remain readable.
ALTER TABLE wf_workflows ADD COLUMN IF NOT EXISTS security_context jsonb;
