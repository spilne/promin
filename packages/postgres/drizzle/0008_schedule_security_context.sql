-- Stable actor/effective references assigned to unattended schedule ticks.
-- NULL is a legacy/unassigned schedule; the Zorya host decides whether it
-- may adopt the built-in operator or must fail closed.
ALTER TABLE wf_schedules
  ADD COLUMN IF NOT EXISTS security_context JSONB;
