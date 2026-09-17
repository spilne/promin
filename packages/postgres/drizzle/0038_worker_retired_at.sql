-- Worker retirement — deregister() now retires a worker (status='retired'
-- + a retired_at timestamp) instead of deleting the row, so the dashboard
-- and run forensics can still resolve a gracefully-stopped worker. The
-- row is kept for a retention window, then reaped by gc().

ALTER TABLE wf_worker_registry ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;

-- The status CHECK from 0025 only allowed active/draining/dead. Drop and
-- recreate it with 'retired' included.
ALTER TABLE wf_worker_registry DROP CONSTRAINT IF EXISTS wf_worker_registry_status_check;
ALTER TABLE wf_worker_registry
  ADD CONSTRAINT wf_worker_registry_status_check
  CHECK (status IN ('active', 'draining', 'dead', 'retired'));
