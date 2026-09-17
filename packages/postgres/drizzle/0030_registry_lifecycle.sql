-- Promote/rollback lifecycle on `wf_workflow_registry`. Each registered
-- (name, version) gets a status — `inactive` (registered but not the
-- one routed to), `active` (the chosen version for new starts), or
-- `archived` (rolled back / drained). The partial unique index makes
-- "at most one active per workflow name" a DB-level invariant.
--
-- This replaces the implicit "latest registered = active" rule with an
-- explicit pointer the dashboard + auto-mint trigger path can drive.
-- Backwards-compatible: existing rows default to `inactive` and the
-- runtime falls back to `latest()` when no `active` is set, so nothing
-- breaks until a caller starts using `promote`.

ALTER TABLE wf_workflow_registry
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'inactive'
    CHECK (status IN ('inactive', 'active', 'archived')),
  ADD COLUMN IF NOT EXISTS active_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS content_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS wf_workflow_registry_active_uniq
  ON wf_workflow_registry (name)
  WHERE status = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS wf_workflow_registry_content_hash_uniq
  ON wf_workflow_registry (name, content_hash)
  WHERE content_hash IS NOT NULL;
