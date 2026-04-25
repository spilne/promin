-- Tripwire support: a structured early-exit distinct from `failed`. Used
-- when a `.tripwire()` step fires, the workflow ends, and the outcome is a
-- business signal (fraud, rate-limited, no-op) rather than an error.
--
-- Two additions:
--   1) `tripwire` column on wf_workflows to store the opaque reason payload
--      returned by the firing step's `reason(prev)`. Null for non-tripwire
--      workflows, queryable via `tripwire->>'code'` etc.
--   2) `tripwire` entry in wf_workflow_status lookup so `status_id = 6`
--      maps to the new state. Seeded at the same time as the column add so
--      the app's WorkflowStatusIds.id.tripwire resolves on first use.

ALTER TABLE wf_workflows
  ADD COLUMN IF NOT EXISTS tripwire JSONB;

INSERT INTO wf_workflow_status (id, name)
  VALUES (6, 'tripwire')
  ON CONFLICT (id) DO NOTHING;
