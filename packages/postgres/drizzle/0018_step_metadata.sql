-- promin-cgci: generic per-step audit metadata column.
--
-- Step-kind-specific audit data written at execution time and queryable
-- directly from SQL. First consumer is `.match()` — records the chosen
-- case so prod debugging ("why did this order route to express?") is a
-- SELECT, not a re-run of the selector against persisted prev.
--
-- Generic rather than match-specific because the same shape serves
-- subworkflow child ids, branch directions, guard failure labels, state-
-- machine transition info, etc. Pattern mirrors sm_machines.metadata.
--
-- NULL means "no audit data for this step". Existing rows migrate in
-- place via ADD COLUMN DEFAULT behaviour.

ALTER TABLE wf_workflow_steps
  ADD COLUMN IF NOT EXISTS metadata JSONB;
