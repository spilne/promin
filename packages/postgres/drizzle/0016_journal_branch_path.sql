-- promin-plif: widen wf_activity_journal PK with branch_path to support
-- ctx.parallel / ctx.race concurrent branches inside a journaled step.
--
-- branch_path = '' means "at top-level in the body" — every pre-parallel row
-- gets the default via ADD COLUMN, so existing data migrates in place.
-- Parallel branches encode positions like '0', '1', '2.3' for nested trees.
-- Including it in the PK lets concurrent branches share the same
-- activity_index without collision.
--
-- Also widen step_type's CHECK to include 'compensation' (promin-9dtj) — the
-- 0014 migration predates that stepType and its CHECK would reject writes.

ALTER TABLE wf_activity_journal
  ADD COLUMN IF NOT EXISTS branch_path TEXT NOT NULL DEFAULT '';

-- Rebuild the PK to include branch_path.
ALTER TABLE wf_activity_journal
  DROP CONSTRAINT IF EXISTS wf_activity_journal_pkey;
ALTER TABLE wf_activity_journal
  ADD PRIMARY KEY (workflow_id, step_name, activity_index, branch_path);

-- Relax step_type CHECK to allow the new 'compensation' value from
-- promin-9dtj alongside the Phase 2b set.
ALTER TABLE wf_activity_journal
  DROP CONSTRAINT IF EXISTS wf_activity_journal_step_type_check;
ALTER TABLE wf_activity_journal
  ADD CONSTRAINT wf_activity_journal_step_type_check
  CHECK (step_type IN ('activity', 'sleep', 'signal', 'compensation'));
