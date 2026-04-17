-- Phase 2b: extend wf_activity_journal with suspend/resume columns for
-- ctx.sleep and ctx.signal inside journaled steps.
--
-- step_type discriminates between activity (Phase 1), sleep, and signal.
-- phase distinguishes in-flight waits from completed entries.
-- wake_at powers the sleep scanner's partial index.

ALTER TABLE wf_activity_journal
  ADD COLUMN IF NOT EXISTS step_type TEXT NOT NULL DEFAULT 'activity'
    CHECK (step_type IN ('activity', 'sleep', 'signal')),
  ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'completed'
    CHECK (phase IN ('pending', 'completed')),
  ADD COLUMN IF NOT EXISTS wake_at TIMESTAMPTZ;

-- Allow `exit` to be NULL for pending entries. Phase 1 required exit
-- NOT NULL; make it nullable now that pending sleep/signal entries exist.
ALTER TABLE wf_activity_journal
  ALTER COLUMN exit DROP NOT NULL;

-- Sleep scanner hot path — find pending sleeps whose wake has passed.
CREATE INDEX IF NOT EXISTS wf_activity_journal_due_sleeps_idx
  ON wf_activity_journal (wake_at)
  WHERE step_type = 'sleep' AND phase = 'pending';

-- Signal delivery lookup by (workflow, step, signalName).
CREATE INDEX IF NOT EXISTS wf_activity_journal_pending_signals_idx
  ON wf_activity_journal (workflow_id, step_name, activity_name)
  WHERE step_type = 'signal' AND phase = 'pending';
