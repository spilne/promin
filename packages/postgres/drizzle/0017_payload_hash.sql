-- promin-4e36: add optional payload_hash column to wf_activity_journal.
--
-- Canonicalized-+-hashed fingerprint of the activity input, opt-in via
-- `ActivityOptions.payloadHash` or pipeline-level `payloadHash: true`. On
-- replay, the engine recomputes the hash and throws JournalNonDeterminismError
-- if the stored fingerprint disagrees — catches silent payload drift where
-- the same activity name is reused with different input across runs.
--
-- NULL means "hashing wasn't requested for this entry" — existing rows stay
-- NULL, new rows stay NULL when the activity opts out.

ALTER TABLE wf_activity_journal
  ADD COLUMN IF NOT EXISTS payload_hash TEXT;
