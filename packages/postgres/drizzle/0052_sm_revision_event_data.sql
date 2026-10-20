-- State machines: transition revision counter and event payloads.

-- ---------------------------------------------------------------------------
-- sm_machines.revision — compare-and-set counter
-- ---------------------------------------------------------------------------
-- `transition` used to compare on the state name alone, so two concurrent
-- self-loop transitions (a → a with a context update) both matched and the
-- later one overwrote the earlier. Every transition now compares on
-- (current_state, revision) and bumps revision by one. Revision also equals
-- the number of recorded transitions, so the `maxTransitions` limit reads it
-- instead of loading the machine's whole event history.

ALTER TABLE sm_machines ADD COLUMN IF NOT EXISTS revision INTEGER NOT NULL DEFAULT 0;

-- Existing machines start at their recorded transition count.
UPDATE sm_machines m
SET revision = counts.n
FROM (
  SELECT machine_id, COUNT(*)::integer AS n
  FROM sm_machine_events
  GROUP BY machine_id
) counts
WHERE counts.machine_id = m.id AND m.revision = 0;

-- ---------------------------------------------------------------------------
-- sm_machine_events.event_data — the payload passed to `send({ data })`
-- ---------------------------------------------------------------------------
-- `StateMachineStorage.transition` accepts `eventData`; Postgres dropped it.

ALTER TABLE sm_machine_events ADD COLUMN IF NOT EXISTS event_data JSONB;
