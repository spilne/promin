-- agent_fact.seq — insertion order tie-breaker
--
-- Facts are listed by created_at, a millisecond timestamp. Facts appended
-- within the same millisecond tied, and the id tie-breaker is a random
-- UUID, so they came back in arbitrary order instead of append order.
-- A bigserial records insertion order; existing rows get values in
-- whatever order Postgres assigns them, which only matters among rows
-- that already share a created_at.

ALTER TABLE agent_fact ADD COLUMN IF NOT EXISTS seq BIGSERIAL;
