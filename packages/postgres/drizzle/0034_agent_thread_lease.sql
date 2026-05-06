-- Agent thread lease — per-(namespace, thread) coordination so two
-- replicas don't run a turn on the same conversation concurrently.
-- One row per leased thread. `expires_at` defines the failover window:
-- a crashed worker loses its lease at that timestamp; the next acquire
-- by anyone else steals it.
--
-- Atomic claim semantics: INSERT ... ON CONFLICT DO UPDATE WHERE
-- expires_at <= now(). Returns the new row only on first-acquire or
-- successful steal; lost contention is detected when the returned
-- lease_id doesn't match what we tried to insert (in which case the
-- caller does a separate read to surface the current owner).

CREATE TABLE IF NOT EXISTS agent_thread_lease (
  namespace_id TEXT NOT NULL,
  thread_id    TEXT NOT NULL,
  lease_id     TEXT NOT NULL,
  owner_id     TEXT NOT NULL,
  acquired_at  BIGINT NOT NULL,
  expires_at   BIGINT NOT NULL,
  PRIMARY KEY (namespace_id, thread_id)
);

-- Hot-path index for steal-expired and observability scans
-- ("which leases are about to expire").
CREATE INDEX IF NOT EXISTS agent_thread_lease_expires_idx
  ON agent_thread_lease (expires_at);
