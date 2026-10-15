-- Leader leases: TTL'd, fenced leadership over a text key.
--
-- The scheduler used a session `pg_try_advisory_lock` through a connection
-- pool: the lock stuck to whichever pooled connection first took it, the
-- same process's other connections were refused, the TTL was ignored and
-- nothing released it, so a stopped leader never handed over. A lease row
-- with a server-side expiry does both, and `epoch` is a fencing token: it
-- goes up whenever a new lease starts on the key, and fenced writes are
-- rejected unless they carry the current epoch.
--
-- `holder` is NULL once the lease is released. Rows are never deleted, so
-- the epoch keeps increasing across expiries and releases.

CREATE TABLE IF NOT EXISTS wf_leader_leases (
  lease_key  TEXT PRIMARY KEY,
  holder     TEXT,
  epoch      BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
