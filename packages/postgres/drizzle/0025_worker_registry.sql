-- Persistent WorkerRegistry table. Replaces the in-memory Map so
-- horizontally-scaled Zorya instances see the same worker fleet instead of
-- each instance tracking only workers that registered through its own
-- process.
--
-- Heartbeat is the hot write path: workers update last_heartbeat_at every
-- few seconds. detectDead scans stale rows (active OR draining) to mark
-- them dead so the runner's requeueStuck path can reclaim their tasks.

CREATE TABLE IF NOT EXISTS wf_worker_registry (
  worker_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'draining', 'dead')),
  capabilities TEXT[] NOT NULL DEFAULT '{}',
  concurrency INTEGER NOT NULL DEFAULT 1,
  metadata JSONB,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS wf_worker_registry_heartbeat_idx
  ON wf_worker_registry(last_heartbeat_at);

CREATE INDEX IF NOT EXISTS wf_worker_registry_caps_idx
  ON wf_worker_registry USING GIN(capabilities);

CREATE INDEX IF NOT EXISTS wf_worker_registry_status_idx
  ON wf_worker_registry(status);
