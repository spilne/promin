-- State machine tables
CREATE TABLE IF NOT EXISTS sm_machines (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  current_state TEXT NOT NULL,
  context JSONB NOT NULL,
  version TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sm_machines_name_idx ON sm_machines(name);
CREATE INDEX IF NOT EXISTS sm_machines_current_idx ON sm_machines(current_state);

CREATE TABLE IF NOT EXISTS sm_machine_events (
  id BIGSERIAL PRIMARY KEY,
  machine_id TEXT NOT NULL REFERENCES sm_machines(id) ON DELETE CASCADE,
  event TEXT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  context JSONB NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sm_machine_events_machine_idx ON sm_machine_events(machine_id);
