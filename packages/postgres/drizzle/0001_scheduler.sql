-- Durable scheduler tables

CREATE TABLE IF NOT EXISTS wf_schedules (
  id TEXT PRIMARY KEY,
  name TEXT,
  cron TEXT,
  interval_ms BIGINT,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  overlap_policy TEXT NOT NULL DEFAULT 'allow',
  max_catch_up INTEGER NOT NULL DEFAULT 0,
  jitter_ms INTEGER NOT NULL DEFAULT 0,
  enabled BOOLEAN NOT NULL DEFAULT true,
  start_at TIMESTAMPTZ,
  end_at TIMESTAMPTZ,
  metadata JSONB,
  last_fired_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS wf_schedules_enabled_idx ON wf_schedules (enabled);

CREATE TABLE IF NOT EXISTS wf_schedule_ticks (
  schedule_id TEXT NOT NULL REFERENCES wf_schedules(id) ON DELETE CASCADE,
  scheduled_at TIMESTAMPTZ NOT NULL,
  fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  catch_up BOOLEAN NOT NULL DEFAULT false,
  tick_number BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS wf_schedule_ticks_schedule_idx ON wf_schedule_ticks (schedule_id);
