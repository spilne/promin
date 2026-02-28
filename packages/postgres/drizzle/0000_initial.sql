-- Initial schema for @ts-backend/postgres workflow storage
-- Generated from Drizzle schema definitions in src/lib/schema.ts

-- Lookup tables (integer IDs for status/type compaction)
CREATE TABLE IF NOT EXISTS wf_workflow_status (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS wf_step_status (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS wf_step_type (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Core workflow table
CREATE TABLE IF NOT EXISTS wf_workflows (
  workflow_id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  workflow_type TEXT,
  status_id INTEGER NOT NULL DEFAULT 1,
  input JSONB NOT NULL,
  metadata JSONB,
  result JSONB,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS wf_workflows_name_status_idx ON wf_workflows (workflow_name, status_id);
CREATE INDEX IF NOT EXISTS wf_workflows_status_idx ON wf_workflows (status_id);
CREATE INDEX IF NOT EXISTS wf_workflows_type_idx ON wf_workflows (workflow_type);

-- Workflow steps
CREATE TABLE IF NOT EXISTS wf_workflow_steps (
  workflow_id TEXT NOT NULL REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE,
  step_name TEXT NOT NULL,
  status_id INTEGER NOT NULL DEFAULT 1,
  step_type_id INTEGER NOT NULL DEFAULT 1,
  depends_on JSONB NOT NULL DEFAULT '[]',
  result JSONB,
  error TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  duration_ms BIGINT,
  attempt INTEGER NOT NULL DEFAULT 0,
  wake_at TIMESTAMPTZ,
  signal_name TEXT,
  signal_timeout_at TIMESTAMPTZ,
  PRIMARY KEY (workflow_id, step_name)
);

-- Map step tasks
CREATE TABLE IF NOT EXISTS wf_workflow_step_tasks (
  workflow_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  task_index INTEGER NOT NULL,
  status_id INTEGER NOT NULL DEFAULT 1,
  input JSONB,
  result JSONB,
  error TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  attempt INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (workflow_id, step_name, task_index)
);

-- Workflow signals
CREATE TABLE IF NOT EXISTS wf_workflow_signals (
  workflow_id TEXT NOT NULL REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE,
  signal_name TEXT NOT NULL,
  payload JSONB NOT NULL,
  delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS wf_signals_workflow_idx ON wf_workflow_signals (workflow_id);
CREATE UNIQUE INDEX IF NOT EXISTS wf_signals_workflow_signal_idx ON wf_workflow_signals (workflow_id, signal_name);

-- Row-based lock fallback
CREATE TABLE IF NOT EXISTS wf_workflow_locks (
  workflow_id TEXT PRIMARY KEY,
  locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  locked_by TEXT
);
