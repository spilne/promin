-- Baseline schema for every promin Postgres store: workflow storage, step
-- queue, scheduler, start queue, registries, state machines, agent and eval
-- stores. `src/lib/schema.ts` and `src/lib/scheduler-schema.ts` declare the
-- same tables; the schema-drift test keeps the two in step.

CREATE TABLE agent_audit_log (
  id bigserial NOT NULL,
  namespace_id text NOT NULL,
  resource_id text NOT NULL,
  agent_id text,
  tool_name text NOT NULL,
  action text NOT NULL,
  target text,
  meta jsonb,
  recorded_at bigint NOT NULL,
  CONSTRAINT agent_audit_log_pkey PRIMARY KEY (id)
);

CREATE TABLE agent_dag (
  id text NOT NULL,
  version text NOT NULL,
  body jsonb NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_dag_pkey PRIMARY KEY (id, version)
);

CREATE TABLE agent_episode (
  id text NOT NULL,
  scope text NOT NULL,
  namespace_id text NOT NULL,
  resource_id text,
  thread_id text,
  summary text NOT NULL,
  outcome text,
  salience double precision DEFAULT 0.5 NOT NULL,
  embedding jsonb,
  source_thread_id text,
  source_msg_from_seq integer,
  source_msg_to_seq integer,
  occurred_at bigint NOT NULL,
  created_at bigint NOT NULL,
  metadata jsonb,
  CONSTRAINT agent_episode_pkey PRIMARY KEY (id)
);

CREATE TABLE agent_fact (
  id text NOT NULL,
  scope text NOT NULL,
  namespace_id text NOT NULL,
  resource_id text,
  thread_id text,
  text text NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  seq bigserial NOT NULL,
  CONSTRAINT agent_fact_pkey PRIMARY KEY (id)
);

CREATE TABLE agent_instance (
  id text NOT NULL,
  registered_agent_id text NOT NULL,
  namespace_id text NOT NULL,
  owner_id text NOT NULL,
  display_name text,
  metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
  created_at bigint NOT NULL,
  CONSTRAINT agent_instance_pkey PRIMARY KEY (id)
);

CREATE TABLE agent_message (
  namespace_id text NOT NULL,
  thread_id text NOT NULL,
  seq integer NOT NULL,
  payload jsonb NOT NULL,
  created_at bigint NOT NULL,
  CONSTRAINT agent_message_pkey PRIMARY KEY (namespace_id, thread_id, seq)
);

CREATE TABLE agent_namespace (
  namespace_id text NOT NULL,
  static_rules text,
  working_memory text,
  inherit_from_parent boolean DEFAULT true NOT NULL,
  metadata jsonb,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_namespace_pkey PRIMARY KEY (namespace_id)
);

CREATE TABLE agent_registry (
  agent_id text NOT NULL,
  version text NOT NULL,
  backend_type text NOT NULL,
  backend jsonb NOT NULL,
  metadata jsonb NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_registry_pkey PRIMARY KEY (agent_id, version)
);

CREATE TABLE agent_resource (
  namespace_id text NOT NULL,
  resource_id text NOT NULL,
  static_rules text,
  working_memory text,
  inherit_from_parent boolean DEFAULT true NOT NULL,
  metadata jsonb,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_resource_pkey PRIMARY KEY (namespace_id, resource_id)
);

CREATE TABLE agent_secret (
  scope_kind text NOT NULL,
  namespace_id text DEFAULT ''::text NOT NULL,
  resource_id text DEFAULT ''::text NOT NULL,
  secret_key text NOT NULL,
  iv text NOT NULL,
  auth_tag text NOT NULL,
  ciphertext text NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_secret_pkey PRIMARY KEY (scope_kind, namespace_id, resource_id, secret_key)
);

CREATE TABLE agent_thread (
  namespace_id text NOT NULL,
  thread_id text NOT NULL,
  resource_id text,
  title text,
  working_memory text,
  inherit_from_parent boolean DEFAULT true NOT NULL,
  metadata jsonb,
  archived_at bigint,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT agent_thread_pkey PRIMARY KEY (namespace_id, thread_id)
);

CREATE TABLE agent_thread_lease (
  namespace_id text NOT NULL,
  thread_id text NOT NULL,
  lease_id text NOT NULL,
  owner_id text NOT NULL,
  acquired_at bigint NOT NULL,
  expires_at bigint NOT NULL,
  CONSTRAINT agent_thread_lease_pkey PRIMARY KEY (namespace_id, thread_id)
);

CREATE TABLE agent_tool_history (
  name text NOT NULL,
  source_kind text NOT NULL,
  source_detail text DEFAULT ''::text NOT NULL,
  schema_hash text NOT NULL,
  description text NOT NULL,
  first_seen_at bigint NOT NULL,
  last_seen_at bigint NOT NULL,
  CONSTRAINT agent_tool_history_pkey PRIMARY KEY (name, source_kind, source_detail, schema_hash)
);

CREATE TABLE eval_dataset (
  dataset_id text NOT NULL,
  cases jsonb NOT NULL,
  CONSTRAINT eval_dataset_pkey PRIMARY KEY (dataset_id)
);

CREATE TABLE eval_run (
  run_id text NOT NULL,
  target_id text NOT NULL,
  target_version text,
  dataset_id text NOT NULL,
  ran_at bigint NOT NULL,
  summary jsonb NOT NULL,
  saved_at bigint NOT NULL,
  CONSTRAINT eval_run_pkey PRIMARY KEY (run_id)
);

CREATE TABLE fragment_store (
  key text NOT NULL,
  content text NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT fragment_store_pkey PRIMARY KEY (key)
);

CREATE TABLE role_registry (
  role_id text NOT NULL,
  version text NOT NULL,
  definition jsonb NOT NULL,
  metadata jsonb NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT role_registry_pkey PRIMARY KEY (role_id, version)
);

CREATE TABLE skill_registry (
  skill_id text NOT NULL,
  version text NOT NULL,
  description text NOT NULL,
  when_to_use text NOT NULL,
  body text NOT NULL,
  metadata jsonb NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT skill_registry_pkey PRIMARY KEY (skill_id, version)
);

CREATE TABLE sm_machine_events (
  id bigserial NOT NULL,
  machine_id text NOT NULL,
  event text NOT NULL,
  from_state text NOT NULL,
  to_state text NOT NULL,
  context jsonb NOT NULL,
  metadata jsonb,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  event_data jsonb,
  CONSTRAINT sm_machine_events_pkey PRIMARY KEY (id)
);

CREATE TABLE sm_machine_locks (
  machine_id text NOT NULL,
  locked_by text NOT NULL,
  expires_at timestamp with time zone NOT NULL,
  CONSTRAINT sm_machine_locks_pkey PRIMARY KEY (machine_id)
);

CREATE TABLE sm_machines (
  id text NOT NULL,
  name text NOT NULL,
  machine_type text,
  namespace text,
  current_state text NOT NULL,
  context jsonb NOT NULL,
  version text,
  metadata jsonb,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  updated_at timestamp with time zone DEFAULT now() NOT NULL,
  revision integer DEFAULT 0 NOT NULL,
  CONSTRAINT sm_machines_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_activity_journal (
  workflow_id text NOT NULL,
  step_name text NOT NULL,
  activity_index integer NOT NULL,
  activity_name text NOT NULL,
  exit jsonb,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  step_type text DEFAULT 'activity'::text NOT NULL,
  phase text DEFAULT 'completed'::text NOT NULL,
  wake_at timestamp with time zone,
  branch_path text DEFAULT ''::text NOT NULL,
  payload_hash text,
  CONSTRAINT wf_activity_journal_phase_check CHECK ((phase = ANY (ARRAY['pending'::text, 'completed'::text]))),
  CONSTRAINT wf_activity_journal_step_type_check CHECK ((step_type = ANY (ARRAY['activity'::text, 'sleep'::text, 'signal'::text, 'compensation'::text, 'child'::text]))),
  CONSTRAINT wf_activity_journal_pkey PRIMARY KEY (workflow_id, step_name, activity_index, branch_path)
);

CREATE TABLE wf_attempt_type (
  id integer NOT NULL,
  name text NOT NULL,
  CONSTRAINT wf_attempt_type_name_key UNIQUE (name),
  CONSTRAINT wf_attempt_type_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_leader_leases (
  lease_key text NOT NULL,
  holder text,
  epoch bigint NOT NULL,
  expires_at timestamp with time zone NOT NULL,
  CONSTRAINT wf_leader_leases_pkey PRIMARY KEY (lease_key)
);

CREATE TABLE wf_schedule_ticks (
  schedule_id text NOT NULL,
  scheduled_at timestamp with time zone NOT NULL,
  fired_at timestamp with time zone DEFAULT now() NOT NULL,
  catch_up boolean DEFAULT false NOT NULL,
  tick_number bigint DEFAULT 0 NOT NULL
);

CREATE TABLE wf_schedules (
  id text NOT NULL,
  name text,
  cron text,
  interval_ms bigint,
  timezone text DEFAULT 'UTC'::text NOT NULL,
  max_catch_up integer DEFAULT 0 NOT NULL,
  jitter_ms integer DEFAULT 0 NOT NULL,
  enabled boolean DEFAULT true NOT NULL,
  start_at timestamp with time zone,
  end_at timestamp with time zone,
  metadata jsonb,
  last_fired_at timestamp with time zone,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  updated_at timestamp with time zone DEFAULT now() NOT NULL,
  rrule text,
  namespace text,
  next_run timestamp with time zone,
  tick_count bigint DEFAULT 0 NOT NULL,
  CONSTRAINT wf_schedules_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_signal_tokens (
  token_id text NOT NULL,
  workflow_id text NOT NULL,
  signal_name text NOT NULL,
  bearer text NOT NULL,
  tags text[] DEFAULT '{}'::text[] NOT NULL,
  idempotency_key text,
  expires_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone,
  completed_value jsonb,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT wf_signal_tokens_pkey PRIMARY KEY (token_id)
);

CREATE TABLE wf_step_attempts (
  workflow_id text NOT NULL,
  step_name text NOT NULL,
  attempt integer NOT NULL,
  attempt_type_id integer NOT NULL,
  status_id integer NOT NULL,
  result jsonb,
  error text,
  duration_ms bigint,
  started_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone NOT NULL,
  run integer DEFAULT 1 NOT NULL,
  executor_id text,
  CONSTRAINT wf_step_attempts_pkey PRIMARY KEY (workflow_id, step_name, run, attempt, attempt_type_id)
);

CREATE TABLE wf_step_queue (
  id bigserial NOT NULL,
  workflow_id text NOT NULL,
  step_name text NOT NULL,
  input jsonb,
  prev_results jsonb,
  attempt integer DEFAULT 1 NOT NULL,
  status text DEFAULT 'pending'::text NOT NULL,
  result jsonb,
  error text,
  duration_ms bigint,
  claimed_by text,
  claimed_at timestamp with time zone,
  completed_at timestamp with time zone,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  priority integer DEFAULT 5 NOT NULL,
  namespace text,
  version text,
  needs text[] DEFAULT '{}'::text[] NOT NULL,
  heartbeat_at timestamp with time zone,
  metadata jsonb,
  concurrency_key text,
  concurrency_scope text,
  concurrency_limit integer,
  claim_token text,
  deliveries integer DEFAULT 0 NOT NULL,
  CONSTRAINT wf_step_queue_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_step_status (
  id integer NOT NULL,
  name text NOT NULL,
  CONSTRAINT wf_step_status_name_key UNIQUE (name),
  CONSTRAINT wf_step_status_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_step_type (
  id integer NOT NULL,
  name text NOT NULL,
  CONSTRAINT wf_step_type_name_key UNIQUE (name),
  CONSTRAINT wf_step_type_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_streams (
  workflow_id text NOT NULL,
  stream_id text NOT NULL,
  chunk_index integer NOT NULL,
  payload jsonb NOT NULL,
  appended_by text DEFAULT 'workflow'::text NOT NULL,
  appended_at timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT wf_streams_appended_by_check CHECK ((appended_by = ANY (ARRAY['workflow'::text, 'external'::text]))),
  CONSTRAINT wf_streams_pkey PRIMARY KEY (workflow_id, stream_id, chunk_index)
);

CREATE TABLE wf_worker_registry (
  worker_id text NOT NULL,
  status text DEFAULT 'active'::text NOT NULL,
  capabilities text[] DEFAULT '{}'::text[] NOT NULL,
  concurrency integer DEFAULT 1 NOT NULL,
  metadata jsonb,
  started_at timestamp with time zone DEFAULT now() NOT NULL,
  last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
  retired_at timestamp with time zone,
  CONSTRAINT wf_worker_registry_status_check CHECK ((status = ANY (ARRAY['active'::text, 'draining'::text, 'dead'::text, 'retired'::text]))),
  CONSTRAINT wf_worker_registry_pkey PRIMARY KEY (worker_id)
);

CREATE TABLE wf_workflow_advertisements (
  worker_id text NOT NULL,
  workflow_name text NOT NULL,
  version text,
  steps jsonb NOT NULL,
  sample_input jsonb,
  advertised_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE wf_workflow_locks (
  workflow_id text NOT NULL,
  locked_at timestamp with time zone DEFAULT now() NOT NULL,
  expires_at timestamp with time zone NOT NULL,
  locked_by text,
  fence_token bigserial NOT NULL,
  CONSTRAINT wf_workflow_locks_pkey PRIMARY KEY (workflow_id)
);

CREATE TABLE wf_workflow_registry (
  name text NOT NULL,
  version text NOT NULL,
  dag_json jsonb NOT NULL,
  idempotency jsonb,
  registered_at timestamp with time zone DEFAULT now() NOT NULL,
  status text DEFAULT 'inactive'::text NOT NULL,
  active_at timestamp with time zone,
  archived_at timestamp with time zone,
  content_hash text,
  CONSTRAINT wf_workflow_registry_status_check CHECK ((status = ANY (ARRAY['inactive'::text, 'active'::text, 'archived'::text]))),
  CONSTRAINT wf_workflow_registry_pkey PRIMARY KEY (name, version)
);

CREATE TABLE wf_workflow_runs (
  workflow_id text NOT NULL,
  run integer NOT NULL,
  status_id integer NOT NULL,
  result jsonb,
  error text,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  completed_at timestamp with time zone,
  started_at timestamp with time zone,
  tripwire jsonb,
  CONSTRAINT wf_workflow_runs_pkey PRIMARY KEY (workflow_id, run)
);

CREATE TABLE wf_workflow_signals (
  workflow_id text NOT NULL,
  signal_name text NOT NULL,
  payload jsonb NOT NULL,
  delivered_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE wf_workflow_starts (
  id text NOT NULL,
  workflow_id text NOT NULL,
  workflow_name text NOT NULL,
  version text,
  input jsonb NOT NULL,
  metadata jsonb,
  enqueued_at timestamp with time zone DEFAULT now() NOT NULL,
  claimed_at timestamp with time zone,
  claimed_by text,
  status text DEFAULT 'pending'::text NOT NULL,
  claim_token text,
  heartbeat_at timestamp with time zone,
  CONSTRAINT wf_workflow_starts_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_workflow_status (
  id integer NOT NULL,
  name text NOT NULL,
  CONSTRAINT wf_workflow_status_name_key UNIQUE (name),
  CONSTRAINT wf_workflow_status_pkey PRIMARY KEY (id)
);

CREATE TABLE wf_workflow_step_tasks (
  workflow_id text NOT NULL,
  step_name text NOT NULL,
  task_index integer NOT NULL,
  status_id integer DEFAULT 1 NOT NULL,
  input jsonb,
  result jsonb,
  error text,
  started_at timestamp with time zone,
  completed_at timestamp with time zone,
  attempt integer DEFAULT 0 NOT NULL,
  run integer DEFAULT 1 NOT NULL,
  CONSTRAINT wf_workflow_step_tasks_pkey PRIMARY KEY (workflow_id, step_name, run, task_index)
);

CREATE TABLE wf_workflow_steps (
  workflow_id text NOT NULL,
  step_name text NOT NULL,
  status_id integer DEFAULT 1 NOT NULL,
  step_type_id integer DEFAULT 1 NOT NULL,
  depends_on jsonb DEFAULT '[]'::jsonb NOT NULL,
  result jsonb,
  error text,
  started_at timestamp with time zone,
  completed_at timestamp with time zone,
  duration_ms bigint,
  attempt integer DEFAULT 0 NOT NULL,
  wake_at timestamp with time zone,
  signal_name text,
  signal_timeout_at timestamp with time zone,
  run integer DEFAULT 1 NOT NULL,
  metadata jsonb,
  signal_json_schema jsonb,
  error_tag text,
  compensation_status text,
  compensation_error text,
  compensated_at timestamp with time zone,
  CONSTRAINT wf_workflow_steps_compensation_status_check CHECK ((compensation_status = ANY (ARRAY['compensated'::text, 'compensation_failed'::text]))),
  CONSTRAINT wf_workflow_steps_pkey PRIMARY KEY (workflow_id, step_name, run)
);

CREATE TABLE wf_workflows (
  workflow_id text NOT NULL,
  workflow_name text NOT NULL,
  workflow_type text,
  status_id integer DEFAULT 0 NOT NULL,
  input jsonb NOT NULL,
  metadata jsonb,
  result jsonb,
  error text,
  created_at timestamp with time zone DEFAULT now() NOT NULL,
  updated_at timestamp with time zone DEFAULT now() NOT NULL,
  completed_at timestamp with time zone,
  namespace text,
  run integer DEFAULT 1 NOT NULL,
  version text,
  started_at timestamp with time zone,
  tripwire jsonb,
  idempotency_key text,
  idempotency_expires_at timestamp with time zone,
  parent_workflow_id text,
  run_source smallint,
  run_source_id text,
  error_tag text,
  CONSTRAINT wf_workflows_pkey PRIMARY KEY (workflow_id)
);

-- Indexes

CREATE INDEX agent_audit_log_ns_time_idx ON agent_audit_log (namespace_id, recorded_at);

CREATE INDEX agent_dag_id_updated_idx ON agent_dag (id, updated_at);

CREATE INDEX agent_episode_ns_idx ON agent_episode (scope, namespace_id, salience);

CREATE INDEX agent_episode_res_idx ON agent_episode (scope, namespace_id, resource_id, salience);

CREATE INDEX agent_episode_thr_idx ON agent_episode (scope, namespace_id, thread_id, created_at);

CREATE INDEX agent_fact_ns_idx ON agent_fact (scope, namespace_id, created_at);

CREATE INDEX agent_fact_res_idx ON agent_fact (scope, namespace_id, resource_id, created_at);

CREATE INDEX agent_fact_thr_idx ON agent_fact (scope, namespace_id, thread_id, created_at);

CREATE INDEX agent_instance_agent_idx ON agent_instance (registered_agent_id);

CREATE INDEX agent_instance_ns_owner_idx ON agent_instance (namespace_id, owner_id);

CREATE INDEX agent_registry_backend_type_idx ON agent_registry (backend_type);

CREATE INDEX agent_registry_id_updated_idx ON agent_registry (agent_id, updated_at);

CREATE INDEX agent_secret_scope_idx ON agent_secret (scope_kind, namespace_id, resource_id);

CREATE INDEX agent_thread_lease_expires_idx ON agent_thread_lease (expires_at);

CREATE INDEX agent_thread_resource_idx ON agent_thread (namespace_id, resource_id);

CREATE INDEX agent_tool_history_name_idx ON agent_tool_history (name, last_seen_at);

CREATE INDEX eval_run_dataset_idx ON eval_run (dataset_id);

CREATE INDEX eval_run_target_ran_idx ON eval_run (target_id, ran_at);

CREATE INDEX role_registry_id_updated_idx ON role_registry (role_id, updated_at);

CREATE INDEX skill_registry_id_updated_idx ON skill_registry (skill_id, updated_at);

CREATE INDEX sm_machine_events_machine_idx ON sm_machine_events (machine_id);

CREATE INDEX sm_machines_current_idx ON sm_machines (current_state);

CREATE INDEX sm_machines_name_idx ON sm_machines (name);

CREATE INDEX sm_machines_namespace_idx ON sm_machines (namespace);

CREATE INDEX sm_machines_type_idx ON sm_machines (machine_type);

CREATE INDEX wf_activity_journal_due_sleeps_idx ON wf_activity_journal (wake_at) WHERE ((step_type = 'sleep'::text) AND (phase = 'pending'::text));

CREATE INDEX wf_activity_journal_pending_signals_idx ON wf_activity_journal (workflow_id, step_name, activity_name) WHERE ((step_type = 'signal'::text) AND (phase = 'pending'::text));

CREATE INDEX wf_schedule_ticks_schedule_idx ON wf_schedule_ticks (schedule_id);

CREATE INDEX wf_schedules_due_idx ON wf_schedules (namespace, next_run) WHERE ((enabled = true) AND (next_run IS NOT NULL));

CREATE INDEX wf_schedules_enabled_idx ON wf_schedules (enabled);

CREATE INDEX wf_signal_tokens_expired_idx ON wf_signal_tokens (expires_at) WHERE (completed_at IS NULL);

CREATE UNIQUE INDEX wf_signal_tokens_idempotency_key_idx ON wf_signal_tokens (workflow_id, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX wf_signal_tokens_tags_gin_idx ON wf_signal_tokens USING gin (tags);

CREATE INDEX wf_signal_tokens_workflow_idx ON wf_signal_tokens (workflow_id);

CREATE UNIQUE INDEX wf_signals_workflow_signal_idx ON wf_workflow_signals (workflow_id, signal_name);

CREATE INDEX wf_step_attempts_workflow_idx ON wf_step_attempts (workflow_id);

CREATE UNIQUE INDEX wf_step_queue_active_uniq ON wf_step_queue (workflow_id, step_name) WHERE (status = ANY (ARRAY['pending'::text, 'running'::text]));

CREATE INDEX wf_step_queue_concurrency_running_idx ON wf_step_queue (concurrency_scope, concurrency_key) WHERE ((status = 'running'::text) AND (concurrency_key IS NOT NULL));

CREATE INDEX wf_step_queue_dequeue_idx ON wf_step_queue (status, priority, created_at);

CREATE INDEX wf_step_queue_namespace_idx ON wf_step_queue (namespace);

CREATE INDEX wf_step_queue_needs_idx ON wf_step_queue USING gin (needs) WHERE (status = 'pending'::text);

CREATE INDEX wf_step_queue_pending_order_idx ON wf_step_queue (priority DESC, created_at, id) WHERE (status = 'pending'::text);

CREATE INDEX wf_step_queue_running_claimed_by_idx ON wf_step_queue (claimed_by) WHERE (status = 'running'::text);

CREATE INDEX wf_step_queue_terminal_completed_idx ON wf_step_queue (completed_at) WHERE (status = ANY (ARRAY['completed'::text, 'failed'::text]));

CREATE INDEX wf_step_queue_workflow_idx ON wf_step_queue (workflow_id);

CREATE INDEX wf_worker_registry_caps_idx ON wf_worker_registry USING gin (capabilities);

CREATE INDEX wf_worker_registry_heartbeat_idx ON wf_worker_registry (last_heartbeat_at);

CREATE INDEX wf_worker_registry_status_idx ON wf_worker_registry (status);

CREATE UNIQUE INDEX wf_workflow_advertisements_worker_name_nullver_idx ON wf_workflow_advertisements (worker_id, workflow_name) WHERE (version IS NULL);

CREATE UNIQUE INDEX wf_workflow_advertisements_worker_name_version_idx ON wf_workflow_advertisements (worker_id, workflow_name, version) WHERE (version IS NOT NULL);

CREATE INDEX wf_workflow_advertisements_workflow_idx ON wf_workflow_advertisements (workflow_name, version);

CREATE UNIQUE INDEX wf_workflow_registry_active_uniq ON wf_workflow_registry (name) WHERE (status = 'active'::text);

CREATE UNIQUE INDEX wf_workflow_registry_content_hash_uniq ON wf_workflow_registry (name, content_hash) WHERE (content_hash IS NOT NULL);

CREATE INDEX wf_workflow_registry_name_idx ON wf_workflow_registry (name);

CREATE INDEX wf_workflow_starts_heartbeat_idx ON wf_workflow_starts (heartbeat_at) WHERE (status = 'claimed'::text);

CREATE INDEX wf_workflow_starts_pending_enqueued_idx ON wf_workflow_starts (enqueued_at) WHERE (status = 'pending'::text);

CREATE INDEX wf_workflow_starts_pending_idx ON wf_workflow_starts (workflow_name, enqueued_at) WHERE (status = 'pending'::text);

CREATE INDEX wf_workflow_steps_signal_timeout_idx ON wf_workflow_steps (signal_timeout_at) WHERE ((status_id = 7) AND (signal_timeout_at IS NOT NULL));

CREATE INDEX wf_workflow_steps_waiting_signal_idx ON wf_workflow_steps (workflow_id, signal_name) WHERE (status_id = 7);

CREATE INDEX wf_workflow_steps_wake_at_idx ON wf_workflow_steps (wake_at) WHERE (status_id = 6);

CREATE INDEX wf_workflows_active_idx ON wf_workflows (workflow_id) WHERE (status_id = ANY (ARRAY[0, 1, 5]));

CREATE UNIQUE INDEX wf_workflows_idempotency_key_idx ON wf_workflows (COALESCE(namespace, ''::text), workflow_name, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX wf_workflows_name_status_idx ON wf_workflows (workflow_name, status_id);

CREATE INDEX wf_workflows_namespace_started_at_idx ON wf_workflows (namespace, started_at DESC NULLS LAST);

CREATE INDEX wf_workflows_parent_idx ON wf_workflows (parent_workflow_id) WHERE (parent_workflow_id IS NOT NULL);

CREATE INDEX wf_workflows_purge_idx ON wf_workflows (completed_at) WHERE (status_id = ANY (ARRAY[2, 3, 6]));

CREATE INDEX wf_workflows_run_source_idx ON wf_workflows (run_source, run_source_id) WHERE (run_source IS NOT NULL);

CREATE INDEX wf_workflows_started_at_idx ON wf_workflows (started_at DESC NULLS LAST);

CREATE INDEX wf_workflows_status_idx ON wf_workflows (status_id);

CREATE INDEX wf_workflows_type_idx ON wf_workflows (workflow_type);

-- Foreign keys

ALTER TABLE sm_machine_events
  ADD CONSTRAINT sm_machine_events_machine_id_fkey FOREIGN KEY(machine_id) REFERENCES sm_machines(id) ON DELETE CASCADE;

ALTER TABLE wf_activity_journal
  ADD CONSTRAINT wf_activity_journal_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

ALTER TABLE wf_schedule_ticks
  ADD CONSTRAINT wf_schedule_ticks_schedule_id_fkey FOREIGN KEY(schedule_id) REFERENCES wf_schedules(id) ON DELETE CASCADE;

ALTER TABLE wf_signal_tokens
  ADD CONSTRAINT wf_signal_tokens_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

ALTER TABLE wf_streams
  ADD CONSTRAINT wf_streams_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

ALTER TABLE wf_workflow_runs
  ADD CONSTRAINT wf_workflow_runs_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

ALTER TABLE wf_workflow_signals
  ADD CONSTRAINT wf_workflow_signals_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

ALTER TABLE wf_workflow_steps
  ADD CONSTRAINT wf_workflow_steps_workflow_id_fkey FOREIGN KEY(workflow_id) REFERENCES wf_workflows(workflow_id) ON DELETE CASCADE;

-- Lookup values (also re-seeded by `migrate()`)

INSERT INTO wf_attempt_type (id, name) VALUES (1, 'execution');
INSERT INTO wf_attempt_type (id, name) VALUES (2, 'compensation');
INSERT INTO wf_step_status (id, name) VALUES (1, 'pending');
INSERT INTO wf_step_status (id, name) VALUES (2, 'running');
INSERT INTO wf_step_status (id, name) VALUES (3, 'completed');
INSERT INTO wf_step_status (id, name) VALUES (4, 'failed');
INSERT INTO wf_step_status (id, name) VALUES (5, 'skipped');
INSERT INTO wf_step_status (id, name) VALUES (6, 'sleeping');
INSERT INTO wf_step_status (id, name) VALUES (7, 'waiting_for_signal');
INSERT INTO wf_step_status (id, name) VALUES (8, 'compensated');
INSERT INTO wf_step_status (id, name) VALUES (9, 'compensation_failed');
INSERT INTO wf_step_type (id, name) VALUES (1, 'single');
INSERT INTO wf_step_type (id, name) VALUES (2, 'map');
INSERT INTO wf_step_type (id, name) VALUES (3, 'sleep');
INSERT INTO wf_step_type (id, name) VALUES (4, 'signal');
INSERT INTO wf_workflow_status (id, name) VALUES (0, 'pending');
INSERT INTO wf_workflow_status (id, name) VALUES (1, 'running');
INSERT INTO wf_workflow_status (id, name) VALUES (2, 'completed');
INSERT INTO wf_workflow_status (id, name) VALUES (3, 'failed');
INSERT INTO wf_workflow_status (id, name) VALUES (4, 'suspended');
INSERT INTO wf_workflow_status (id, name) VALUES (5, 'compensating');
INSERT INTO wf_workflow_status (id, name) VALUES (6, 'tripwire');
