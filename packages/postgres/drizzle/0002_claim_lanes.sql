-- Claim lanes: a generated column joining the columns claim's admission
-- checks read, and the indexes that let claim probe each lane's head instead
-- of scanning a throttled backlog. Every statement is idempotent. Adding the
-- stored column rewrites wf_step_queue once (ACCESS EXCLUSIVE); old writers
-- need no change because the column is generated.
ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS lane text GENERATED ALWAYS AS (coalesce('=' || namespace, '~') || chr(31) || coalesce('=' || concurrency_scope, '~') || chr(31) || coalesce('=' || concurrency_key, '~') || chr(31) || coalesce('=' || concurrency_limit::text, '~') || chr(31) || coalesce('=' || group_name, '~') || chr(31) || coalesce('=' || group_max_concurrent::text, '~') || chr(31) || coalesce('=' || group_max_starts::text, '~') || chr(31) || coalesce('=' || group_window_ms::text, '~') || chr(31) || coalesce('=' || fairness_key, '~')) STORED;
CREATE INDEX IF NOT EXISTS wf_step_queue_lane_idx ON wf_step_queue (lane, priority DESC, created_at, id) WHERE (status = 'pending'::text);
CREATE INDEX IF NOT EXISTS wf_step_queue_pending_group_idx ON wf_step_queue (group_name) WHERE ((status = 'pending'::text) AND (group_name IS NOT NULL));
