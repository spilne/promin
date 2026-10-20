-- Start queue: claim in enqueue order without sorting the backlog.

-- `claim` asks for the oldest pending starts across every workflow name a
-- worker hosts (`workflow_name = ANY(...) ORDER BY enqueued_at LIMIT n`).
-- `wf_workflow_starts_pending_idx` (workflow_name, enqueued_at) can't return
-- rows of several names in enqueue order, so the planner read every pending
-- row of those names and sorted them on each claim (30 600 rows, 18 ms with
-- a 250k backlog). This index lets it walk pending starts oldest first and
-- stop after `limit` matches; the per-name index stays for workers that
-- host rare names.
CREATE INDEX IF NOT EXISTS wf_workflow_starts_pending_enqueued_idx
  ON wf_workflow_starts (enqueued_at)
  WHERE status = 'pending';
