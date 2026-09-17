-- Add `metadata` jsonb to wf_step_queue, mirroring wf_workflows.metadata.
-- Generic place for callers (agents, scheduling, custom workloads) to
-- attach search attributes — userId, subject, tags, experiment flags. The
-- platform never reads keys here for control flow; it round-trips through
-- enqueue → claim unchanged.
--
-- No index created here. Add a GIN (jsonb_path_ops) or per-key expression
-- index when a query path becomes hot — speculative indexes on a low-
-- cardinality observability column hurt write throughput more than they
-- help reads.

ALTER TABLE wf_step_queue ADD COLUMN IF NOT EXISTS metadata JSONB;
