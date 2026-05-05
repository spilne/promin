-- Generic typed streams — bidirectional append-only channels per workflow.
--
-- Both output streams (workflow → subscribers) and input streams
-- (subscribers → workflow) share this table; direction is purely
-- a convention at the API layer (`ctx.streams.append` writes, `read()`
-- + `peek()` consume). Chunks are append-only by `(workflow_id,
-- stream_id, chunk_index)`; subscribers replay from any `since` index
-- across reconnects.

CREATE TABLE IF NOT EXISTS wf_streams (
  workflow_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  payload JSONB NOT NULL,
  -- 'workflow' (written by ctx.streams) or 'external' (written via
  -- /api/runs/:id/streams/:streamId POST). Round-trips through `read`
  -- so subscribers can render workflow-vs-external chunks differently.
  appended_by TEXT NOT NULL DEFAULT 'workflow'
    CHECK (appended_by IN ('workflow', 'external')),
  appended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workflow_id, stream_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS wf_streams_workflow_idx
  ON wf_streams (workflow_id);

-- For SSE replay-from-index queries.
CREATE INDEX IF NOT EXISTS wf_streams_workflow_stream_idx
  ON wf_streams (workflow_id, stream_id, chunk_index);
