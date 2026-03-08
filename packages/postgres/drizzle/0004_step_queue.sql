CREATE TABLE IF NOT EXISTS "wf_step_queue" (
  "id" bigserial PRIMARY KEY,
  "workflow_id" text NOT NULL,
  "step_name" text NOT NULL,
  "queue" text NOT NULL DEFAULT 'default',
  "input" jsonb,
  "prev_results" jsonb,
  "attempt" integer NOT NULL DEFAULT 1,
  "status" text NOT NULL DEFAULT 'pending',
  "result" jsonb,
  "error" text,
  "duration_ms" bigint,
  "claimed_by" text,
  "claimed_at" timestamp with time zone,
  "completed_at" timestamp with time zone,
  "created_at" timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "wf_step_queue_dequeue_idx" ON "wf_step_queue" ("status", "queue", "created_at");
CREATE INDEX IF NOT EXISTS "wf_step_queue_workflow_idx" ON "wf_step_queue" ("workflow_id");
