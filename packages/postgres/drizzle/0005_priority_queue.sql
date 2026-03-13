ALTER TABLE "wf_step_queue" ADD COLUMN IF NOT EXISTS "priority" integer NOT NULL DEFAULT 5;
DROP INDEX IF EXISTS "wf_step_queue_dequeue_idx";
CREATE INDEX "wf_step_queue_dequeue_idx" ON "wf_step_queue" ("status", "queue", "priority", "created_at");
