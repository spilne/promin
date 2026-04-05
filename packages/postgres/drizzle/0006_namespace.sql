ALTER TABLE "wf_workflows" ADD COLUMN IF NOT EXISTS "namespace" text;
CREATE INDEX IF NOT EXISTS "wf_workflows_namespace_idx" ON "wf_workflows" ("namespace");
ALTER TABLE "wf_step_queue" ADD COLUMN IF NOT EXISTS "namespace" text;
CREATE INDEX IF NOT EXISTS "wf_step_queue_namespace_idx" ON "wf_step_queue" ("namespace");
