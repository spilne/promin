-- Add run counter for workflow idempotency (fresh re-execution with history)
ALTER TABLE "wf_workflows" ADD COLUMN "run" integer NOT NULL DEFAULT 1;

-- Steps: add run column and update primary key
ALTER TABLE "wf_workflow_steps" ADD COLUMN "run" integer NOT NULL DEFAULT 1;
ALTER TABLE "wf_workflow_steps" DROP CONSTRAINT "wf_workflow_steps_pkey";
ALTER TABLE "wf_workflow_steps" ADD PRIMARY KEY ("workflow_id", "step_name", "run");

-- Tasks: add run column and update primary key
ALTER TABLE "wf_workflow_step_tasks" ADD COLUMN "run" integer NOT NULL DEFAULT 1;
ALTER TABLE "wf_workflow_step_tasks" DROP CONSTRAINT "wf_workflow_step_tasks_pkey";
ALTER TABLE "wf_workflow_step_tasks" ADD PRIMARY KEY ("workflow_id", "step_name", "run", "task_index");

-- Step attempts: add run column and update primary key
ALTER TABLE "wf_step_attempts" ADD COLUMN "run" integer NOT NULL DEFAULT 1;
ALTER TABLE "wf_step_attempts" DROP CONSTRAINT "wf_step_attempts_pkey";
ALTER TABLE "wf_step_attempts" ADD PRIMARY KEY ("workflow_id", "step_name", "run", "attempt", "attempt_type_id");
