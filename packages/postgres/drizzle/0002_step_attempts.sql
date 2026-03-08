-- New lookup values for compensation statuses
INSERT INTO "wf_workflow_status" ("id", "name") VALUES (5, 'compensating') ON CONFLICT ("name") DO UPDATE SET "id" = excluded."id";
INSERT INTO "wf_step_status" ("id", "name") VALUES (8, 'compensated') ON CONFLICT ("name") DO UPDATE SET "id" = excluded."id";
INSERT INTO "wf_step_status" ("id", "name") VALUES (9, 'compensation_failed') ON CONFLICT ("name") DO UPDATE SET "id" = excluded."id";

-- Attempt type lookup table
CREATE TABLE IF NOT EXISTS "wf_attempt_type" (
  "id" integer PRIMARY KEY,
  "name" text NOT NULL UNIQUE
);
INSERT INTO "wf_attempt_type" ("id", "name") VALUES (1, 'execution') ON CONFLICT ("name") DO UPDATE SET "id" = excluded."id";
INSERT INTO "wf_attempt_type" ("id", "name") VALUES (2, 'compensation') ON CONFLICT ("name") DO UPDATE SET "id" = excluded."id";

-- Step attempts history table
CREATE TABLE IF NOT EXISTS "wf_step_attempts" (
  "workflow_id" text NOT NULL,
  "step_name" text NOT NULL,
  "attempt" integer NOT NULL,
  "attempt_type_id" integer NOT NULL,
  "status_id" integer NOT NULL,
  "result" jsonb,
  "error" text,
  "duration_ms" bigint,
  "started_at" timestamp with time zone NOT NULL,
  "completed_at" timestamp with time zone NOT NULL,
  CONSTRAINT "wf_step_attempts_pkey" PRIMARY KEY ("workflow_id", "step_name", "attempt", "attempt_type_id")
);

CREATE INDEX IF NOT EXISTS "wf_step_attempts_workflow_idx" ON "wf_step_attempts" ("workflow_id");
