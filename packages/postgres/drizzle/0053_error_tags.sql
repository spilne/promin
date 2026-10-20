-- Error tags on failed runs and steps.
--
-- `failWorkflow` / `saveStepFailure` store the `_tag` of the error that
-- failed the run or step next to its message, so `WorkflowHandle.result()`
-- and status reads keep the original error type across processes.
-- `cancelWorkflow` stores 'WorkflowCancelledError'. Null for untagged
-- errors and for rows written before this migration.

ALTER TABLE wf_workflows ADD COLUMN IF NOT EXISTS error_tag TEXT;
ALTER TABLE wf_workflow_steps ADD COLUMN IF NOT EXISTS error_tag TEXT;
