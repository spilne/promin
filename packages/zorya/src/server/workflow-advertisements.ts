// ---------------------------------------------------------------------------
// Re-export of WorkflowAdvertisementRegistry from @promin/workflow.
//
// Kept under this path so existing zorya-internal imports keep working.
// The interface itself lives in @promin/workflow because (a) it's a
// coordination primitive (sibling of StepQueue, WorkerRegistry) and
// (b) backend implementations like @promin/sqlite need to depend on it
// without creating a cycle through @promin/zorya.
// ---------------------------------------------------------------------------

export {
  type WorkflowAdvertisementRegistry,
  type AdvertisedWorkflow,
  type AdvertisementEntry,
  InMemoryWorkflowAdvertisementRegistry,
} from "@promin/workflow";
