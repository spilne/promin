// ---------------------------------------------------------------------------
// Workflow lookup ID mappings — extends core string enums with integer IDs
// ---------------------------------------------------------------------------

import type { WorkflowStatus, StepStatus, StepType } from "@ts-backend/core";
import { defineLookup } from "./lookup.ts";

export const WorkflowStatusIds = defineLookup<WorkflowStatus>({
  running: 1,
  completed: 2,
  failed: 3,
  suspended: 4,
});

export const StepStatusIds = defineLookup<StepStatus>({
  pending: 1,
  running: 2,
  completed: 3,
  failed: 4,
  skipped: 5,
  sleeping: 6,
  waiting_for_signal: 7,
});

export const StepTypeIds = defineLookup<StepType>({
  single: 1,
  map: 2,
  sleep: 3,
  signal: 4,
});
