// ---------------------------------------------------------------------------
// Workflow lookup ID mappings — extends core string enums with integer IDs
// ---------------------------------------------------------------------------

import type { WorkflowStatus, StepStatus, StepType, StepAttemptType } from "@promin/core";
import { defineLookup } from "./lookup.ts";

export const WorkflowStatusIds = defineLookup<WorkflowStatus>({
  pending: 0,
  running: 1,
  completed: 2,
  failed: 3,
  suspended: 4,
  compensating: 5,
});

export const StepStatusIds = defineLookup<StepStatus>({
  pending: 1,
  running: 2,
  completed: 3,
  failed: 4,
  skipped: 5,
  sleeping: 6,
  waiting_for_signal: 7,
  compensated: 8,
  compensation_failed: 9,
});

export const StepTypeIds = defineLookup<StepType>({
  single: 1,
  map: 2,
  sleep: 3,
  signal: 4,
});

export const AttemptTypeIds = defineLookup<StepAttemptType>({
  execution: 1,
  compensation: 2,
});
