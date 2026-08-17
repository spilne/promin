import { createWorkflowStepCatalog, type WorkflowStepDefinition } from "@promin/workflow";
import { controlSteps } from "./control.ts";
import { jsonSteps } from "./json.ts";
import { mergeSteps } from "./merge.ts";
import { postgresSteps } from "./postgres.ts";
import { sourceSteps } from "./source.ts";
import { systemSteps } from "./system.ts";
import { textSteps } from "./text.ts";
import type { WorkflowStepLibraryDeps } from "./types.ts";
export type { PostgresQueryClient, WorkflowStepLibrary, WorkflowStepLibraryDeps } from "./types.ts";

const libraries = [
  sourceSteps,
  jsonSteps,
  textSteps,
  mergeSteps,
  controlSteps,
  systemSteps,
  postgresSteps,
];

export function demoWorkflowSteps(deps: WorkflowStepLibraryDeps = {}): WorkflowStepDefinition[] {
  return libraries.flatMap((library) => library(deps));
}

export function createDemoWorkflowStepCatalog(deps: WorkflowStepLibraryDeps = {}) {
  return createWorkflowStepCatalog(demoWorkflowSteps(deps));
}
