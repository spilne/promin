import type { WorkflowStepDefinition } from "@promin/workflow";

export interface WorkflowStepLibraryDeps {
  readonly now?: () => Date;
  readonly postgres?: PostgresQueryClient;
}

export interface PostgresQueryClient {
  queryJson(sql: string, params?: readonly unknown[]): Promise<unknown>;
}

export type WorkflowStepLibrary = (
  deps: WorkflowStepLibraryDeps,
) => ReadonlyArray<WorkflowStepDefinition>;
