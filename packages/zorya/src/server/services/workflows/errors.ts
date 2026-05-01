export class UnknownWorkflowError extends Error {
  override readonly name = "UnknownWorkflowError";
  constructor(public readonly workflowName: string) {
    super(`Unknown workflow "${workflowName}" — no layer in the chain accepted it`);
  }
}
