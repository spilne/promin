import { createWorkflowRunner, InMemoryWorkflowStorage, workflow } from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";

export async function double(input: number): Promise<number> {
  const flow = workflow<number>({ name: "double" })
    .step("multiply", ({ input }) => succeed(input * 2))
    .build();
  const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
  return runner.run({ workflow: flow, workflowId: "double-1", input });
}

if (import.meta.main) console.log(await double(21)); // 42
