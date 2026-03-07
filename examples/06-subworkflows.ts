/**
 * Subworkflows — child workflow composition
 *
 * invoke() returns Pipeline for use inside any step.
 * .subworkflow() is builder sugar for the common pattern.
 * Parent-child tracking enables cascade cancel and querying.
 */

import { workflow, Pipeline, InMemoryWorkflowStorage } from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// Define reusable child workflows
const enrichUser = workflow<{ userId: string }>({ name: "enrich-user", storage })
  .step("fetch-profile", ({ input }) =>
    Pipeline.succeed({ userId: input.userId, name: "Alice", score: 95 }),
  )
  .build();

const sendNotification = workflow<{ email: string; message: string }>({
  name: "send-notification",
  storage,
})
  .step("send", ({ input }) =>
    Pipeline.succeed({ sent: true, to: input.email }),
  )
  .build();

// .subworkflow() — builder sugar
async function subworkflowSugar() {
  const result = await workflow<{ userId: string }>({ name: "onboard", storage })
    .step("create-account", ({ input }) =>
      Pipeline.succeed({ id: input.userId, email: "alice@example.com" }),
    )
    .subworkflow("enrich", enrichUser, {
      input: (prev) => ({ userId: prev.id }),
      workflowId: (prev) => `enrich-${prev.id}`,
    })
    .step("format", ({ prev }) => Pipeline.succeed(`${prev.name}: ${prev.score}`))
    .run({ workflowId: "onboard-1", input: { userId: "u_42" } });

  console.log(result); // "Alice: 95"
}

// invoke() — primitive for use inside any step
async function invokePrimitive() {
  const result = await workflow<{ userId: string }>({ name: "parent", storage })
    .step("create", ({ input }) =>
      Pipeline.succeed({ id: input.userId, email: "alice@example.com" }),
    )
    .step("enrich-and-notify", ({ prev }) =>
      // Compose multiple child workflows in a single step
      enrichUser
        .invoke({ workflowId: `enrich-${prev.id}`, input: { userId: prev.id } })
        .flatMap((enriched) =>
          sendNotification.invoke({
            workflowId: `notify-${prev.id}`,
            input: { email: prev.email, message: `Welcome ${enriched.name}!` },
          }),
        ),
    )
    .run({ workflowId: "parent-2", input: { userId: "u_43" } });

  console.log(result); // { sent: true, to: "alice@example.com" }
}

// Fan-out with child workflows — mapOver + invoke
async function fanOutSubworkflows() {
  const processItem = workflow<{ itemId: string }>({ name: "process-item", storage })
    .step("process", ({ input }) => Pipeline.succeed(`processed-${input.itemId}`))
    .build();

  const result = await workflow<{ items: string[] }>({ name: "batch", storage })
    .step("get-items", ({ input }) => Pipeline.succeed(input.items))
    .mapOver("process-all", { array: "get-items", concurrency: 5 }, (itemId) =>
      processItem.invoke({ workflowId: `item-${itemId}`, input: { itemId } }),
    )
    .run({ workflowId: "batch-1", input: { items: ["a", "b", "c"] } });

  console.log(result); // ["processed-a", "processed-b", "processed-c"]
}

// Parent-child tracking
async function parentChildTracking() {
  await workflow<{ userId: string }>({ name: "parent", storage })
    .step("create", ({ input }) => Pipeline.succeed({ id: input.userId }))
    .subworkflow("enrich", enrichUser, {
      input: (prev) => ({ userId: prev.id }),
      workflowId: (prev) => `enrich-${prev.id}`,
    })
    .run({ workflowId: "parent-3", input: { userId: "u_44" } });

  // Query children
  const children = await storage.listWorkflows({ parentId: "parent-3" });
  console.log(`Parent has ${children.length} children`);

  // Cascade cancel — cancels parent + all children
  await storage.cancelWorkflow("parent-3", { cascade: true });
}

export { subworkflowSugar, invokePrimitive, fanOutSubworkflows, parentChildTracking };
