/**
 * Visual editor schema — compile JSON workflows into WorkflowDefinitions
 *
 * A node-based UI produces WorkflowSchema JSON. The compiler validates
 * the DAG, resolves activity references, and builds an executable workflow.
 */

import {
  Pipeline,
  InMemoryWorkflowStorage,
  MapActivityRegistry,
  compileWorkflow,
  validateWorkflowSchema,
  validateWorkflowSchemaSafe,
  type WorkflowSchema,
} from "@promin/core";

// 1. Define an activity registry — maps names to Pipeline functions
const registry = new MapActivityRegistry({
  "transform.uppercase": () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),

  "transform.reverse": () => (ctx) =>
    Pipeline.succeed(String(ctx.prev).split("").reverse().join("")),

  "transform.concat": (config) => (ctx) => {
    const separator = (config?.separator as string) ?? " ";
    const deps = ctx.deps as Record<string, string>;
    return Pipeline.succeed(Object.values(deps).join(separator));
  },

  "data.split": (config) => (ctx) => {
    const separator = (config?.separator as string) ?? " ";
    return Pipeline.succeed(String(ctx.prev).split(separator));
  },
});

const storage = new InMemoryWorkflowStorage();

// 2. Compile a workflow from JSON (as produced by a visual editor)
async function compileFromJson() {
  const schema: WorkflowSchema = {
    version: 1,
    name: "text-pipeline",
    steps: [
      {
        type: "step",
        name: "uppercase",
        dependsOn: [],
        activityRef: "transform.uppercase",
      },
      {
        type: "step",
        name: "reverse",
        dependsOn: ["uppercase"],
        activityRef: "transform.reverse",
      },
    ],
    ui: {
      uppercase: { x: 0, y: 100, label: "To Uppercase" },
      reverse: { x: 200, y: 100, label: "Reverse" },
    },
  };

  const definition = compileWorkflow({ schema, storage, registry });

  const result = await definition.run({ workflowId: "visual-1", input: "hello world" });
  console.log(result); // "DLROW OLLEH"
}

// 3. Diamond DAG from JSON
async function compileDiamondDag() {
  const schema: WorkflowSchema = {
    version: 1,
    name: "diamond",
    steps: [
      { type: "step", name: "start", dependsOn: [], activityRef: "transform.uppercase" },
      { type: "step", name: "left", dependsOn: ["start"], activityRef: "transform.reverse" },
      {
        type: "step",
        name: "right",
        dependsOn: ["start"],
        activityRef: "data.split",
        config: { separator: " " },
      },
      {
        type: "step",
        name: "join",
        dependsOn: ["left", "right"],
        activityRef: "transform.concat",
        config: { separator: " | " },
      },
    ],
  };

  const definition = compileWorkflow({ schema, storage, registry });
  const result = await definition.run({ workflowId: "diamond-1", input: "hello world" });
  console.log(result);
}

// 4. Validate untrusted schema from API
async function validateFromApi() {
  const untrustedJson = {
    version: 1,
    name: "from-api",
    steps: [
      { type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" },
    ],
  };

  // Throws ZodError if invalid
  const validated = validateWorkflowSchema(untrustedJson);
  console.log("Valid schema:", validated.name);

  // Safe version — no throw
  const result = validateWorkflowSchemaSafe({ version: 2, name: "", steps: [] });
  if (!result.success) {
    console.log("Validation errors:", result.error.issues.length);
  }
}

// 5. Register activities at runtime (plugins)
async function dynamicRegistry() {
  registry.register("custom.greet", (config) => (ctx) => {
    const greeting = (config?.greeting as string) ?? "Hello";
    return Pipeline.succeed(`${greeting}, ${ctx.prev}!`);
  });

  console.log("Available activities:", registry.list());
  // [..., "custom.greet"]
}

export { compileFromJson, compileDiamondDag, validateFromApi, dynamicRegistry };
