/**
 * Observability — lifecycle hooks, workflow queries, DAG visualization
 */

import {
  workflow,
  flow,
  Pipeline,
  InMemoryWorkflowStorage,
  dagToMermaid,
  dagToDot,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// Lifecycle hooks — observe step and workflow events
async function lifecycleHooks() {
  const result = await workflow<{ n: number }>({
    name: "observable",
    storage,
    hooks: {
      onStepComplete: ({ stepName, durationMs }) => {
        console.log(`Step "${stepName}" completed in ${durationMs}ms`);
      },
      onStepFailure: ({ stepName, error }) => {
        console.error(`Step "${stepName}" failed:`, error);
      },
      onWorkflowComplete: ({ workflowId, durationMs }) => {
        console.log(`Workflow ${workflowId} completed in ${durationMs}ms`);
      },
      onWorkflowFailure: ({ workflowId, error }) => {
        console.error(`Workflow ${workflowId} failed:`, error);
      },
    },
  })
    .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
    .step("add", ({ prev }) => Pipeline.succeed(prev + 1))
    .run({ workflowId: "obs-1", input: { n: 5 } });

  console.log("Result:", result); // 11
}

// DAG visualization — export to Mermaid or DOT
function dagVisualization() {
  const builder = flow<{ text: string }>("analyze")
    .step("parse", ({ input }) => Pipeline.succeed(input.text))
    .step("summarize", { dependsOn: ["parse"] }, ({ deps }) =>
      Pipeline.succeed(deps.parse),
    )
    .step("keywords", { dependsOn: ["parse"] }, ({ deps }) =>
      Pipeline.succeed(deps.parse),
    )
    .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) =>
      Pipeline.succeed(deps),
    );

  const dag = builder.toJSON();

  // Mermaid (paste into mermaid.live)
  console.log("Mermaid:");
  console.log(dagToMermaid(dag));
  // graph LR
  //     parse["parse"]
  //     summarize["summarize"]
  //     parse --> summarize
  //     ...

  // DOT / Graphviz (paste into viz-js.com)
  console.log("\nDOT:");
  console.log(dagToDot(dag));
  // digraph "analyze" {
  //     "parse";
  //     "summarize";
  //     "parse" -> "summarize";
  //     ...
  // }
}

export { lifecycleHooks, dagVisualization };
