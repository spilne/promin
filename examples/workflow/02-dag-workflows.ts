/**
 * DAG workflows — steps with explicit dependencies
 *
 * Steps that share a dependency run in parallel automatically.
 * The engine resolves the DAG via topological sort and computes
 * the ready set on each iteration.
 */

import { flow, Pipeline } from "@promin/core";

// Diamond DAG — summarize and keywords run in parallel
async function diamondDag() {
  const result = await flow<{ text: string }>("analyze")
    .step("parse", ({ input }) => Pipeline.succeed(input.text))
    .step("summarize", { dependsOn: ["parse"] }, ({ deps }) =>
      Pipeline.succeed(`Summary of: ${deps.parse.slice(0, 50)}`),
    )
    .step("keywords", { dependsOn: ["parse"] }, ({ deps }) =>
      Pipeline.succeed(deps.parse.split(" ").slice(0, 5)),
    )
    .step("publish", { dependsOn: ["summarize", "keywords"] }, ({ deps }) =>
      Pipeline.succeed({
        summary: deps.summarize,
        keywords: deps.keywords,
      }),
    )
    .execute({ text: "The quick brown fox jumps over the lazy dog" });

  console.log(result);
  // { summary: "Summary of: The quick brown fox...", keywords: ["The", "quick", "brown", "fox", "jumps"] }
}

// Fan-out with mapOver — process array elements in parallel
async function fanOut() {
  const result = await flow<{ urls: string[] }>("batch-fetch")
    .step("get-urls", ({ input }) => Pipeline.succeed(input.urls))
    .mapOver("fetch-all", { array: "get-urls", concurrency: 3 }, (url) =>
      Pipeline.succeed(`Response from ${url}`),
    )
    .execute({ urls: ["https://a.com", "https://b.com", "https://c.com"] });

  console.log(result); // ["Response from https://a.com", ...]
}

// Conditional branching
async function branching() {
  const result = await flow<{ amount: number }>("classify")
    .step("get-amount", ({ input }) => Pipeline.succeed(input.amount))
    .branch("classify", {
      condition: (amount) => amount > 1000,
      ifTrue: ({ prev }) => Pipeline.succeed(`Large order: $${prev}`),
      ifFalse: ({ prev }) => Pipeline.succeed(`Small order: $${prev}`),
    })
    .execute({ amount: 5000 });

  console.log(result); // "Large order: $5000"
}

// DAG visualization
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
  console.log(dag);
  // { name: "analyze", steps: [{ name: "parse", dependsOn: [], kind: "normal" }, ...] }
}

export { diamondDag, fanOut, branching, dagVisualization };
