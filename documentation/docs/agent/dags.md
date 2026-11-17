---
title: Agent graphs
description: Fan-out, routing, loops and typed outputs in stored multi-agent recipes.
---

# Agent graphs

An agent graph is a stored, versioned recipe executed by `createDagWorkflow`. The graph itself is
acyclic, while node modifiers provide bounded fan-out and iteration. Every node start, child run and
settlement is durable, so recovery replays completed work instead of repeating it.

## Node kinds

Every node has a required `kind`:

- `agent` invokes a registered agent as a child run.
- `transform` applies deterministic operations such as `pick`, `compact`, `filter`, `sort` and
  `take` without an LLM call.

Each node maps named `inputs` from the run's `initialInput`, an upstream node's output, or a literal.
Edges define ordering and routing; `entry` and `terminals` name the graph boundaries.

## Conditions

Conditions are JSON values shared by edge routing, transform filters and `repeat.until`. They support
`equals`, `in`, `exists`, numeric `compare`, and nested `and`, `or` and `not`. A condition reads a
dot-separated `path` from the value it evaluates.

```ts
{ kind: "compare", path: "confidence", op: "gte", value: 0.8 }
```

## Typed output

An agent node's `output.schema` is a JSON Schema with an object root. The recipe is rejected if the
schema is unusable. At runtime the agent must return a matching structured object; that validated
object becomes the node output and can be addressed safely by downstream inputs.

## `forEach`

`forEach` resolves an array and runs one child attempt per item. `as` names the generated item field,
and each attempt also receives a zero-based `index`. Results keep item order even when calls finish
out of order.

```ts
forEach: {
  items: { kind: "node", nodeId: "planner", path: "questions" },
  as: "question",
  maxItems: 8,
  concurrency: 4,
  onItemError: "null",
}
```

The default error policy records a failed item as `null`; if every item fails, the node fails.
`onItemError: "fail"` instead fails and cancels the remaining work at the first substantive error.

## `repeat`

`repeat` runs attempts sequentially until its condition matches an iteration's output. Each attempt
receives `iteration` and, after the first, `previous`. The loop is always bounded.

```ts
repeat: {
  until: { kind: "equals", path: "verdict", value: "pass" },
  maxIterations: 5,
  onExhausted: "continue",
}
```

Exhausting the iteration or budget limit normally produces `repeat_exhausted`. With
`onExhausted: "continue"`, the last completed output is used instead.

## `join`

A node waits for all incoming nodes to settle. With the default `join: "all"`, every incoming edge
must be live. `join: "any"` starts the node when at least one incoming edge is live, which is useful
for merging conditionally routed branches.

## Budget sharing

A run budget is shared between work that can still start. A plain agent or repeat loop has weight 1;
a `forEach` node uses its resolved item count and divides its share equally between items. Settled
nodes return unused capacity to the pool. Repeat iterations receive what remains after earlier
iterations' spend.

## `nodeStats`

`DurableDagOutput.nodeStats` reports every node's `spend` and `durationMs`. Fan-out nodes also report
`items` and `failedItems`; repeated nodes report `iterations`. The run's `spend` is the sum of node
spend.

## Full recipe

The demo plans three typed questions, researches them with one fan-out node, removes failed-item
`null` values in a transform, and synthesizes the remaining findings.

<!-- @embed packages/zorya/examples/dags/research-synthesis.ts -->

```ts
// ---------------------------------------------------------------------------
// research-synthesis DAG recipe — typed planning, fan-out and synthesis.
//
// One planner returns a typed question list. `forEach` runs the researcher
// once per question, then a transform compacts successful findings before
// the synthesizer writes the terminal report.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const researchSynthesisRecipe: RegisterDagInput = {
  id: "research-synthesis",
  version: "v3",
  metadata: {
    description: "Typed research plan → fan-out research → compact findings → synthesis.",
    tags: ["demo", "research", "fan-out"],
  },
  nodes: [
    {
      id: "planner",
      kind: "agent",
      agentId: "dag-planner",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      output: {
        schema: {
          type: "object",
          properties: {
            questions: { type: "array", items: { type: "string" } },
          },
          required: ["questions"],
        },
      },
    },
    {
      id: "research",
      kind: "agent",
      agentId: "dag-researcher",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      forEach: {
        items: { kind: "node", nodeId: "planner", path: "questions" },
        as: "question",
        maxItems: 8,
      },
    },
    {
      id: "findings",
      kind: "transform",
      inputs: {
        research: { kind: "node", nodeId: "research", path: "" },
      },
      from: "research",
      ops: [{ op: "compact" }],
    },
    {
      id: "synthesizer",
      kind: "agent",
      agentId: "dag-synthesizer",
      inputs: {
        topic: { kind: "initial", path: "topic" },
        findings: { kind: "node", nodeId: "findings", path: "" },
      },
    },
  ],
  edges: [
    { from: "planner", to: "research" },
    { from: "research", to: "findings" },
    { from: "findings", to: "synthesizer" },
  ],
  entry: ["planner"],
  terminals: ["synthesizer"],
};
```

<!-- @end -->
