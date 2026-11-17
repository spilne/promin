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
- `dag` invokes another registered graph as a child run.
- `decide` asks a configured decision model a fixed set of typed questions without running an
  agent child.
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

## Decisions

A `decide` node sends its resolved input state and named typed questions to a decision provider.
Questions may be `choice`, `score`, or `yesno`; the answers become the node output, so routes can
address paths such as `team.choice` or `approved.yes`. The recipe references a model by provider
and id, while the host supplies an `InMemoryDecisionModelCatalog` to `ZoryaDags`.

```ts
{
  id: "triage",
  kind: "decide",
  model: { provider: "llm", id: "small" },
  inputs: { ticket: { kind: "initial", path: "ticket" } },
  questions: {
    team: {
      type: "choice",
      instructions: "Which team owns this ticket?",
      options: [{ name: "billing" }, { name: "technical" }, { name: "other" }],
    },
  },
}
```

Three provider choices cover the common deployment shapes:

- `createSystemOneDecisionProvider` calls a local or self-hosted System One endpoint.
- TypeSafe's hosted Jev models use the same System One provider with `https://api.typesafe.ai` and
  an API key.
- `createLlmDecisionProvider` forces an existing chat model to answer through a typed decision
  tool, which is useful when no specialized decision model is available.

Provider answers report `probabilitySource`. A source of `"none"` means the adapter only knows the
selected categorical answer and synthesizes certainty; probability thresholds are therefore not
meaningful for that output. Thresholds also are not portable constants: tune and evaluate them for
each model and question set before using them as a gate.

### Router

A decision model can route directly on a typed choice:

```ts
const nodes = [
  {
    id: "route",
    kind: "decide",
    model: { provider: "system-one", id: "tev1" },
    inputs: { request: { kind: "initial", path: "request" } },
    questions: {
      team: {
        type: "choice",
        instructions: "Who should handle this request?",
        options: [{ name: "sales" }, { name: "support" }],
      },
    },
  },
  { id: "sales", kind: "agent", agentId: "sales", inputs: {} },
  { id: "support", kind: "agent", agentId: "support", inputs: {} },
];
const edges = [
  {
    from: "route",
    to: "sales",
    condition: { kind: "equals", path: "team.choice", value: "sales" },
  },
  {
    from: "route",
    to: "support",
    condition: { kind: "equals", path: "team.choice", value: "support" },
  },
];
```

The equivalent agent recipe uses typed output when the routing step needs tools, memory, or a
longer prompt:

```ts
const nodes = [
  {
    id: "route",
    kind: "agent",
    agentId: "request-router",
    inputs: { request: { kind: "initial", path: "request" } },
    output: {
      schema: {
        type: "object",
        properties: { team: { type: "string", enum: ["sales", "support"] } },
        required: ["team"],
      },
    },
  },
  { id: "sales", kind: "agent", agentId: "sales", inputs: {} },
  { id: "support", kind: "agent", agentId: "support", inputs: {} },
];
const edges = [
  { from: "route", to: "sales", condition: { kind: "equals", path: "team", value: "sales" } },
  { from: "route", to: "support", condition: { kind: "equals", path: "team", value: "support" } },
];
```

### Verifier gate

A decision node can expose a calibrated yes/no probability for a gate and an unsure path:

```ts
const nodes = [
  {
    id: "verify",
    kind: "decide",
    model: { provider: "typesafe", id: "jev-1.14.0", credentialRef: "typesafe-key" },
    inputs: { draft: { kind: "node", nodeId: "writer", path: "" } },
    questions: { approved: { type: "yesno", instructions: "Is this draft ready to publish?" } },
  },
  { id: "publish", kind: "agent", agentId: "publisher", inputs: {} },
  { id: "revise", kind: "agent", agentId: "reviser", inputs: {} },
];
const edges = [
  {
    from: "verify",
    to: "publish",
    condition: { kind: "compare", path: "approved.yes", op: "gte", value: 0.8 },
  },
  {
    from: "verify",
    to: "revise",
    condition: { kind: "compare", path: "approved.yes", op: "lt", value: 0.8 },
  },
];
```

An agent can implement the same gate with a typed verdict when evaluation needs richer reasoning:

```ts
const nodes = [
  {
    id: "verify",
    kind: "agent",
    agentId: "draft-verifier",
    inputs: { draft: { kind: "node", nodeId: "writer", path: "" } },
    output: {
      schema: {
        type: "object",
        properties: {
          verdict: { type: "string", enum: ["pass", "revise"] },
          notes: { type: "string" },
        },
        required: ["verdict", "notes"],
      },
    },
  },
  { id: "publish", kind: "agent", agentId: "publisher", inputs: {} },
  { id: "revise", kind: "agent", agentId: "reviser", inputs: {} },
];
const edges = [
  { from: "verify", to: "publish", condition: { kind: "equals", path: "verdict", value: "pass" } },
  { from: "verify", to: "revise", condition: { kind: "equals", path: "verdict", value: "revise" } },
];
```

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

## Timeouts and retries

Agent and sub-graph nodes can bound each attempt with `timeoutMs` and retry failures that are safe
to repeat. Decision nodes support `timeoutMs` but not graph-level retries because their provider
already applies its own transient retry policy.

```ts
{
  id: "research",
  kind: "agent",
  agentId: "researcher",
  inputs: { topic: { kind: "initial", path: "topic" } },
  timeoutMs: 30_000,
  retry: { maxAttempts: 3, backoffMs: 2_000 },
}
```

A timeout is measured separately for every plain, fan-out, or repeat attempt. Its start is
journaled, so recovery does not grant a fresh deadline. Timing out cancels the child and reports
`node_timeout`. The allowed range is 1 second through 24 hours.

Retries use a distinct durable child run for every attempt. They apply to child failures, timeouts,
and output-schema mismatches, but not to missing or disabled agents, exhausted budgets, child-run
limits, unrelated cancellation, or rejected approvals. Later attempts receive the node budget
minus all earlier spend. `maxAttempts` is 2 through 5, and `backoffMs` is a durable wait between 0
and 60 seconds. With `forEach` or `repeat`, each item or iteration owns its retry sequence.

## Sub-graphs

A `dag` node runs another registered graph and uses that child graph's terminal outputs as its
output. Inputs mapped onto the node become the child graph's `initialInput`, and the run keeps the
same scope and shared budget. Sub-graphs can use `forEach` and `repeat` just like agent nodes.

```ts
{
  id: "round",
  kind: "dag",
  dagId: "draft-review-round",
  version: "v1",
  inputs: { topic: { kind: "initial", path: "topic" } },
}
```

Nesting is limited to four levels. Ancestor cycles, missing or disabled graphs, and runs beyond the
global 500-child ceiling fail before the child starts. The run route also checks every reachable
agent's enabled state and scope authorization, and every reachable decision model, before starting
the root graph.

## Evaluator / optimizer loop

The demo composes two recipes. `draft-review-round` is the optimizer/evaluator sub-graph: a writer
produces a draft and a reviewer returns a typed `pass` or `revise` verdict. `draft-review` repeats
that sub-graph up to three times. On later iterations, `previous` contains the prior round's terminal
outputs, so the writer can address the review notes. The repeat stops when `review.verdict` is
`pass`.

<!-- @embed packages/zorya/examples/dags/draft-review.ts -->

```ts
// ---------------------------------------------------------------------------
// draft-review DAG recipes — a sub-graph inside an evaluator/optimizer loop.
//
// One round drafts and reviews a topic. The parent repeats that round until
// the reviewer passes it, carrying the prior round's outputs into the writer.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const draftReviewRoundRecipe: RegisterDagInput = {
  id: "draft-review-round",
  version: "v1",
  metadata: {
    description: "Write a draft, then return a typed editorial verdict.",
    tags: ["demo", "evaluation", "sub-dag"],
  },
  nodes: [
    {
      id: "writer",
      kind: "agent",
      agentId: "dag-writer",
      inputs: {
        topic: { kind: "initial", path: "topic" },
        previous: { kind: "initial", path: "previous" },
      },
    },
    {
      id: "review",
      kind: "agent",
      agentId: "dag-reviewer",
      inputs: {
        draft: { kind: "node", nodeId: "writer", path: "" },
        previous: { kind: "initial", path: "previous" },
      },
      output: {
        schema: {
          type: "object",
          properties: {
            verdict: { type: "string", enum: ["pass", "revise"] },
            notes: { type: "string" },
          },
          required: ["verdict", "notes"],
        },
      },
    },
  ],
  edges: [{ from: "writer", to: "review" }],
  entry: ["writer"],
  terminals: ["writer", "review"],
};

export const draftReviewRecipe: RegisterDagInput = {
  id: "draft-review",
  version: "v1",
  metadata: {
    description: "Repeat the draft-review sub-graph until its reviewer passes.",
    tags: ["demo", "evaluation", "repeat", "sub-dag"],
  },
  nodes: [
    {
      id: "round",
      kind: "dag",
      dagId: "draft-review-round",
      version: "v1",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      repeat: {
        until: { kind: "equals", path: "review.verdict", value: "pass" },
        maxIterations: 3,
        onExhausted: "continue",
      },
    },
  ],
  edges: [],
  entry: ["round"],
  terminals: ["round"],
};
```

<!-- @end -->

## `join`

A node waits for all incoming nodes to settle. With the default `join: "all"`, every incoming edge
must be live. `join: "any"` starts the node when at least one incoming edge is live, which is useful
for merging conditionally routed branches.

## Budget sharing

A run budget is shared between work that can still start. A plain agent, decide node, or repeat loop
has weight 1; a `forEach` node uses its resolved item count and divides its share equally between
items. Settled nodes return unused capacity to the pool. Repeat iterations receive what remains
after earlier iterations' spend.

## `nodeStats`

`DurableDagOutput.nodeStats` reports every node's `spend` and `durationMs`. Fan-out nodes also report
`items` and `failedItems`; repeated nodes report `iterations`. A node that retried also reports
`attempts`. The run's `spend` is the sum of all attempts.

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
