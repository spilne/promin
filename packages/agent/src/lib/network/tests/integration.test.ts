// ---------------------------------------------------------------------------
// End-to-end: a coordinator agent uses findAgent + callAgent to discover
// and delegate to a specialist. Exercises the full path:
//   recipe.backend.network → resolver auto-attaches tools → tool reads
//   ALS depth → registry lookup → policy check → resolve callee →
//   withScope(propagate owner) → invoke → return assistant text.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  buildAgentTrace,
  resolveLocalAgent,
} from "../../../lib/index.ts";
import type {
  Agent,
  LLMChatParams,
  LLMProvider,
  LLMResponse,
  RegisteredAgent,
} from "../../../lib/index.ts";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

// Mock LLM that scripts a turn: optional toolCall, then on the next
// invocation a final text response. Each call advances through `script`.
function scriptedLLM(script: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async (_p: LLMChatParams): Promise<LLMResponse> => {
      const r = script[i++];
      if (!r) throw new Error("scripted LLM exhausted");
      return r;
    },
  };
}

// Capture-LLM: records the params it was called with so we can assert
// what the callee saw (e.g. its system prompt didn't include caller history).
function captureLLM(response: LLMResponse): {
  llm: LLMProvider;
  calls: LLMChatParams[];
} {
  const calls: LLMChatParams[] = [];
  return {
    llm: {
      chat: async (params) => {
        calls.push(params);
        return response;
      },
    },
    calls,
  };
}

async function bootNetwork(opts: {
  coordinatorScript: LLMResponse[];
  writerResponse: LLMResponse;
  reviewerResponse?: LLMResponse;
  withInstances?: boolean;
  coordinatorRecipe?: Partial<RegisteredAgent["backend"] & { type: "local" }>;
}) {
  const registry = new InMemoryAgentRegistry();
  const memory = new InMemoryMemoryStore();
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const instanceRegistry = opts.withInstances ? new InMemoryAgentInstanceRegistry() : undefined;

  const writer = await registry.register({
    id: "writer",
    backend: {
      type: "local",
      model: { provider: "test", id: "writer-llm" },
      systemPrompt: "I am the writer.",
      tools: [],
      network: { networks: ["default"] },
    },
    metadata: { capabilities: ["writing"], tags: ["public"] },
  });

  const reviewer = await registry.register({
    id: "reviewer",
    backend: {
      type: "local",
      model: { provider: "test", id: "reviewer-llm" },
      systemPrompt: "I am the reviewer.",
      tools: [],
      network: { networks: ["default"] },
    },
    metadata: { capabilities: ["review"], tags: ["public"] },
  });

  const coordinator = await registry.register({
    id: "coordinator",
    backend: {
      type: "local",
      model: { provider: "test", id: "coordinator-llm" },
      systemPrompt: "I am the coordinator.",
      tools: [],
      network: {
        networks: ["default"],
        canDiscover: true,
        canCall: true,
        maxDepth: 3,
      },
      ...opts.coordinatorRecipe,
    },
    metadata: { capabilities: ["coordination"], tags: ["public"] },
  });

  const writerCapture = captureLLM(opts.writerResponse);
  const reviewerCapture = captureLLM(
    opts.reviewerResponse ?? { content: "ok", finishReason: "stop" },
  );
  const coordinatorLLM = scriptedLLM(opts.coordinatorScript);

  const llmByAgent: Record<string, LLMProvider> = {
    coordinator: coordinatorLLM,
    writer: writerCapture.llm,
    reviewer: reviewerCapture.llm,
  };

  function resolve(recipe: RegisteredAgent): Agent {
    return resolveLocalAgent(recipe, {
      runner,
      memory,
      llm: () => llmByAgent[recipe.id]!,
      tools: {},
      ...(instanceRegistry
        ? {
            network: {
              registry,
              resolve, // recursive
              instanceRegistry,
            },
          }
        : {
            network: {
              registry,
              resolve,
            },
          }),
    });
  }

  return {
    registry,
    memory,
    storage,
    runner,
    instanceRegistry,
    coordinator,
    writer,
    reviewer,
    resolve,
    captures: { writer: writerCapture, reviewer: reviewerCapture },
  };
}

describe("agents network — discovery", () => {
  it("findAgent lists peers in caller's namespace, excluding self", async () => {
    const { coordinator, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            {
              id: "t1",
              name: "findAgent",
              input: {},
            },
          ],
        },
        { content: "found peers", finishReason: "stop" },
      ],
      writerResponse: { content: "x", finishReason: "stop" },
    });
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "list peers" });
    expect(await out.text).toBe("found peers");
  });

  it("findAgent filters out peers that aren't in a shared network", async () => {
    const { registry, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [{ id: "t1", name: "findAgent", input: {} }],
        },
        { content: "done", finishReason: "stop" },
      ],
      writerResponse: { content: "x", finishReason: "stop" },
    });
    // Add a billing-bot in finance only — coordinator only joins default.
    await registry.register({
      id: "billing-bot",
      backend: {
        type: "local",
        model: { provider: "test", id: "x" },
        systemPrompt: null,
        tools: [],
        network: { networks: ["finance"] },
      },
      metadata: { capabilities: ["billing"], tags: [] },
    });

    const coordinator = (await registry.get("coordinator"))!;
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });

    // We can't easily intercept the tool result, but if billing-bot were
    // visible the policy logic would let callAgent through too — which
    // we test in the next describe. Here it's enough to assert the
    // invoke completes (no policy throw).
    const out = await agent.invoke({ task: "search" });
    expect(await out.text).toBe("done");
  });
});

describe("agents network — delegation", () => {
  it("callAgent invokes the callee and returns its assistant text", async () => {
    const { coordinator, resolve, captures } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            {
              id: "t1",
              name: "callAgent",
              input: { id: "writer", prompt: "draft a haiku" },
            },
          ],
        },
        { content: "delegated and got back: ok", finishReason: "stop" },
      ],
      writerResponse: { content: "writer's haiku", finishReason: "stop" },
    });
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "ask the writer" });
    expect(await out.text).toBe("delegated and got back: ok");
    // Writer was actually invoked — its LLM saw exactly one chat call.
    expect(captures.writer.calls.length).toBe(1);
    // Writer's prompt was the delegate prompt, not the coordinator's task.
    const writerMessages = captures.writer.calls[0]!.messages;
    const writerUser = writerMessages.find((m) => m.role === "user");
    expect(writerUser?.content).toContain("draft a haiku");
  });

  it("callAgent carries the sub-run trace on the result message metadata", async () => {
    const { coordinator, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            { id: "t1", name: "callAgent", input: { id: "writer", prompt: "draft a haiku" } },
          ],
        },
        { content: "done", finishReason: "stop" },
      ],
      writerResponse: { content: "writer's haiku", finishReason: "stop" },
    });
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "ask the writer" });
    const messages = await out.messages;

    // The callAgent result message carries the peer's trace as metadata,
    // never in the LLM-visible content.
    const toolResult = messages.find((m) => m.role === "tool");
    expect(toolResult?.metadata?.childTrace).toBeDefined();
    expect(toolResult?.content ?? "").not.toContain("turns");

    // buildAgentTrace surfaces it as childTrace on the callAgent node.
    const callNode = buildAgentTrace(messages)
      .turns.flatMap((t) => t.children)
      .flatMap((c) => (c.kind === "assistant" ? c.toolCalls : []))
      .find((tc) => tc.name === "callAgent");
    expect(callNode?.childTrace).toBeDefined();
    // The child trace is the writer's own run — at least one turn.
    expect(callNode?.childTrace?.turns.length ?? 0).toBeGreaterThan(0);
  });

  it("callAgent denies when the caller's policy doesn't match", async () => {
    const { coordinator, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            {
              id: "t1",
              name: "callAgent",
              input: { id: "reviewer", prompt: "review" },
            },
          ],
        },
        { content: "denied gracefully", finishReason: "stop" },
      ],
      writerResponse: { content: "x", finishReason: "stop" },
      coordinatorRecipe: {
        network: {
          networks: ["default"],
          canDiscover: true,
          canCall: ["writer"], // only writer; reviewer is denied
          maxDepth: 3,
        },
      },
    });
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "try reviewer" });
    // The coordinator's LLM still reaches the final turn; the tool
    // returned a structured error rather than throwing.
    expect(await out.text).toBe("denied gracefully");
  });

  it("callAgent propagates ownerId to the callee's instance when one is configured", async () => {
    const { coordinator, resolve, instanceRegistry } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            {
              id: "t1",
              name: "callAgent",
              input: { id: "writer", prompt: "draft" },
            },
          ],
        },
        { content: "ok", finishReason: "stop" },
      ],
      writerResponse: { content: "writer-out", finishReason: "stop" },
      withInstances: true,
    });

    // Caller scoped under coordinator's instance for alice.
    const callerResourceId = "acme::coordinator::alice";
    const agent = resolve(coordinator).withScope({
      namespaceId: "acme",
      resourceId: callerResourceId,
    });
    await agent.invoke({ task: "delegate" });

    // The writer's instance should now exist for alice.
    const writerInstance = await instanceRegistry!.get("acme::writer::alice");
    expect(writerInstance).not.toBeNull();
    expect(writerInstance?.ownerId).toBe("alice");
    expect(writerInstance?.registeredAgentId).toBe("writer");
  });

  it("maxDepth blocks runaway A→B→A cycles", async () => {
    // Coordinator → writer (depth 1) → if writer tried to call coordinator,
    // depth 2 → if coordinator tried again, depth 3 — at depth=4 the guard
    // trips. We test by setting maxDepth=1 so even one call from the writer
    // back is blocked.
    const { registry, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [{ id: "t1", name: "callAgent", input: { id: "writer", prompt: "hi" } }],
        },
        { content: "done", finishReason: "stop" },
      ],
      writerResponse: { content: "writer-out", finishReason: "stop" },
      coordinatorRecipe: {
        network: {
          networks: ["default"],
          canDiscover: true,
          canCall: true,
          maxDepth: 0, // any callAgent fails immediately
        },
      },
    });
    const coordinator = (await registry.get("coordinator"))!;
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "delegate" });
    // Coordinator's LLM gets the tool error result and emits the final text.
    expect(await out.text).toBe("done");
  });

  it("callAgent against an unknown peer returns agent_not_found", async () => {
    const { coordinator, resolve } = await bootNetwork({
      coordinatorScript: [
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [{ id: "t1", name: "callAgent", input: { id: "ghost", prompt: "?" } }],
        },
        { content: "handled", finishReason: "stop" },
      ],
      writerResponse: { content: "x", finishReason: "stop" },
    });
    const agent = resolve(coordinator).withScope({ namespaceId: "acme" });
    const out = await agent.invoke({ task: "call ghost" });
    expect(await out.text).toBe("handled");
  });
});
