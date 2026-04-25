// ---------------------------------------------------------------------------
// `resolveLocalAgent` — turn a `RegisteredAgent` recipe into a live
// `LocalAgent` by combining the JSON-serializable backend config with
// runtime injectables (LLM provider factory, tool implementations,
// runner, memory store).
//
// This is the bridge between "static config in a database" and
// "executable agent in process." Gateways call it once per request to
// materialize the agent, then call `.bind()` for tenancy.
//
// The function is intentionally narrow: it only handles `backend.type === "local"`.
// Future backends (acp, mastra, http) get their own resolvers — the
// gateway picks based on `backend.type`.
// ---------------------------------------------------------------------------

import type { WorkflowRunner } from "@promin/workflow";
import { LocalAgent, type LocalAgentConfig } from "../agent/local-agent.ts";
import type { LLMProvider } from "../llm-provider.ts";
import type { MemoryStore } from "../memory/types.ts";
// biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
import type { AgentTool } from "../tool.ts";
import type { Consolidator } from "../memory/consolidator.ts";
import type { LocalAgentBackend, RegisteredAgent } from "./types.ts";

/** Caller-supplied runtime injectables. */
export interface ResolveLocalAgentDeps {
  readonly runner: WorkflowRunner;
  readonly memory?: MemoryStore;
  /**
   * Resolve `(provider, modelId)` → `LLMProvider`. The gateway typically
   * builds this from a config map of API keys + provider adapters.
   */
  readonly llm: (provider: string, modelId: string) => LLMProvider;
  /**
   * Tool name → implementation. Tool names referenced by the recipe but
   * absent from this map cause a clear error (see `onUnknownTool`).
   */
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
  readonly tools: Readonly<Record<string, AgentTool<any, any>>>;
  /**
   * Optional default namespace baked into the resolved agent. Multi-tenant
   * gateways usually leave this unset and `.bind()` per request.
   */
  readonly namespaceId?: string;
  /** Optional default resource. Same caveat as `namespaceId`. */
  readonly resourceId?: string;
  /**
   * Behavior for tool names referenced by the recipe but missing from `tools`.
   * Default `"throw"`. `"skip"` quietly drops them — useful when an agent
   * recipe pins forward-looking tool names that the deployment doesn't have yet.
   */
  readonly onUnknownTool?: "throw" | "skip";
  /**
   * Plug in a custom Consolidator for `compactThread` / `distillThread`.
   * When omitted, LocalAgent auto-builds a `DefaultConsolidator` (using
   * `consolidatorLlm` below or the chat LLM as a fallback).
   */
  readonly consolidator?: Consolidator;
  /**
   * LLM used by the auto-built consolidator. Distillation is summarisation
   * — usually fine to use a cheaper model than the chat LLM.
   */
  readonly consolidatorLlm?: LLMProvider;
}

/**
 * Materialize a `LocalAgent` from a registered recipe. Throws if
 * `agent.backend.type !== "local"` — callers should branch on backend
 * type before calling this resolver.
 */
export function resolveLocalAgent(agent: RegisteredAgent, deps: ResolveLocalAgentDeps): LocalAgent {
  if (agent.backend.type !== "local") {
    throw new Error(
      `resolveLocalAgent: backend type "${agent.backend.type}" is not supported. ` +
        "Branch on agent.backend.type before resolving.",
    );
  }
  const backend: LocalAgentBackend = agent.backend;

  const tools = pickTools(backend.tools, deps.tools, deps.onUnknownTool ?? "throw");
  const llm = deps.llm(backend.model.provider, backend.model.id);

  const config: LocalAgentConfig = {
    agent: {
      name: agent.id,
      llm,
      tools,
      systemPrompt: backend.systemPrompt ?? undefined,
      maxStepsPerTurn: backend.maxStepsPerTurn,
      maxTurns: backend.maxTurns,
    },
    runner: deps.runner,
    memory: deps.memory,
    namespaceId: deps.namespaceId,
    resourceId: deps.resourceId,
    consolidator: deps.consolidator,
    consolidatorLlm: deps.consolidatorLlm,
  };

  return new LocalAgent(config);
}

function pickTools(
  names: ReadonlyArray<string>,
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
  available: Readonly<Record<string, AgentTool<any, any>>>,
  onUnknown: "throw" | "skip",
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
): Record<string, AgentTool<any, any>> {
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
  const out: Record<string, AgentTool<any, any>> = {};
  for (const name of names) {
    const tool = available[name];
    if (!tool) {
      if (onUnknown === "skip") continue;
      throw new Error(
        `resolveLocalAgent: tool "${name}" referenced by the recipe but not provided ` +
          "in deps.tools. Either add the implementation or set onUnknownTool: 'skip'.",
      );
    }
    out[name] = tool;
  }
  return out;
}
