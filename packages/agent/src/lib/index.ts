export { agentAction, MaxStepsError, StructuredOutputParseError } from "./agent-action.ts";
export type {
  AgentActionConfig,
  AgentActionMemoryConfig,
  AgentInput,
  AgentResult,
  StepContext,
} from "./agent-action.ts";

export { agentTool } from "./agent-tool.ts";
export type { AgentToolConfig } from "./agent-tool.ts";

export { agentLoop } from "./agent-loop.ts";
export type {
  AgentLoopConfig,
  AgentSession,
  AgentLoop,
  AgentStatus,
  StreamOptions,
  ContextConfig,
  MemoryConfig,
  HooksConfig,
  HooksTurnParams,
  HooksAfterTurnParams,
  CompactResult,
} from "./agent-loop.ts";

export { InMemorySessionLogger, SessionEventBus } from "./session-logger.ts";
export type { SessionLogger, SessionEvent, LogUsage } from "./session-logger.ts";

export { InMemoryMemoryIndex } from "./memory-index.ts";
export type { MemoryIndex, MemoryEntry, MemoryScope, EmbeddingProvider } from "./memory-index.ts";

export type {
  MemoryStore,
  Fact,
  EpisodicRecord,
  EpisodeInput,
  EpisodeListParams,
  StoredMessage,
  ScopedKey,
  ThreadKey,
  NamespaceRow,
  ResourceRow,
  ThreadRow,
  ThreadSummary,
  ThreadInit,
  NamespacePatch,
  ResourcePatch,
  ListThreadsParams,
  MessageRange,
  TokenBudget,
  ResolvedContext,
  RecallHit,
  SemanticRecall,
} from "./memory/types.ts";
export { isSemanticRecall, PROMPT_CACHE_BOUNDARY } from "./memory/types.ts";

export { DefaultConsolidator } from "./memory/consolidator.ts";
export type {
  Consolidator,
  CompactThreadOptions,
  DistillThreadOptions,
  DistillResourceOptions,
  ConsolidationSignals,
  DefaultConsolidatorConfig,
} from "./memory/consolidator.ts";
export {
  RateLimitedConsolidator,
  ConsolidatorRateLimitError,
} from "./memory/rate-limited-consolidator.ts";
export type {
  ConsolidatorRateLimitConfig,
  ConsolidatorRateLimitScope,
} from "./memory/rate-limited-consolidator.ts";

export {
  CompactionStrategy,
  between,
  days,
  eq,
  gt,
  gte,
  hours,
  lt,
  lte,
  minutes,
  olderThan,
  seconds,
  within,
} from "./memory/compaction-strategy.ts";
export type { NumberComparator, TimeComparator } from "./memory/compaction-strategy.ts";

export { InMemoryMemoryStore } from "./memory/in-memory-memory-store.ts";
export type { InMemoryMemoryStoreConfig } from "./memory/in-memory-memory-store.ts";

export { resolveContext } from "./memory/resolve-context.ts";
export type { ResolveContextInput } from "./memory/resolve-context.ts";

// RAG / knowledge retrieval primitives.
export {
  FlatTextChunker,
  createFlatTextChunker,
  InMemoryRetriever,
  createInMemoryRetriever,
  RerankingRetriever,
  createRerankingRetriever,
  RouterRetriever,
  createRouterRetriever,
  InMemoryRetrieverRegistry,
  createInMemoryRetrieverRegistry,
  isRetrieverRegistry,
  createRetrieverTool,
} from "./rag/index.ts";
export type {
  FlatTextChunkerConfig,
  InMemoryRetrieverConfig,
  KnowledgeChunk,
  KnowledgeChunker,
  KnowledgeIngestDocument,
  KnowledgeSource,
  RetrieveRequest,
  RetrieveResult,
  Retriever,
  RetrieverFilter,
  RerankingRetrieverConfig,
  RetrieveReranker,
  RouterRetrieverConfig,
  RetrieverRoute,
  ListRetrieversParams,
  RegisteredRetriever,
  RegisterRetrieverInput,
  RetrieverRegistry,
  RetrieverToolConfig,
  RetrieverToolInput,
  RetrieverToolOutput,
} from "./rag/index.ts";

// Auto-compaction + auto-distillation config — exposed alongside Consolidator for demos that wire them.
export type {
  AutoCompactConfig,
  AutoCompactSignals,
  AutoDistillConfig,
  AutoDistillSignals,
  AgentRetrieverBinding,
} from "./agent/local-agent.ts";

// Universal Agent interface + LocalAgent backend.
export type {
  Agent,
  AgentInput as AgentInterfaceInput,
  AgentInputSource,
  AgentThread,
  AgentRunOutput,
  AgentEvent,
  AgentInvokeOpts,
  ThreadOptions,
  ListThreadsParams as AgentListThreadsParams,
  ThreadSummary as AgentThreadSummary,
  MessageRange as AgentMessageRange,
  Step,
  ToolResult,
  UsageStats,
  FinishReason,
  AgentScope,
} from "./agent/types.ts";

export { LocalAgent } from "./agent/local-agent.ts";
export type { LocalAgentConfig } from "./agent/local-agent.ts";

export { frameTask } from "./agent/frame-task.ts";

// AgentRegistry — versioned recipe store.
export type {
  AgentRegistry,
  RegisteredAgent,
  RegisterAgentInput,
  AgentBackend,
  LocalAgentBackend,
  AgentMetadata,
  ListAgentsParams,
  AutoCompactRecipe,
  AutoDistillRecipe,
  ContextBudgetRecipe,
  AgentKnowledgeRecipe,
} from "./registry/types.ts";
export { DEFAULT_AGENT_VERSION, DEFAULT_AGENT_METADATA } from "./registry/types.ts";
export { InMemoryAgentRegistry } from "./registry/in-memory-agent-registry.ts";
export type { InMemoryAgentRegistryConfig } from "./registry/in-memory-agent-registry.ts";

// RoleRegistry — versioned behavioral-bundle store (persona prompt +
// fragments + tools + skills). An agent binds a role; see ROLE_AGENT_MODEL.
export type {
  RoleRegistry,
  RegisteredRole,
  RegisterRoleInput,
  RoleDefinition,
  RoleMetadata,
  RoleBinding,
  ListRolesParams,
} from "./role/types.ts";
export { DEFAULT_ROLE_VERSION, DEFAULT_ROLE_METADATA } from "./role/types.ts";
export { InMemoryRoleRegistry } from "./role/in-memory-role-registry.ts";
export type { InMemoryRoleRegistryConfig } from "./role/in-memory-role-registry.ts";
export { resolveRoleBinding, inlineRoleDefinition } from "./role/resolve-role.ts";
export type { ResolveRoleBindingDeps } from "./role/resolve-role.ts";
export { resolveLocalAgent, resolveCredentialRef } from "./registry/resolve-local-agent.ts";
export type { ResolveLocalAgentDeps } from "./registry/resolve-local-agent.ts";
export { RemoteAgent } from "./registry/remote-agent.ts";
export { resolveRemoteAgent } from "./registry/resolve-remote-agent.ts";
export type { RemoteAgentBackend, CursorAgentBackend } from "./registry/types.ts";

export { InMemoryModelCatalog } from "./registry/model-catalog.ts";
export type {
  ModelCatalog,
  ModelCatalogItem,
  SerializedModelCatalogItem,
  ModelCapability,
  ModelCostTier,
} from "./registry/model-catalog.ts";
export { createFileModelCatalog } from "./registry/file-model-catalog.ts";
export type { FileModelCatalogConfig } from "./registry/file-model-catalog.ts";

// SkillRegistry — versioned instruction-block store. Sibling of
// AgentRegistry: an agent recipe references a catalog of skills, the
// resolver injects their description + whenToUse into the system prompt,
// and the model pulls a full body into context on demand via loadSkill.
export type {
  SkillRegistry,
  RegisteredSkill,
  RegisterSkillInput,
  SkillMetadata,
  SkillRef,
  ListSkillsParams,
} from "./skills/types.ts";
export { DEFAULT_SKILL_VERSION, DEFAULT_SKILL_METADATA } from "./skills/types.ts";
export {
  InMemorySkillRegistry,
  mergeSkillMetadata,
  matchesSkillFilter,
  sortSkills,
} from "./skills/in-memory-skill-registry.ts";
export type { InMemorySkillRegistryConfig } from "./skills/in-memory-skill-registry.ts";
export {
  resolveSkillCatalog,
  buildSkillCatalogPrompt,
  skillAllowedByCapabilities,
} from "./skills/resolve-skill-catalog.ts";
export type {
  ResolvedSkillEntry,
  ResolveSkillCatalogParams,
} from "./skills/resolve-skill-catalog.ts";
export { createLoadSkillTool, LOAD_SKILL_TOOL_NAME } from "./skills/load-skill-tool.ts";
export type { LoadSkillToolConfig, LoadSkillOutput } from "./skills/load-skill-tool.ts";

// Prompt fragments — reusable instruction blocks composed into a role
// recipe's system prompt at resolve time. Always-on (vs. skills, which are
// load-on-demand). See packages/zorya/examples/fragments for the curated
// library; resolveLocalAgent reads fragments via the optional `fragments`
// dep (FragmentRegistry).
export type { FragmentRegistry, FragmentStore } from "./fragments/types.ts";
export { InMemoryFragmentRegistry } from "./fragments/in-memory-fragment-registry.ts";
export { InMemoryFragmentStore } from "./fragments/in-memory-fragment-store.ts";
export { resolveSystemPrompt } from "./fragments/resolve-prompt.ts";
export type { ResolveSystemPromptParams } from "./fragments/resolve-prompt.ts";
export { parseMarkdownSkill, slugify } from "./skills/parse-markdown-skill.ts";
export type { ParseMarkdownSkillParams } from "./skills/parse-markdown-skill.ts";

// Cursor backend — drives the Cursor CLI (`agent -p`) over its
// stream-json NDJSON output. Two surfaces share the same session engine:
// the registry-resolved `CursorAgent` (for whole-conversation routing)
// and `createCursorCodingTool` (for inline delegation from another agent).
export {
  CursorAgent,
  resolveCursorAgent,
  runCursorSession,
  buildCursorArgs,
  defaultCursorTransport,
  NdJsonLineParser,
  classifyFrame,
  assistantFrameText,
} from "./cursor/index.ts";
export type {
  CursorAgentConfig,
  ResolveCursorAgentDeps,
  CursorEvent,
  CursorSessionRequest,
  CursorSessionResult,
  CursorTransport,
  CursorChild,
  TransportSpawnOptions,
  CursorFrame,
  CursorAssistantFrame,
  CursorResultFrame,
  CursorSystemInitFrame,
  CursorToolCallFrame,
  CursorUnknownFrame,
  CursorUserFrame,
  CursorTextContent,
} from "./cursor/index.ts";
export { createCursorCodingTool } from "./tools/cursor-coding-tool.ts";
export type {
  CursorCodingToolDeps,
  CursorCodingToolResult,
  CursorCodingToolResultOk,
  CursorCodingToolResultError,
} from "./tools/cursor-coding-tool.ts";

// Metrics — pluggable telemetry sink (Counter / Histogram). Default
// no-op so instrumentation has zero cost when unconfigured. Hosts wire
// Prometheus / OTel adapters; tests use InMemoryAgentMetrics.
export {
  NoopAgentMetrics,
  InMemoryAgentMetrics,
  staticCostRegistry,
  computeCallCostUsd,
} from "./metrics/types.ts";
export type {
  AgentMetrics,
  Counter,
  Histogram,
  MetricLabels,
  ModelCostRates,
  ModelCostRegistry,
} from "./metrics/types.ts";
export { recordChat, recordTool, recordApproval } from "./metrics/instrument.ts";
export type { RecordChatArgs, RecordToolArgs } from "./metrics/instrument.ts";

// Network — peer discovery + delegation (findAgent, callAgent). Opt-in
// per recipe via `backend.network`. See packages/agent/src/lib/network/types.ts.
export {
  DEFAULT_NETWORK,
  DEFAULT_MAX_DEPTH,
  NetworkMaxDepthError,
  NetworkPermissionError,
  matchesNetworkScope,
  networksOverlap,
  peerVisible,
} from "./network/types.ts";
export type { NetworkRecipe, NetworkScope, NetworkScopeObject, PeerView } from "./network/types.ts";
export type { NetworkRuntimeDeps, NetworkCallerScope } from "./network/runtime.ts";
export { createFindAgentTool, createCallAgentTool } from "./network/runtime.ts";
export { currentCallContext, nextCallFrame, runInCallContext } from "./network/depth.ts";
export type { NetworkCallContext } from "./network/depth.ts";

// Instance — long-lived agent instances keyed by (registeredAgentId, namespace, ownerId).
// Class/instance: RegisteredAgent is the recipe, AgentInstance is the live
// thing with its own state. The actual chat state stays in MemoryStore under
// `resourceId = instance.id`. See packages/agent/src/lib/instance/types.ts.
export { composeAgentInstanceId } from "./instance/types.ts";
export type {
  AgentInstance,
  AgentInstanceRegistry,
  CreateAgentInstanceInput,
  ListAgentInstancesParams,
  UpdateAgentInstancePatch,
} from "./instance/types.ts";
export { InMemoryAgentInstanceRegistry } from "./instance/in-memory-agent-instance-registry.ts";
export type { InMemoryAgentInstanceRegistryConfig } from "./instance/in-memory-agent-instance-registry.ts";
export { wipeAgentInstance } from "./instance/wipe.ts";
export type { WipeAgentInstanceResult } from "./instance/wipe.ts";

// Discovery — auto-scan agent recipes from a folder + reconcile into a registry.
export {
  AgentScanner,
  applyDiscoveredAgents,
  startAgentScanLoop,
} from "./discovery/agent-scanner.ts";
export type {
  AgentScannerOptions,
  AgentScanResult,
  ApplyDiscoveredAgentsOptions,
  ApplyDiscoveredAgentsResult,
  AgentScanLoopOptions,
  AgentScanLoopTick,
  AgentScanLoopHandle,
} from "./discovery/agent-scanner.ts";

// Discovery — auto-scan skill manifests from a folder + reconcile into a SkillRegistry.
export {
  SkillScanner,
  applyDiscoveredSkills,
  startSkillScanLoop,
} from "./discovery/skill-scanner.ts";
export type {
  SkillScannerOptions,
  SkillScanResult,
  ApplyDiscoveredSkillsOptions,
  ApplyDiscoveredSkillsResult,
  SkillScanLoopOptions,
  SkillScanLoopTick,
  SkillScanLoopHandle,
} from "./discovery/skill-scanner.ts";

// Discovery — auto-scan markdown prompt fragments from a folder + reconcile
// into a FragmentRegistry. Each .md file is one fragment, key = basename.
export {
  FragmentScanner,
  applyDiscoveredFragments,
  startFragmentScanLoop,
} from "./discovery/fragment-scanner.ts";
export type {
  FragmentSpec,
  FragmentScannerOptions,
  FragmentScanResult,
  ApplyDiscoveredFragmentsOptions,
  ApplyDiscoveredFragmentsResult,
  FragmentScanLoopOptions,
  FragmentScanLoopTick,
  FragmentScanLoopHandle,
} from "./discovery/fragment-scanner.ts";

// ThreadLeaseStore — coordination primitive for horizontally-scaled
// agent runtimes. AgentTurnGate exposes strict-mode policy on top.
export type {
  LeaseStore,
  ThreadLeaseKey,
  ThreadLease,
  AcquireResult,
  ExtendResult,
} from "./lease/types.ts";
export {
  InMemoryLeaseStore,
  type InMemoryLeaseStoreConfig,
} from "./lease/in-memory-lease-store.ts";
export {
  AgentTurnGate,
  type AgentTurnGateConfig,
  type AgentTurnPolicy,
  type AgentTurnLease,
  type RunParams as AgentTurnRunParams,
  TurnInProgressError,
  QueuedPolicyNotImplementedError,
  DEFAULT_TURN_LEASE_TTL_MS,
} from "./lease/agent-turn-gate.ts";

// Federation — host-side allowlist gating remote-backend recipe
// registration. Composable as a wrapper around any AgentRegistry impl.
export type { FederationManifest } from "./federation/types.ts";
export {
  StaticFederationManifest,
  AllowAllFederationManifest,
  FederationManifestError,
} from "./federation/types.ts";
export { FederatedAgentRegistry } from "./federation/federated-agent-registry.ts";

// RemoteDeploymentRegistry — tracks live remote Zorya deployments that
// have self-registered to expose RemoteAgentBackend recipes here.
// Mirrors the WorkerRegistry pattern (heartbeat + TTL + sweep).
export type {
  RemoteDeploymentRegistry,
  RegisterDeploymentInput,
  RegisteredDeployment,
  RemoteDeploymentAuth,
} from "./remote-deployments/types.ts";
export { DEFAULT_DEPLOYMENT_TTL_MS } from "./remote-deployments/types.ts";
export {
  InMemoryRemoteDeploymentRegistry,
  type InMemoryRemoteDeploymentRegistryConfig,
} from "./remote-deployments/in-memory-remote-deployment-registry.ts";

// SecretsStorage — scoped secret storage (global / namespace / resource).
// Distinct from the flat SecretStore in secret-store.ts; this surface
// supports the multi-tenant SaaS path (BYOK, MCP credentialRef, etc.).
// `SecretScope` is exported as both a type AND a const (with builders)
// — the single `export {}` carries both.
export type { SecretsStorage, ResolvedSecret } from "./secrets/types.ts";
export { SecretScope } from "./secrets/types.ts";
export { InMemorySecretsStorage } from "./secrets/in-memory-secrets-storage.ts";

// Audit log — durable record of elevated (cross-scope) tool calls.
export type { ToolAuditEntry, ToolAuditRecord, ToolAuditLogger } from "./tool-audit/types.ts";
export {
  InMemoryToolAuditLogger,
  type InMemoryToolAuditLoggerConfig,
} from "./tool-audit/in-memory-tool-audit-logger.ts";

// MCP integration — connect agents to Model Context Protocol servers.
export type {
  McpClient,
  McpServerConfig,
  StdioMcpServerConfig,
  HttpMcpServerConfig,
  SseMcpServerConfig,
  McpToolDefinition,
  McpToolResult,
  McpContentBlock,
} from "./mcp/types.ts";
export { createMcpTools } from "./mcp/mcp-tools.ts";
export { createSdkMcpClient, type SdkMcpClientConfig } from "./mcp/mcp-client.ts";
export {
  DefaultMcpClientPool,
  InMemoryMcpClientPool,
  type McpClientPool,
  type DefaultMcpClientPoolConfig,
} from "./mcp/mcp-client-pool.ts";
export {
  loadMcpToolsFromRecipe,
  type LoadMcpToolsFromRecipeParams,
} from "./mcp/load-mcp-tools-from-recipe.ts";

export { tool, createScopedTool, createElevatedTool, filterToolsByCapability } from "./tool.ts";
export type {
  AgentTool,
  ApprovalDecision,
  AutoApprove,
  ScopedToolContext,
  ElevatedToolContext,
  ScopedToolConfig,
  ElevatedToolConfig,
  ScopedToolSecretsConfig,
  ScopedToolMemoryConfig,
  ScopedMemory,
  ToolScope,
  ToolExecuteContext,
  ToolWriter,
} from "./tool.ts";

export { multiTool, command } from "./multi-tool.ts";
export type { CommandDef } from "./multi-tool.ts";

export { buildToolDefs, createFileToolRegistry } from "./tool-registry.ts";
export type { ToolRegistry, FileToolRegistryConfig } from "./tool-registry.ts";

export {
  DefaultAgentToolCatalog,
  type AgentToolCatalog,
  type ToolCatalogEntry,
  type ToolCatalogSource,
  type DefaultAgentToolCatalogConfig,
} from "./tool-catalog.ts";

// Tool catalog history — durable audit trail of catalogued tools over time.
export type {
  ToolHistorySourceKind,
  ToolObservation,
  ToolHistoryRecord,
  ToolHistoryQuery,
  ToolHistoryStore,
} from "./tool-history/types.ts";
export {
  InMemoryToolHistoryStore,
  type InMemoryToolHistoryStoreConfig,
} from "./tool-history/in-memory-tool-history-store.ts";
export {
  AgentToolCatalogHistory,
  type AgentToolCatalogHistoryConfig,
  toObservation,
} from "./tool-history/agent-tool-catalog-history.ts";

export {
  reconcileToolReferences,
  type RecipeToolRefHealth,
  type OrphanToolEntry,
  type ToolRefReport,
  type ReconcileToolReferencesDeps,
} from "./reconcile-tool-refs.ts";

export { combineProcessors } from "./processors.ts";
export type { ProcessorsConfig, ProcessorContext } from "./processors.ts";

export { createWriteToolTool } from "./tools/write-tool.ts";
export type { WriteToolConfig } from "./tools/write-tool.ts";

export { createRequireSecretTool } from "./tools/require-secret-tool.ts";
export type { RequireSecretToolConfig } from "./tools/require-secret-tool.ts";

export { createApiKeyBootstrap } from "./api-key-bootstrap.ts";
export type { ApiKeyBootstrapConfig } from "./api-key-bootstrap.ts";

export { createGetSecretTool, createSetSecretTool } from "./tools/secret-tools.ts";

export {
  createSearchMemoryTool,
  createSaveMemoryTool,
  createMemoryTools,
  createMemoryTool,
} from "./tools/memory-tools.ts";
export type { MemoryToolConfig } from "./tools/memory-tools.ts";

export { createLayeredMemoryTool } from "./tools/layered-memory-tools.ts";
export type { LayeredMemoryToolConfig } from "./tools/layered-memory-tools.ts";

export { InMemoryKnowledgeGraph } from "./knowledge-graph.ts";
export type {
  KnowledgeGraph,
  KnowledgeGraphNode,
  KnowledgeGraphEdge,
  QueryOptions,
  SearchResult,
} from "./knowledge-graph.ts";

export { createKnowledgeGraphTool } from "./tools/knowledge-graph-tools.ts";
export type { KnowledgeGraphToolsConfig } from "./tools/knowledge-graph-tools.ts";

// Durable scheduler tool — agents create cron / interval schedules that
// re-fire the agent (or a peer) when they trigger. Pairs with the
// dispatchAgentSchedule helper for the host's scheduler-loop fireOverride.
export { listPendingApprovals } from "./approvals/list-pending-approvals.ts";
export type {
  PendingApproval,
  ListPendingApprovalsParams,
} from "./approvals/list-pending-approvals.ts";
export { listPendingSignals } from "./approvals/list-pending-signals.ts";
export type { PendingSignal, ListPendingSignalsParams } from "./approvals/list-pending-signals.ts";
export {
  APPROVE_SIGNAL_PREFIX,
  isApprovalSignal,
  composeApprovalSignal,
  parseApprovalSignal,
} from "./approvals/approve-signal.ts";

export {
  createDurableSchedulerTool,
  createDurableSchedulerTools,
} from "./tools/durable-scheduler-tool.ts";
export type {
  DurableSchedulerToolDeps,
  DurableSchedulerTools,
} from "./tools/durable-scheduler-tool.ts";
export { inProcessSchedulerClient, httpSchedulerClient } from "./tools/scheduler-client.ts";
export type {
  SchedulerClient,
  SchedulerClientScope,
  SchedulerCreateInput,
  SchedulerSummary,
  InProcessSchedulerClientConfig,
  HttpSchedulerClientConfig,
} from "./tools/scheduler-client.ts";
export { dispatchAgentSchedule, isAgentSchedule } from "./tools/dispatch-agent-schedule.ts";
export type {
  AgentScheduleMetadata,
  DispatchAgentScheduleDeps,
  DispatchAgentScheduleResult,
  ScheduleFireContext,
} from "./tools/dispatch-agent-schedule.ts";

export { createLlmTool } from "./tools/llm-tool.ts";
export type { LlmToolConfig } from "./tools/llm-tool.ts";
export type { SecretToolConfig, SetSecretToolConfig } from "./tools/secret-tools.ts";

export {
  createFilesystemTools,
  createReadFileTool,
  createWriteFileTool,
  createListDirTool,
  createStatTool,
  safePath,
} from "./tools/filesystem-tools.ts";
export type { FilesystemToolsConfig, FilesystemTools } from "./tools/filesystem-tools.ts";

export { createShellTool } from "./tools/shell-tool.ts";
export type { ShellToolConfig } from "./tools/shell-tool.ts";

export { createSchedulerTools } from "./tools/scheduler-tools.ts";
export type { SchedulerToolsConfig } from "./tools/scheduler-tools.ts";

export { createAgentTool } from "./tools/agent-tool-factory.ts";
export type { AgentToolFactoryConfig } from "./tools/agent-tool-factory.ts";

export { createChatGptAgentTool } from "./tools/chatgpt-agent-tool.ts";
export type { ChatGptAgentToolConfig } from "./tools/chatgpt-agent-tool.ts";

export { Terminal, isDumbTerminal, PROMPT, PLAIN_PROMPT } from "./terminal/terminal.ts";
export type { TreeNode, AgentUIRenderer } from "./terminal/terminal.ts";

export { TerminalIO } from "./terminal/terminal-io.ts";

export { MarkdownRenderer, osc8 } from "./terminal/terminal-markdown.ts";
export type { MarkdownRendererConfig } from "./terminal/terminal-markdown.ts";

export { ChatTerminal } from "./terminal/chat-terminal.ts";
export type { ChatTerminalConfig } from "./terminal/chat-terminal.ts";

export { ConsoleRunner, abortable } from "./terminal/console-runner.ts";
export type { RunTurnOptions, RunTurnResult, TurnTracker } from "./terminal/console-runner.ts";

export { createFetchUrlTool, createWebSearchTool, fetchUrl, webSearch } from "./tools/web-tools.ts";
export type { WebToolsConfig } from "./tools/web-tools.ts";

export {
  InMemorySecretStore,
  EnvSecretStore,
  FileSecretStore,
  CompositeSecretStore,
} from "./secret-store.ts";
export type { SecretStore } from "./secret-store.ts";

export type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
  LLMToolDefinition,
  LLMUsage,
  LLMFinishReason,
  RateLimitHint,
} from "./llm-provider.ts";

export type {
  Message,
  SystemMessage,
  UserMessage,
  AssistantMessage,
  ToolResultMessage,
  ToolCall,
  ThinkingBlock,
} from "./message.ts";

export { anthropic } from "./adapters/anthropic.ts";
export type { AnthropicOptions } from "./adapters/anthropic.ts";

export { openai } from "./adapters/openai.ts";
export type { OpenAIOptions } from "./adapters/openai.ts";

export { gemini } from "./adapters/gemini.ts";
export type { GeminiOptions } from "./adapters/gemini.ts";

export { openaiEmbedding } from "./adapters/openai-embedding.ts";
export type { OpenAIEmbeddingOptions } from "./adapters/openai-embedding.ts";

export { voyageEmbedding } from "./adapters/voyage-embedding.ts";
export type { VoyageEmbeddingOptions } from "./adapters/voyage-embedding.ts";

export { routerLLM } from "./adapters/router-llm.ts";
export type { LLMRoute } from "./adapters/router-llm.ts";

export { fallbackLLM } from "./adapters/fallback-llm.ts";
export { resilientLLM } from "./adapters/resilient-llm.ts";
export type { ResilientLLMPolicy } from "./adapters/resilient-llm.ts";
export { classifyLLMError } from "./adapters/llm-error-classification.ts";
export type { LLMErrorClass } from "./adapters/llm-error-classification.ts";

export { rotatingLLM } from "./adapters/rotating-llm.ts";
export type { RotatingLLMOptions, RotatingLLMStrategy } from "./adapters/rotating-llm.ts";

export { InMemoryCapacityStore } from "./adapters/capacity-store.ts";
export type { CapacityStore } from "./adapters/capacity-store.ts";

export { ollama } from "./adapters/ollama.ts";
export type { OllamaOptions } from "./adapters/ollama.ts";

export { llamacpp } from "./adapters/llamacpp.ts";
export type { LlamaCppOptions } from "./adapters/llamacpp.ts";

export { twoSpeedLLM } from "./adapters/two-speed-llm.ts";
export type { TwoSpeedLLMConfig } from "./adapters/two-speed-llm.ts";

export { SystemClock, FakeClock } from "@promin/core";
export type { Clock, TimerHandle } from "@promin/core";

export { broadcast } from "./broadcast.ts";

export { compact, DEFAULT_SUMMARY_PROMPT, RECAP_SUMMARY_PROMPT } from "./agent-loop-compaction.ts";
export type { CompactionConfig, CompactionResult } from "./agent-loop-compaction.ts";

export { agentNetwork } from "./agent-network.ts";
export type { AgentSpec, AgentNetworkConfig, AgentNetwork } from "./agent-network.ts";

export { createAgentTown } from "./agent-town.ts";
export type { AgentDefinition, AgentTownConfig, AgentTown } from "./agent-town.ts";

export { runCouncil, createCouncilTool, formatCouncilResult } from "./council.ts";
export type {
  Councilor,
  CouncilConfig,
  CouncilContribution,
  CouncilRound,
  CouncilResult,
  CouncilToolConfig,
} from "./council.ts";

// Agent run trace — turn-structured tree from stored messages.
export { buildAgentTrace } from "./trace.ts";
export type {
  AgentTrace,
  TraceNode,
  TraceTurnNode,
  TraceUserNode,
  TraceAssistantNode,
  TraceToolCallNode,
  TraceSystemNode,
  TraceSummary,
} from "./trace.ts";

// Agentic DAG — operator-authored multi-agent execution graph.
export { executeDag, pickPath, createRegistryResolver } from "./dag/executor.ts";
export type {
  DagExecuteParams,
  DagExecutionEvent,
  DagExecutionResult,
  AgentResolver,
} from "./dag/executor.ts";
export { validateDag, topologicalOrder } from "./dag/validate.ts";
export { DagValidationError } from "./dag/types.ts";
export type {
  AgenticDagRecipe,
  DagNode,
  DagEdge,
  DagRunState,
  NodeInputSource,
} from "./dag/types.ts";
export { createDagWorkflow } from "./dag/durable-executor.ts";
export type {
  DurableDagInput,
  DurableDagOutput,
  CreateDagWorkflowConfig,
} from "./dag/durable-executor.ts";
export { InMemoryDagRegistry } from "./dag/registry.ts";
export type {
  DagRegistry,
  RegisteredDag,
  RegisterDagInput,
  ListDagsParams,
  InMemoryDagRegistryConfig,
} from "./dag/registry.ts";
