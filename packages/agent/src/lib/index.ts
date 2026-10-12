export {
  agentAction,
  CANCELLED_ANSWER,
  RESPOND_TOOL_NAME,
  STALLED_ANSWER,
  STEP_LIMIT_ANSWER,
  StructuredOutputParseError,
} from "./agent-action.ts";
export type {
  AgentActionConfig,
  AgentEngineConfig,
  AgentFinishReason,
  AgentInput,
  AgentResult,
  AgentTurnHooks,
  AgentTurnLifecycleEvent,
  AgentTurnState,
  StepContext,
  TurnApprovalIndex,
  TurnTranscript,
} from "./agent-action.ts";
export { StallDetector } from "./stall-detector.ts";
export type { StallDetectorOptions, StallVerdict } from "./stall-detector.ts";

// Delegation — one sub-agent tool; each call runs as a child workflow.
export { agentTurnWorkflow, subAgentTool } from "./delegation/sub-agent-tool.ts";
export type {
  FixedSubAgentToolConfig,
  RoutedSubAgentToolConfig,
} from "./delegation/sub-agent-tool.ts";
export { delegatedRunId } from "./delegation/run-delegated-call.ts";
export { childToolCallId, delegatedApprovalId } from "./delegation/child-wiring.ts";
export type { ChildTurnWiring } from "./delegation/child-wiring.ts";
export {
  DEFAULT_MAX_DEPTH,
  DelegationCycleError,
  DelegationDepthError,
  ROOT_FRAME,
  isDelegableAgent,
  parseDelegationFrame,
} from "./delegation/types.ts";
export type {
  DelegableAgent,
  DelegationFrame,
  PreparedTurn,
  SubAgentDelegation,
  SubAgentRoute,
  SubAgentRouteResult,
} from "./delegation/types.ts";

export { SessionEventBus } from "./session-logger.ts";
export type { SessionEvent, LogUsage } from "./session-logger.ts";

export type { EmbeddingProvider } from "./embedding-provider.ts";

export type {
  MemoryStore,
  Fact,
  FactInput,
  FactSource,
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
  AppendMessagesOptions,
} from "./memory/types.ts";
export {
  MAX_FACT_CHARS,
  MAX_STATIC_RULES_CHARS,
  MAX_WORKING_MEMORY_CHARS,
} from "./memory/types.ts";
export {
  REFERENCE_DATA_METADATA_KEY,
  REFERENCE_DATA_TAG,
  fenceReferenceData,
  isReferenceDataMessage,
  neutralizeReferenceDelimiters,
  referenceDataMessage,
} from "./reference-data.ts";
export type { ReferenceSection } from "./reference-data.ts";

export { DefaultConsolidator } from "./memory/consolidator.ts";
export type {
  Consolidator,
  CompactThreadOptions,
  DistillThreadOptions,
  DistillResourceOptions,
  ConsolidationSignals,
  DefaultConsolidatorConfig,
} from "./memory/consolidator.ts";
export { DEFAULT_MAX_RESOURCE_FACTS, NothingToConsolidateError } from "./memory/consolidator.ts";
export {
  RateLimitedConsolidator,
  ConsolidatorRateLimitError,
} from "./memory/rate-limited-consolidator.ts";
export type {
  ConsolidatorRateLimitConfig,
  ConsolidatorRateLimitScope,
} from "./memory/rate-limited-consolidator.ts";

export { InMemoryMemoryStore } from "./memory/in-memory-memory-store.ts";
export type { InMemoryMemoryStoreConfig } from "./memory/in-memory-memory-store.ts";

export { capStaticRules, resolveContext } from "./memory/resolve-context.ts";
export type { ResolveContextInput } from "./memory/resolve-context.ts";

// RAG — retrievers, chunking, the retriever tool and registry.
export {
  DEFAULT_RETRIEVER_TOOL_NAME,
  FlatTextChunker,
  InMemoryKnowledgeBaseStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  createRetrieverTool,
  formatRetrievedChunks,
  retrieverToolName,
} from "./rag/index.ts";
export type {
  FlatTextChunkerConfig,
  InMemoryRetrieverConfig,
  IngestibleRetriever,
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeChunk,
  KnowledgeChunker,
  KnowledgeIngestDocument,
  KnowledgeSource,
  KnowledgeSourceRecord,
  KnowledgeSourceStatus,
  ListRetrieversParams,
  RegisteredRetriever,
  RegisterRetrieverInput,
  RetrieveRequest,
  RetrieveResult,
  Retriever,
  RetrieverFilter,
  RetrieverRegistry,
  RetrieverToolConfig,
  RetrieverToolInput,
  RetrieverToolOutput,
  RetrieverToolSource,
  StoredKnowledgeBase,
} from "./rag/index.ts";

// Auto-compaction + auto-distillation config — exposed alongside Consolidator for demos that wire them.
export type {
  AutoCompactConfig,
  AutoCompactSignals,
  AutoDistillConfig,
  AutoDistillSignals,
} from "./agent/auto-consolidation.ts";
export type { AgentRetrieverBinding } from "./agent/retriever-bindings.ts";

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

export { LocalAgent, threadTurnWorkflowId } from "./agent/local-agent.ts";
export { THREAD_AGENT_METADATA_KEY } from "./agent/types.ts";
export { ThreadNotFoundError, TurnKeyConflictError } from "./agent/errors.ts";
export type { LocalAgentConfig } from "./agent/local-agent.ts";

export { frameTask } from "./agent/frame-task.ts";

// VersionedRegistry — the one store behind agents, roles, skills, DAGs and
// fragments: immutable versions, an active pointer, compare-and-set writes.
export type {
  VersionedRegistry,
  Versioned,
  VersionInfo,
  VersionTrust,
  RegisterOptions,
  RegisterVersionInput,
  ListVersionedParams,
  VersionedPage,
  RegistryKind,
  FacetQuery,
} from "./versioned-registry/types.ts";
export {
  RegistryConflictError,
  RegistryNotFoundError,
  RegistryInputError,
  isRegistryConflict,
  isRegistryNotFound,
  isRegistryInputError,
  listAllVersioned,
  VERSION_PATTERN,
} from "./versioned-registry/types.ts";
export {
  assertListLimit,
  assertVersionLabel,
  nextActiveVersion,
  pushHistory,
  autoVersionLabel,
  bodyOf,
  sameBody,
  canonicalJson,
  contentHash,
  facetQuery,
  matchesFacets,
  prepareRegister,
} from "./versioned-registry/store-helpers.ts";
export type { PreparedRegister } from "./versioned-registry/store-helpers.ts";
export {
  AGENT_KIND,
  DAG_KIND,
  FRAGMENT_KIND,
  ROLE_KIND,
  SKILL_KIND,
} from "./versioned-registry/kinds.ts";
export {
  InMemoryVersionedRegistry,
  type InMemoryVersionedRegistryConfig,
} from "./versioned-registry/in-memory-versioned-registry.ts";
export {
  InMemoryAgentRegistry,
  InMemoryDagRegistry,
  InMemoryFragmentRegistry,
  InMemoryRoleRegistry,
  InMemorySkillRegistry,
  type InMemoryRegistryConfig,
} from "./versioned-registry/in-memory-registries.ts";

// AgentRegistry — versioned recipe store.
export type {
  AgentRegistry,
  AgentRecipe,
  AgentRecipeInput,
  AgentFilter,
  RegisteredAgent,
  RegisterAgentInput,
  AgentBackend,
  LocalAgentBackend,
  AgentMetadata,
  AutoCompactRecipe,
  AutoDistillRecipe,
  ContextBudgetRecipe,
  AgentKnowledgeRecipe,
  AgentKnowledgeMode,
} from "./registry/types.ts";
export { AGENT_DRAFT_ID_PREFIX, DEFAULT_AGENT_METADATA } from "./registry/types.ts";

// RoleRegistry — versioned behavioral-bundle store (persona prompt +
// fragments + tools + skills). An agent binds a role; see ROLE_AGENT_MODEL.
export type {
  RoleRegistry,
  RoleRecipe,
  RoleRecipeInput,
  RoleFilter,
  RegisteredRole,
  RegisterRoleInput,
  RoleDefinition,
  RoleMetadata,
  RoleBinding,
} from "./role/types.ts";
export { DEFAULT_ROLE_METADATA } from "./role/types.ts";
export { resolveRoleBinding, inlineRoleDefinition } from "./role/resolve-role.ts";
export type { ResolveRoleBindingDeps } from "./role/resolve-role.ts";
export { resolveLocalAgent, resolveCredentialRef } from "./registry/resolve-local-agent.ts";
export type { ResolveLocalAgentDeps } from "./registry/resolve-local-agent.ts";
export { DEFAULT_REMOTE_REQUEST_TIMEOUT_MS, RemoteAgent } from "./registry/remote-agent.ts";
export type { RemoteAgentConfig } from "./registry/remote-agent.ts";
export { resolveRemoteAgent } from "./registry/resolve-remote-agent.ts";
export type { RemoteAgentBackend } from "./registry/types.ts";

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
  SkillRecipe,
  SkillRecipeInput,
  SkillFilter,
  RegisteredSkill,
  RegisterSkillInput,
  SkillMetadata,
  SkillRef,
} from "./skills/types.ts";
export { DEFAULT_SKILL_METADATA } from "./skills/types.ts";
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
export type {
  FragmentRegistry,
  FragmentRecipe,
  FragmentFilter,
  RegisteredFragment,
  RegisterFragmentInput,
} from "./fragments/types.ts";
export { FRAGMENT_VERSION_SEPARATOR } from "./fragments/types.ts";
export { parseLayerRef, resolveSystemPrompt } from "./fragments/resolve-prompt.ts";
export type { ResolveSystemPromptParams } from "./fragments/resolve-prompt.ts";
export { parseMarkdownSkill, slugify } from "./skills/parse-markdown-skill.ts";
export type { ParseMarkdownSkillParams } from "./skills/parse-markdown-skill.ts";

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
export type { RecordApprovalArgs, RecordChatArgs, RecordToolArgs } from "./metrics/instrument.ts";

// Network — peer discovery + delegation (findAgent, callAgent). Opt-in
// per recipe via `backend.network`. See packages/agent/src/lib/network/types.ts.
export {
  DEFAULT_NETWORK,
  NetworkPermissionError,
  matchesNetworkScope,
  networksOverlap,
  peerVisible,
} from "./network/types.ts";
export type { NetworkRecipe, NetworkScope, NetworkScopeObject, PeerView } from "./network/types.ts";
export type {
  NetworkCallAuthorizer,
  NetworkCallerScope,
  NetworkRuntimeDeps,
} from "./network/runtime.ts";
export { createFindAgentTool, createCallAgentTool } from "./network/runtime.ts";

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

// Discovery — scan folders for agent recipes, skills and prompt fragments
// and reconcile them into their registries (one shared reconcile + loop).
export { applyDiscovered, FILE_SOURCE } from "./discovery/apply-discovered.ts";
export type { ApplyDiscoveredParams, ApplyDiscoveredResult } from "./discovery/apply-discovered.ts";
export { startScanLoop } from "./discovery/scan-loop.ts";
export type { ScanLoopConfig, ScanLoopHandle, ScanLoopTick } from "./discovery/scan-loop.ts";
export {
  AgentScanner,
  applyDiscoveredAgents,
  startAgentScanLoop,
} from "./discovery/agent-scanner.ts";
export type {
  AgentScannerOptions,
  AgentScanResult,
  ApplyDiscoveredAgentsParams,
  AgentScanLoopOptions,
} from "./discovery/agent-scanner.ts";
export {
  SkillScanner,
  applyDiscoveredSkills,
  startSkillScanLoop,
} from "./discovery/skill-scanner.ts";
export type {
  SkillScannerOptions,
  SkillScanResult,
  ApplyDiscoveredSkillsParams,
  SkillScanLoopOptions,
} from "./discovery/skill-scanner.ts";
export {
  FragmentScanner,
  applyDiscoveredFragments,
  startFragmentScanLoop,
} from "./discovery/fragment-scanner.ts";
export type {
  FragmentSpec,
  FragmentScannerOptions,
  FragmentScanResult,
  ApplyDiscoveredFragmentsParams,
  FragmentScanLoopOptions,
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
  DEFAULT_TURN_LEASE_TTL_MS,
} from "./lease/agent-turn-gate.ts";

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

// SecretsStorage — scoped secret storage (global / namespace / resource)
// for the multi-tenant path (BYOK, MCP credentialRef, etc.).
// `SecretScope` is exported as both a type AND a const (with builders)
// — the single `export {}` carries both.
export type { SecretsStorage, ResolvedSecret } from "./secrets/types.ts";
export { SecretScope } from "./secrets/types.ts";
export { InMemorySecretsStorage } from "./secrets/in-memory-secrets-storage.ts";

// Audit log — durable record of elevated (cross-scope) tool calls.
export type {
  ToolAuditEntry,
  ToolAuditPhase,
  ToolAuditRecord,
  ToolAuditLogger,
} from "./tool-audit/types.ts";
export {
  InMemoryToolAuditLogger,
  type InMemoryToolAuditLoggerConfig,
} from "./tool-audit/in-memory-tool-audit-logger.ts";

// MCP integration — connect agents to Model Context Protocol servers.
export type {
  BaseMcpServerConfig,
  McpCallOptions,
  McpClient,
  McpContentBlock,
  McpHttpServerRecipe,
  McpServerConfig,
  McpServerRecipe,
  McpServerRefRecipe,
  McpToolDefinition,
  McpToolOrigin,
  McpToolResult,
  StdioMcpServerConfig,
  HttpMcpServerConfig,
} from "./mcp/types.ts";
export {
  DEFAULT_MCP_MAX_CONCURRENT_CALLS,
  DEFAULT_MCP_MAX_TOOLS,
  DEFAULT_MCP_TIMEOUT_MS,
  MCP_RECIPE_SERVER_NAME,
  RECIPE_MCP_MAX_TOOLS,
} from "./mcp/types.ts";
export {
  createMcpTools,
  displayToolName,
  MAX_MCP_DESCRIPTION_CHARS,
  MAX_MCP_RESULT_CHARS,
  MAX_MCP_SCHEMA_CHARS,
  MAX_MCP_SERVER_SCHEMA_CHARS,
  MAX_MCP_TOOL_NAME_CHARS,
  type McpSkipCode,
  type McpToolSet,
  type SkippedMcpTool,
} from "./mcp/mcp-tools.ts";
export { mcpToolName, PROVIDER_TOOL_NAME } from "./mcp/mcp-tool-name.ts";
export {
  connectMcpClient,
  createMcpTransport,
  DEFAULT_MCP_MAX_RESPONSE_BYTES,
  MAX_LISTED_TOOLS,
  MAX_TOOL_LIST_PAGES,
  McpResponseTooLargeError,
} from "./mcp/mcp-client.ts";
export {
  MAX_STDIO_MESSAGE_BYTES,
  ProcessGroupStdioTransport,
  type ProcessGroupStdioTransportConfig,
} from "./mcp/mcp-stdio-transport.ts";
export { McpInputValidators, sanitizeValidationSchema } from "./mcp/mcp-schema-validator.ts";
export {
  McpClientPool,
  McpCredentialRotatedError,
  McpPoolClosedError,
  mcpPoolKey,
  type McpClientPoolConfig,
  type McpConnect,
  type McpServerHandle,
} from "./mcp/mcp-client-pool.ts";
export {
  loadMcpToolsFromRecipe,
  mcpRecipeProblems,
  McpRecipeError,
  resolveMcpCredential,
  resolveMcpServerRecipes,
  type LoadMcpToolsFromRecipeParams,
  type McpCallerScope,
  type McpRecipePolicy,
  type ResolvedMcpCredential,
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

export { buildToolDefs, createFileToolRegistry } from "./tool-registry.ts";
export type { ToolRegistry, FileToolRegistryConfig } from "./tool-registry.ts";

export {
  DefaultAgentToolCatalog,
  listMcpToolCatalog,
  type AgentToolCatalog,
  type McpCatalogSource,
  type ToolCatalogEntry,
  type ToolCatalogError,
  type ToolCatalogErrorCode,
  type ToolCatalogListing,
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

export { createLayeredMemoryTool } from "./tools/layered-memory-tools.ts";
export type { LayeredMemoryToolConfig } from "./tools/layered-memory-tools.ts";

// Durable scheduler tool — agents create cron / interval schedules that
// re-fire the agent (or a peer) when they trigger. Pairs with the
// dispatchAgentSchedule helper for the host's scheduler-loop fireOverride.
export { findAgentApprovalThread, listPendingSignals } from "./approvals/list-pending-signals.ts";
export type {
  AgentApprovalThread,
  PendingSignal,
  ListPendingSignalsParams,
} from "./approvals/list-pending-signals.ts";
export {
  APPROVE_SIGNAL_PREFIX,
  isApprovalSignal,
  composeApprovalSignal,
  parseApprovalSignal,
} from "./approvals/approve-signal.ts";
// Pending-approval index — thread turns suspended on a tool approval,
// keyed by (namespace, thread, tool call).
export type {
  PendingApprovalStore,
  PendingApproval,
  PendingApprovalKey,
  PendingApprovalStatus,
  RecordPendingApprovalParams,
  DecidePendingApprovalParams,
  DecidePendingApprovalResult,
  ListPendingApprovalsParams,
} from "./approvals/pending-approval-store.ts";
export {
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ThreadAwaitingApprovalError,
} from "./approvals/pending-approval-store.ts";
export {
  InMemoryPendingApprovalStore,
  type InMemoryPendingApprovalStoreConfig,
} from "./approvals/in-memory-pending-approval-store.ts";

export { createDurableSchedulerTools } from "./tools/durable-scheduler-tool.ts";
export type {
  DurableSchedulerToolDeps,
  DurableSchedulerTools,
} from "./tools/durable-scheduler-tool.ts";
export { inProcessSchedulerClient, httpSchedulerClient } from "./tools/scheduler-client.ts";
export type {
  SchedulerClient,
  SchedulerClientScope,
  SchedulerCreateInput,
  SchedulerCreateResult,
  SchedulerSummary,
  InProcessSchedulerClientConfig,
  HttpSchedulerClientConfig,
} from "./tools/scheduler-client.ts";
export {
  dispatchAgentSchedule,
  isAgentSchedule,
  startAgentSchedule,
} from "./tools/dispatch-agent-schedule.ts";
export type {
  AgentScheduleMetadata,
  DispatchAgentScheduleDeps,
  DispatchAgentScheduleResult,
  StartedAgentSchedule,
  ScheduleFireContext,
} from "./tools/dispatch-agent-schedule.ts";

export { createLlmTool } from "./tools/llm-tool.ts";
export type { LlmToolConfig } from "./tools/llm-tool.ts";

export {
  createFilesystemTools,
  createReadFileTool,
  createWriteFileTool,
  createListDirTool,
  createStatTool,
  safePath,
} from "./tools/filesystem-tools.ts";
export type { FilesystemToolsConfig, FilesystemTools } from "./tools/filesystem-tools.ts";

export { createShellTool, DEFAULT_INHERITED_ENV } from "./tools/shell-tool.ts";
export type { ShellToolConfig } from "./tools/shell-tool.ts";

export { createFetchUrlTool, createWebSearchTool, fetchUrl, webSearch } from "./tools/web-tools.ts";
export type { FetchUrlToolConfig, WebToolsConfig } from "./tools/web-tools.ts";

export { isPublicAddress } from "./outbound/address-ranges.ts";
export { checkOutboundUrl } from "./outbound/outbound-policy.ts";
export type {
  OutboundPolicy,
  OutboundRejection,
  OutboundUrlCheck,
} from "./outbound/outbound-policy.ts";
export {
  OutboundRequestError,
  createOutboundFetch,
  outboundFetch,
} from "./outbound/outbound-fetch.ts";
export type {
  OutboundErrorCode,
  OutboundFetchOptions,
  OutboundFetchTransport,
  OutboundResponse,
} from "./outbound/outbound-fetch.ts";

export type {
  LLMProvider,
  LLMChatParams,
  LLMToolChoice,
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

export { routerLLM, twoSpeedLLM, isToolResultStep } from "./adapters/router-llm.ts";
export type { LLMRoute, TwoSpeedLLMConfig } from "./adapters/router-llm.ts";

export { resilientLLM, ALL_LLM_ERROR_CLASSES } from "./adapters/resilient-llm.ts";
export type { ResilientLLMPolicy } from "./adapters/resilient-llm.ts";
export { classifyLLMError } from "./adapters/llm-error-classification.ts";
export type { LLMErrorClass } from "./adapters/llm-error-classification.ts";
export {
  LLMHttpError,
  LLMTimeoutError,
  LLMStreamInterruptedError,
  DEFAULT_LLM_REQUEST_TIMEOUT_MS,
} from "./adapters/llm-http.ts";
export type { LLMHttpOptions, LLMFetch } from "./adapters/llm-http.ts";

export { rotatingLLM } from "./adapters/rotating-llm.ts";
export type { RotatingLLMOptions, RotatingLLMStrategy } from "./adapters/rotating-llm.ts";

export { InMemoryCapacityStore } from "./adapters/capacity-store.ts";
export type { CapacityStore } from "./adapters/capacity-store.ts";

export { ollama } from "./adapters/ollama.ts";
export type { OllamaOptions } from "./adapters/ollama.ts";

export { llamacpp } from "./adapters/llamacpp.ts";
export type { LlamaCppOptions } from "./adapters/llamacpp.ts";

export { SystemWallClock, FakeWallClock } from "@promin/workflow";
export type { WallClock, TimerHandle } from "@promin/workflow";
export { executeToolCall, redactPriorThinkingBlocks } from "./agent-shared.ts";
export type {
  ExecuteToolCallOptions,
  LlmRetryInfo,
  LlmRetryOptions,
  RateLimiter,
} from "./agent-shared.ts";

export {
  estimateMessageTokens,
  TURN_SUMMARY_PROMPT,
  TURN_SUMMARY_SOURCE,
} from "./turn-compaction.ts";
export type { TurnCompactionConfig } from "./turn-compaction.ts";

// Agent run trace — turn-structured tree from stored messages.
export { buildAgentTrace, TURN_TRACE_META_KEY, turnTraceOf } from "./trace.ts";
export type {
  TurnSpan,
  TurnTrace,
  TurnTraceUsage,
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
export { pickPath } from "./dag/dag-helpers.ts";
export { validateDag, topologicalOrder } from "./dag/validate.ts";
export { AgentRefusedError, DagValidationError, isAgentRefusal } from "./dag/types.ts";
export { createRegistryAgentResolver } from "./dag/registry-agent-resolver.ts";
export type {
  AgentResolver,
  AgenticDagRecipe,
  DagGraph,
  DagRecipe,
  DagFilter,
  DagRegistry,
  RegisteredDag,
  RegisterDagInput,
  DagRunScope,
  DagNode,
  DagEdge,
  NodeInputSource,
} from "./dag/types.ts";
export { createDagWorkflow, DAG_WORKFLOW_NAME, dagNodeRunId } from "./dag/durable-executor.ts";
export type {
  DurableDagInput,
  DurableDagOutput,
  CreateDagWorkflowConfig,
} from "./dag/durable-executor.ts";
