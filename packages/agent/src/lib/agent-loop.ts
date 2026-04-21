import {
  workflow,
  completeSignal,
  isActivityJournalStorage,
  isJournaledSuspendStorage,
} from "@promin/workflow";
import type { WorkflowRunner, JournaledSuspendStorage } from "@promin/workflow";
import { SystemClock } from "@promin/core";
import type { RateLimiter, Clock, TimerHandle } from "@promin/core";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool, AutoApprove } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";
import type { MemoryStore, MemoryScope } from "./memory-store.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { Message, AssistantMessage, ToolResultMessage } from "./message.ts";

// ---- hooks ----

export interface HooksTurnParams {
  task: string;
  messages: Message[];
}

export interface HooksAfterTurnParams {
  task: string;
  answer: string;
  messages: Message[];
}

export interface HooksConfig {
  /**
   * Runs before the LLM think loop for each turn.
   * Return an updated message list to inject extra context, or void to leave unchanged.
   * Runs as a journaled activity — crash-safe, skipped on replay.
   */
  beforeTurn?: (params: HooksTurnParams) => Promise<Message[] | void>;
  /**
   * Runs after the answer is emitted for each turn.
   * Runs as a journaled activity — crash-safe, skipped on replay.
   */
  afterTurn?: (params: HooksAfterTurnParams) => Promise<void>;
  /**
   * Fires when no send() arrives within idleTimeoutMs after the previous turn ended.
   * Not journaled — runs outside the workflow via a clock timer.
   * Receives the actual elapsed idle time in milliseconds.
   */
  onIdle?: (idleMs: number) => Promise<void>;
  /**
   * How long (ms) the session must be idle before onIdle fires.
   * Required when onIdle is set.
   */
  idleTimeoutMs?: number;
  /**
   * Runs when session.close() is called.
   * Not journaled — use for cleanup, flushing buffers, or final memory writes.
   */
  onClose?: () => Promise<void>;
}

// ---- config types ----

export interface ContextConfig {
  /**
   * Compact when non-system messages exceed this count.
   * Default: 80.
   */
  maxMessages?: number;
  /**
   * Number of recent messages to keep after compaction.
   * Default: 40.
   */
  keepMessages?: number;
  /**
   * Ask the LLM to summarize dropped messages and inject the summary as context.
   * Default: true.
   */
  summarize?: boolean;
}

export interface MemoryConfig {
  store: MemoryStore;
  /**
   * Scope for all read and write operations on this store.
   * Omit for the global namespace (same as pre-scoping behaviour).
   * Example: { namespaceId: orgId, sessionId: sessionId }
   */
  scope?: MemoryScope;
  /**
   * Save compaction summaries to the memory store so they are retrievable
   * in future sessions. Default: true.
   */
  saveOnCompact?: boolean;
  /**
   * How many memories to retrieve and inject at session start.
   * Set to 0 to disable injection. Default: 5.
   */
  injectLimit?: number;
  /**
   * Query used to retrieve relevant memories at session start.
   * Defaults to the system prompt, or "general context" if no system prompt.
   */
  searchQuery?: string;
}

export interface AgentLoopConfig {
  name: string;
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  toolRegistry?: ToolRegistry;
  autoApprove?: AutoApprove;
  systemPrompt?: string;
  maxTurns?: number;
  maxStepsPerTurn?: number;
  rateLimiter?: RateLimiter;
  context?: ContextConfig;
  memory?: MemoryConfig;
  hooks?: HooksConfig;
  processors?: ProcessorsConfig;
  /** Time source. Default: SystemClock. Pass FakeClock in tests to drive idle timers. */
  clock?: Clock;
}

export interface AgentSession {
  send(task: string): Promise<string>;
  close(): Promise<void>;
}

export interface AgentLoop {
  session(params: { runner: WorkflowRunner; sessionId: string }): Promise<AgentSession>;
}

// ---- compaction ----

interface CompactionResult {
  messages: Message[];
  summary: string | null;
}

async function compact(
  messages: Message[],
  config: Required<Pick<ContextConfig, "keepMessages" | "summarize">>,
  llm: LLMProvider,
): Promise<CompactionResult> {
  const systemMessages = messages.filter((m) => m.role === "system");
  const nonSystem = messages.filter((m) => m.role !== "system");

  const keep = nonSystem.slice(-config.keepMessages);
  const dropped = nonSystem.slice(0, nonSystem.length - config.keepMessages);

  if (!config.summarize || dropped.length === 0) {
    return { messages: [...systemMessages, ...keep], summary: null };
  }

  const summaryResp = await llm.chat({
    messages: [
      {
        role: "system",
        content:
          "Summarize the following conversation segment concisely. " +
          "Preserve key facts, decisions, user preferences, and any context needed for future turns.",
      },
      ...dropped,
      { role: "user", content: "Summarize the above conversation." },
    ],
  });

  const summary = summaryResp.content ?? "";
  return {
    messages: [
      ...systemMessages,
      { role: "system", content: `Earlier conversation summary:\n${summary}` },
      ...keep,
    ],
    summary,
  };
}

// ---- agentLoop ----

export function agentLoop(config: AgentLoopConfig): AgentLoop {
  const maxTurns = config.maxTurns ?? 1000;
  const maxStepsPerTurn = config.maxStepsPerTurn ?? 20;

  const contextConfig: Required<ContextConfig> = {
    maxMessages: config.context?.maxMessages ?? 80,
    keepMessages: config.context?.keepMessages ?? 40,
    summarize: config.context?.summarize ?? true,
  };

  return {
    async session({ runner, sessionId }) {
      if (!isActivityJournalStorage(runner.storage) || !isJournaledSuspendStorage(runner.storage)) {
        throw new Error(
          "agentLoop requires storage that implements JournaledSuspendStorage " +
            "(e.g. InMemoryWorkflowStorage or PgWorkflowStorage).",
        );
      }
      const journalStorage = runner.storage as unknown as JournaledSuspendStorage;

      const clock = config.clock ?? SystemClock;
      const pendingResponses = new Map<number, (answer: string) => void>();
      let sessionTurn = 0;
      let closed = false;
      let idleTimer: TimerHandle | null = null;
      let idleStart = 0;

      const resetIdleTimer = () => {
        idleTimer?.clear();
        idleTimer = null;
        const { onIdle, idleTimeoutMs } = config.hooks ?? {};
        if (!onIdle || !idleTimeoutMs) return;
        idleStart = clock.currentTimeMs();
        idleTimer = clock.setTimeout(() => {
          idleTimer = null;
          onIdle(clock.currentTimeMs() - idleStart).catch(() => {});
        }, idleTimeoutMs);
      };

      const sessionWorkflow = workflow<void>({ name: config.name }).journaled(
        "conversation",
        function* (ctx, _input) {
          let messages: Message[] = [
            ...(config.systemPrompt
              ? [{ role: "system" as const, content: config.systemPrompt }]
              : []),
          ];

          // Inject relevant memories from previous sessions
          if (config.memory && (config.memory.injectLimit ?? 5) > 0) {
            const memories = yield* ctx.activity("inject-memories", () =>
              config.memory!.store.search(
                config.memory!.searchQuery ?? config.systemPrompt ?? "general context",
                config.memory!.injectLimit ?? 5,
                config.memory!.scope,
              ),
            );
            if (memories.length > 0) {
              const block = memories.map((m) => `- ${m.content}`).join("\n");
              messages = [
                ...messages,
                {
                  role: "system",
                  content: `Relevant context from previous sessions:\n${block}`,
                },
              ];
            }
          }

          for (let turn = 0; turn < maxTurns; turn++) {
            const { task } = yield* ctx.signal<{ task: string }>(`task-${turn}`);
            messages = [...messages, { role: "user", content: task }];

            // beforeTurn hook — can inject additional context into the message list
            if (config.hooks?.beforeTurn) {
              const modified = yield* ctx.activity(`before-turn-${turn}`, () =>
                config.hooks!.beforeTurn!({ task, messages }),
              );
              if (modified) messages = modified;
            }

            let answer = "";

            for (let step = 0; step < maxStepsPerTurn; step++) {
              const toolMap = config.toolRegistry?.getTools() ?? config.tools ?? {};
              const toolDefs = buildToolDefs(toolMap);

              const response = yield* ctx.activity(`think-${turn}-${step}`, async () => {
                const processorCtx = { step, turn, workflowId: ctx.workflowId };
                const processedMessages = config.processors?.beforeLLM
                  ? await config.processors.beforeLLM(messages, processorCtx)
                  : messages;

                const call = () =>
                  config.llm.chat({
                    messages: processedMessages,
                    tools: toolDefs.length > 0 ? toolDefs : undefined,
                  });
                const raw = await (config.rateLimiter
                  ? config.rateLimiter.withLimitAsync(call)
                  : call());

                return config.processors?.afterLLM
                  ? await config.processors.afterLLM(raw, processorCtx)
                  : raw;
              });

              const assistantMsg: AssistantMessage = {
                role: "assistant",
                content: response.content,
                toolCalls: response.toolCalls,
              };
              messages = [...messages, assistantMsg];

              if (response.finishReason === "stop" || !response.toolCalls?.length) {
                answer = response.content ?? "";
                break;
              }

              const toolResultMsgs: ToolResultMessage[] = [];
              for (const call of response.toolCalls) {
                const toolDef = toolMap[call.name];
                if (!toolDef) {
                  toolResultMsgs.push({
                    role: "tool",
                    toolCallId: call.id,
                    content: `Error: unknown tool "${call.name}"`,
                  });
                  continue;
                }

                if (
                  toolDef.requireApproval &&
                  !shouldAutoApprove(config.autoApprove, call, toolDef)
                ) {
                  const decision = yield* ctx.signal<{ approved: boolean; reason?: string }>(
                    `approve:${call.id}`,
                  );
                  if (!decision.approved) {
                    toolResultMsgs.push({
                      role: "tool",
                      toolCallId: call.id,
                      content: `Rejected: ${decision.reason ?? "user rejected"}`,
                    });
                    continue;
                  }
                }

                // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
                const output = yield* ctx.activity(
                  `tool-${call.name}-${turn}-${step}-${call.id}`,
                  async () => {
                    const parsed = toolDef.parameters.parse(call.input);
                    return toolDef.execute(parsed as any);
                  },
                );

                const content = toolDef.toModelOutput
                  ? toolDef.toModelOutput(output)
                  : typeof output === "string"
                    ? output
                    : JSON.stringify(output);

                toolResultMsgs.push({ role: "tool", toolCallId: call.id, content });
              }
              messages = [...messages, ...toolResultMsgs];
            }

            yield* ctx.activity(`emit-${turn}`, async () => {
              pendingResponses.get(turn)?.(answer);
              pendingResponses.delete(turn);
              return answer;
            });

            // afterTurn hook — logging, memory writes, analytics
            if (config.hooks?.afterTurn) {
              yield* ctx.activity(`after-turn-${turn}`, () =>
                config.hooks!.afterTurn!({ task, answer, messages }),
              );
            }

            // Compact if non-system messages exceed the threshold
            const nonSystemCount = messages.filter((m) => m.role !== "system").length;
            if (nonSystemCount > contextConfig.maxMessages) {
              const result = yield* ctx.activity(`compact-${turn}`, () =>
                compact(messages, contextConfig, config.llm),
              );
              messages = result.messages;

              if (
                result.summary &&
                config.memory?.saveOnCompact !== false &&
                config.memory?.store
              ) {
                yield* ctx.activity(`save-memory-${turn}`, () =>
                  config.memory!.store.save(
                    {
                      content: result.summary!,
                      metadata: { sessionId, turn, type: "compaction-summary" },
                    },
                    config.memory!.scope,
                  ),
                );
              }
            }
          }
        },
      );

      const builtWorkflow = sessionWorkflow.build();

      await runner.start({
        workflow: builtWorkflow,
        workflowId: sessionId,
        input: undefined,
      });

      return {
        async send(task: string): Promise<string> {
          if (closed) throw new Error("Session is closed");
          const turn = sessionTurn++;
          const promise = new Promise<string>((resolve) => pendingResponses.set(turn, resolve));

          // Deliver signal into the journal entry (journaled steps don't use deliverSignal)
          await completeSignal({
            storage: journalStorage,
            workflowId: sessionId,
            stepName: "conversation",
            signalName: `task-${turn}`,
            value: { task },
          });

          // Re-run the workflow. It replays past activities from the journal,
          // consumes the delivered signal, runs until the next signal, then
          // suspends (WorkflowSuspendedError is swallowed by runSafe).
          await runner.runSafe({
            workflow: builtWorkflow,
            workflowId: sessionId,
            input: undefined,
          });

          // The emit-N activity resolved the promise during the runSafe call above.
          const answer = await promise;
          resetIdleTimer();
          return answer;
        },
        async close() {
          closed = true;
          idleTimer?.clear();
          idleTimer = null;
          pendingResponses.clear();
          await config.hooks?.onClose?.();
        },
      };
    },
  };
}
