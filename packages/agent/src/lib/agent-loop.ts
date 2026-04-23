import {
  workflow,
  completeSignal,
  isActivityJournalStorage,
  isJournaledSuspendStorage,
} from "@promin/workflow";
import type {
  WorkflowRunner,
  JournaledSuspendStorage,
  ActivityJournalStorage,
} from "@promin/workflow";
import { SystemClock } from "@promin/core";
import type { RateLimiter, Clock, TimerHandle } from "@promin/core";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool, AutoApprove } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";
import type { MemoryStore, MemoryScope } from "./memory-store.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import { executeToolCall, runLlmCall } from "./agent-shared.ts";

// ---- hooks ----

export interface HooksTurnParams {
  task: string;
  messages: Message[];
}

export interface HooksAfterTurnParams {
  task: string;
  answer: string;
  messages: Message[];
  usage: { inputTokens: number; outputTokens: number };
  /** True when the turn was cut short by maxStepsPerTurn — the answer is a "(step limit reached)" sentinel. */
  truncated: boolean;
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
  /**
   * Called when a tool with `requireApproval: true` needs user approval.
   * Return `{ approved: true }` to allow execution, or `{ approved: false, reason? }` to reject.
   *
   * When set, the workflow does NOT suspend — the decision is awaited inline inside a journaled
   * activity, so it is skipped on replay. This is simpler than the `session.approve()` /
   * `session.reject()` signal path; use it when the approval UI lives in the same process (e.g. a
   * terminal REPL). Falls back to the signal-based path when omitted.
   */
  onApprovalRequired?: (call: ToolCall) => Promise<{ approved: boolean; reason?: string }>;
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
  /**
   * Separate LLM used only for context compaction (summarising dropped messages).
   * Useful for routing summaries to a cheaper or locally-hosted model.
   * Falls back to `llm` when omitted.
   */
  compactionLlm?: LLMProvider;
  /**
   * Called on every agent lifecycle transition across all sessions created by this loop.
   * Fires from inside a journaled activity — async return values are awaited in the
   * background (errors are logged, never thrown into the workflow).
   *
   * **At-most-once semantics:** because the activity is skipped on journal replay (e.g.
   * after a server restart), the callback does NOT re-fire for historical turns.
   * Use it for observability — driving an external state machine, websocket push, etc.
   * Do not rely on it for durable side-effects.
   */
  onLifecycle?: (event: AgentLifecycleEvent) => void | Promise<void>;
  /**
   * Token budget for extended thinking. When set, every LLM call will include a
   * thinking phase up to this many tokens before producing the final response.
   * Requires a model that supports extended thinking (e.g. Claude 3.7+).
   */
  thinkingBudgetTokens?: number;
}

/**
 * Current processing state of the session.
 * - `"idle"` — waiting for the next `send()` / `stream()` call.
 * - `"thinking"` — LLM call or tool execution in progress.
 * - `"waiting_approval"` — suspended mid-turn waiting for `approve()` / `reject()`.
 */
export type AgentStatus = "idle" | "thinking" | "waiting_approval";

/** Mirrors AgentStatus — the set of states the embedded lifecycle machine can be in. */
export type AgentLifecycleState = "idle" | "thinking" | "waiting_approval";

export interface AgentLifecycleEntry {
  from: AgentLifecycleState;
  event: string;
  to: AgentLifecycleState;
  createdAt: Date;
}

export interface AgentLifecycleEvent extends AgentLifecycleEntry {
  sessionId: string;
  /** The context of the destination state (e.g. `{ turn, task }` for "thinking"). */
  context: unknown;
}

export interface StreamOptions {
  signal?: AbortSignal;
  /**
   * Called with each thinking delta when extended thinking is enabled.
   * Fires before the first text delta for the same step.
   */
  onThinking?: (delta: string) => void;
}

export interface AgentSession {
  send(task: string): Promise<string>;
  /**
   * Send a task and receive the answer as a stream of token deltas.
   * Falls back to a single-chunk stream when the LLM adapter has no chatStream.
   *
   * Pass an AbortSignal (or `options.signal`) to cancel mid-flight.
   * Pass `options.onThinking` to receive extended-thinking deltas in real time.
   * The signal is propagated to the underlying fetch — the workflow completes
   * the turn with an empty response so conversation history stays consistent.
   */
  stream(task: string, options?: AbortSignal | StreamOptions): AsyncIterable<string>;
  /**
   * Approve a pending tool call that has `requireApproval: true`.
   * Returns `true` if the signal was delivered and the workflow was resumed,
   * `false` if no approval gate is pending for that `toolCallId`.
   */
  approve(toolCallId: string): Promise<boolean>;
  /**
   * Reject a pending tool call. The agent receives the rejection as a tool
   * result and continues its turn without executing the tool.
   * Returns `true` if the signal was delivered, `false` if nothing was pending.
   */
  reject(toolCallId: string, reason?: string): Promise<boolean>;
  /** Query the current processing state of this session. */
  status(): Promise<AgentStatus>;
  /** Return the full conversation message history as of the last completed turn. */
  messages(): Message[];
  /** Cumulative token usage for this session across all completed turns. */
  usage(): { inputTokens: number; outputTokens: number };
  /** Current lifecycle state. Matches `status()` but is synchronous and sourced from in-memory state. */
  lifecycleState(): { current: AgentLifecycleState; context: unknown };
  /**
   * Full lifecycle transition history for this session.
   * **Ephemeral** — stored in-memory only, resets on server restart. For durable tracking,
   * use `onLifecycle` to drive an external state machine.
   */
  lifecycleHistory(): AgentLifecycleEntry[];
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

  // Find a clean slice boundary: the first user-turn at or after the keep window.
  // Slicing mid-sequence (e.g. keeping a tool_result without its tool_use) produces
  // invalid Anthropic API input — messages[0] would contain a tool_result block with
  // no matching tool_use in the previous message.
  let keepStart = Math.max(0, nonSystem.length - config.keepMessages);
  while (keepStart < nonSystem.length && nonSystem[keepStart]!.role !== "user") {
    keepStart++;
  }

  const keep = nonSystem.slice(keepStart);
  const dropped = nonSystem.slice(0, keepStart);

  if (!config.summarize || dropped.length === 0) {
    return { messages: [...systemMessages, ...keep], summary: null };
  }

  let summary: string | null = null;
  try {
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
    summary = summaryResp.content ?? "";
  } catch (err) {
    // Summarization failed — drop messages without a summary rather than crashing the turn.
    console.error("[agentLoop] compaction summarization failed, dropping without summary:", err);
  }

  return {
    messages: [
      ...systemMessages,
      ...(summary
        ? [{ role: "system" as const, content: `Earlier conversation summary:\n${summary}` }]
        : []),
      ...keep,
    ],
    summary,
  };
}

// Effect wraps thrown errors inside journaled steps in a FiberFailure.
// WorkflowSuspendedError is a normal signal that the workflow is waiting —
// not a real failure. Check both the direct tag and the FiberFailure defect.
// Validated against Effect 3.x; the cause symbol is a stable public API.
const FIBER_FAILURE_CAUSE = Symbol.for("effect/Runtime/FiberFailure/Cause");

type FiberFailureCause =
  | { _tag: "Die"; defect: unknown }
  | { _tag: "Fail"; error: unknown }
  | { _tag: string };

function getFiberFailureCause(error: unknown): FiberFailureCause | undefined {
  if (!error || typeof error !== "object") return undefined;
  return (error as Record<symbol, FiberFailureCause | undefined>)[FIBER_FAILURE_CAUSE];
}

function isWorkflowSuspension(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  if ((error as { _tag?: string })._tag === "WorkflowSuspendedError") return true;
  const cause = getFiberFailureCause(error);
  return (
    cause?._tag === "Die" &&
    (cause as { defect?: { _tag?: string } }).defect?._tag === "WorkflowSuspendedError"
  );
}

/** Unwrap an Effect FiberFailure to get the underlying thrown error, if any. */
function unwrapFiberFailure(error: unknown): unknown {
  const cause = getFiberFailureCause(error);
  if (cause?._tag === "Die") return (cause as { defect: unknown }).defect;
  if (cause?._tag === "Fail") return (cause as { error: unknown }).error;
  return error;
}

// ---- ChunkQueue ----

// Single-consumer async queue for streaming token chunks.
// Synchronous push/close (safe to call from inside ctx.activity) with a pull-based
// AsyncIterable consumer. Replaces the hand-rolled array+notify+null-sentinel pattern.
class ChunkQueue implements AsyncIterable<string> {
  private readonly _buf: string[] = [];
  private _closed = false;
  private _notify: (() => void) | null = null;

  push(chunk: string): void {
    this._buf.push(chunk);
    this._notify?.();
    this._notify = null;
  }

  close(): void {
    this._closed = true;
    this._notify?.();
    this._notify = null;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<string> {
    while (true) {
      while (this._buf.length > 0) yield this._buf.shift()!;
      if (this._closed) return;
      await new Promise<void>((r) => {
        this._notify = r;
      });
    }
  }
}

// ---- SessionState ----

interface SessionState {
  /** Next turn number to assign on send()/stream(). */
  turn: number;
  closed: boolean;
  inTurn: boolean;
  inDelivery: boolean;
  latestMessages: Message[];
  idleStart: number;
  idleTimer: TimerHandle | null;
  /** Cumulative token usage across all completed turns. */
  totalInputTokens: number;
  totalOutputTokens: number;
  /** Embedded lifecycle state machine. */
  lifecycleState: AgentLifecycleState;
  lifecycleContext: unknown;
  lifecycleHistory: AgentLifecycleEntry[];
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
      const activityStorage = runner.storage as unknown as ActivityJournalStorage;

      const clock = config.clock ?? SystemClock;
      // Aborted by close() to cancel any in-flight LLM fetch.
      const sessionAc = new AbortController();
      const pendingResponses = new Map<
        number,
        { resolve: (answer: string) => void; reject: (err: Error) => void }
      >();
      const pendingStreams = new Map<number, ChunkQueue>();
      const pendingThinkingCallbacks = new Map<number, (delta: string) => void>();
      const pendingSignals = new Map<number, AbortSignal>();
      const state: SessionState = {
        turn: 0,
        closed: false,
        inTurn: false,
        inDelivery: false,
        latestMessages: [],
        idleStart: 0,
        idleTimer: null,
        totalInputTokens: 0,
        totalOutputTokens: 0,
        lifecycleState: "idle",
        lifecycleContext: { turns: 0 },
        lifecycleHistory: [],
      };

      function transitionLifecycle(event: string, to: AgentLifecycleState, ctx: unknown) {
        const entry: AgentLifecycleEntry = {
          from: state.lifecycleState,
          event,
          to,
          createdAt: new Date(),
        };
        state.lifecycleHistory.push(entry);
        state.lifecycleState = to;
        state.lifecycleContext = ctx;
        Promise.resolve(config.onLifecycle?.({ ...entry, sessionId, context: ctx })).catch((err) =>
          console.error("[agentLoop] onLifecycle error:", err),
        );
      }

      const resetIdleTimer = () => {
        state.idleTimer?.clear();
        state.idleTimer = null;
        const { onIdle, idleTimeoutMs } = config.hooks ?? {};
        if (!onIdle || !idleTimeoutMs) return;
        state.idleStart = clock.currentTimeMs();
        state.idleTimer = clock.setTimeout(() => {
          state.idleTimer = null;
          onIdle(clock.currentTimeMs() - state.idleStart).catch((err) =>
            console.error("[agentLoop] onIdle error:", err),
          );
        }, idleTimeoutMs);
      };

      const sessionWorkflow = workflow<void>({ name: config.name }).journaled(
        "conversation",
        function* (ctx, _input) {
          let messages: Message[] = config.systemPrompt
            ? [{ role: "system" as const, content: config.systemPrompt }]
            : [];

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
            yield* ctx.activity(`lc-${turn}-message`, async () => {
              transitionLifecycle("message", "thinking", { turns: turn, turn, task });
            });
            messages = [...messages, { role: "user", content: task }];

            // beforeTurn hook — can inject additional context into the message list
            if (config.hooks?.beforeTurn) {
              const modified = yield* ctx.activity(`before-turn-${turn}`, () =>
                config.hooks!.beforeTurn!({ task, messages }),
              );
              if (modified !== undefined) messages = modified;
            }

            let answer = "";
            let hitStepLimit = true;
            let turnInputTokens = 0;
            let turnOutputTokens = 0;

            for (let step = 0; step < maxStepsPerTurn; step++) {
              // Snapshot tool names each step so new tools hot-loaded via writeTool are visible
              // immediately on the next think step. Only names are journaled — implementations
              // come from the live registry so replay sees the same names but current code.
              const allTools = config.toolRegistry?.getTools() ?? config.tools ?? {};
              const stepToolNames = yield* ctx.activity(`tool-snapshot-${turn}-${step}`, async () =>
                Object.keys(allTools),
              );
              const toolMap = Object.fromEntries(
                stepToolNames.flatMap((name) => (allTools[name] ? [[name, allTools[name]]] : [])),
              );
              const toolDefs = buildToolDefs(toolMap);
              const chunkQueue = pendingStreams.get(turn);
              const thinkingCb = pendingThinkingCallbacks.get(turn);
              const response = yield* ctx.activity(`think-${turn}-${step}`, () =>
                runLlmCall({
                  llm: config.llm,
                  messages,
                  tools: toolDefs.length > 0 ? toolDefs : undefined,
                  rateLimiter: config.rateLimiter,
                  processors: config.processors,
                  processorCtx: { step, turn, workflowId: ctx.workflowId },
                  onChunk: chunkQueue ? (delta) => chunkQueue.push(delta) : undefined,
                  onThinking: thinkingCb,
                  thinkingBudgetTokens: config.thinkingBudgetTokens,
                  signal: pendingSignals.get(turn),
                }),
              );

              if (response.usage) {
                turnInputTokens += response.usage.inputTokens;
                turnOutputTokens += response.usage.outputTokens;
              }

              const assistantMsg: AssistantMessage = {
                role: "assistant",
                content: response.content,
                toolCalls: response.toolCalls,
                thinkingBlocks: response.thinkingBlocks,
              };
              messages = [...messages, assistantMsg];

              if (response.finishReason === "stop" || !response.toolCalls?.length) {
                hitStepLimit = false;
                answer = response.content ?? "";
                break;
              }

              const toolResultMsgs: ToolResultMessage[] = [];
              // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
              const toExecute: Array<{ call: ToolCall; toolDef: AgentTool<any, any> }> = [];

              // Phase 1: resolve approvals sequentially (each may need a user signal)
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
                  let decision: { approved: boolean; reason?: string };
                  if (config.hooks?.onApprovalRequired) {
                    decision = yield* ctx.activity(`approval-${call.id}`, () =>
                      config.hooks!.onApprovalRequired!(call),
                    );
                  } else {
                    yield* ctx.activity(`lc-${turn}-approval-${call.id}-start`, async () => {
                      transitionLifecycle("approval-required", "waiting_approval", {
                        toolCallId: call.id,
                      });
                    });
                    decision = yield* ctx.signal<{ approved: boolean; reason?: string }>(
                      `approve:${call.id}`,
                    );
                    yield* ctx.activity(`lc-${turn}-approval-${call.id}-end`, async () => {
                      transitionLifecycle(decision.approved ? "approved" : "rejected", "thinking", {
                        toolCallId: call.id,
                        approved: decision.approved,
                      });
                    });
                  }
                  if (!decision.approved) {
                    toolResultMsgs.push({
                      role: "tool",
                      toolCallId: call.id,
                      content: `Rejected: ${decision.reason ?? "user rejected"}`,
                    });
                    continue;
                  }
                }
                toExecute.push({ call, toolDef });
              }

              // Phase 2: run all approved tools in parallel, each individually journaled
              if (toExecute.length > 0) {
                const results = yield* ctx.parallel(
                  toExecute.map(({ call, toolDef }) =>
                    ctx.activity(`tool-${call.name}-${turn}-${step}-${call.id}`, () =>
                      executeToolCall(call, toolDef),
                    ),
                  ),
                );
                toolResultMsgs.push(...results);
              }

              messages = [...messages, ...toolResultMsgs];
            }

            if (hitStepLimit) {
              console.warn(
                `[agentLoop] turn ${turn}: maxStepsPerTurn (${maxStepsPerTurn}) reached without a final answer.`,
              );
              // Synthesize a closing assistant message so history doesn't end on tool
              // results. Without this, the next turn's LLM call receives malformed context
              // (consecutive user-role messages in Anthropic's format).
              answer = "(step limit reached)";
              messages = [...messages, { role: "assistant", content: answer }];
            }

            yield* ctx.activity(`emit-${turn}`, async () => {
              // Update messages snapshot here so messages() is consistent immediately
              // after send() / stream() returns, not only after after-turn-N completes.
              state.latestMessages = messages;
              pendingResponses.get(turn)?.resolve(answer);
              pendingResponses.delete(turn);
              pendingStreams.get(turn)?.close();
              pendingThinkingCallbacks.delete(turn);
              pendingSignals.delete(turn);
              return answer;
            });

            // afterTurn hook — user hook for logging, memory writes, analytics.
            yield* ctx.activity(`after-turn-${turn}`, async () => {
              state.totalInputTokens += turnInputTokens;
              state.totalOutputTokens += turnOutputTokens;
              await config.hooks?.afterTurn?.({
                task,
                answer,
                messages,
                usage: { inputTokens: turnInputTokens, outputTokens: turnOutputTokens },
                truncated: hitStepLimit,
              });
            });
            yield* ctx.activity(`lc-${turn}-done`, async () => {
              transitionLifecycle("done", "idle", { turns: turn + 1 });
            });

            // Compact if non-system messages exceed the threshold
            const nonSystemCount = messages.filter((m) => m.role !== "system").length;
            if (nonSystemCount > contextConfig.maxMessages) {
              const result = yield* ctx.activity(`compact-${turn}`, () =>
                compact(messages, contextConfig, config.compactionLlm ?? config.llm),
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

      // Restore turn counter from the journal so that recreating the session
      // object (e.g. server restart with persistent storage) doesn't re-deliver
      // task-0. Count completed emit-N activities — each represents one done turn.
      const pastEntries = await activityStorage.loadJournal(sessionId, "conversation");
      state.turn = pastEntries.reduce((max, e) => {
        const m = e.activityName.match(/^emit-(\d+)$/);
        return m && e.exit?.tag === "Success" ? Math.max(max, Number(m[1]) + 1) : max;
      }, 0);

      async function deliverAndRun(task: string, turn: number): Promise<void> {
        await completeSignal({
          storage: journalStorage,
          workflowId: sessionId,
          stepName: "conversation",
          signalName: `task-${turn}`,
          value: { task },
        });
        // Re-run: replays journal, consumes signal, suspends at next signal.
        const { error } = await runner.runSafe({
          workflow: builtWorkflow,
          workflowId: sessionId,
          input: undefined,
        });
        if (error && !isWorkflowSuspension(error)) {
          throw unwrapFiberFailure(error);
        }
      }

      return {
        async send(task: string): Promise<string> {
          if (state.closed) throw new Error("Session is closed");
          if (state.inTurn) throw new Error("Session is busy — only one turn at a time");
          const turn = state.turn++;
          state.inTurn = true;
          state.inDelivery = true;
          pendingSignals.set(turn, sessionAc.signal);
          const promise = new Promise<string>((resolve, reject) =>
            pendingResponses.set(turn, { resolve, reject }),
          );
          try {
            await deliverAndRun(task, turn);
            state.inDelivery = false;
            return await promise;
          } catch (err) {
            pendingResponses.delete(turn);
            pendingStreams.delete(turn);
            pendingThinkingCallbacks.delete(turn);
            pendingSignals.delete(turn);
            throw err;
          } finally {
            state.inTurn = false;
            state.inDelivery = false;
            resetIdleTimer();
          }
        },

        async *stream(task: string, options?: AbortSignal | StreamOptions): AsyncIterable<string> {
          if (state.closed) throw new Error("Session is closed");
          if (state.inTurn) throw new Error("Session is busy — only one turn at a time");
          const turn = state.turn++;
          state.inTurn = true;

          const opts: StreamOptions =
            options instanceof AbortSignal ? { signal: options } : (options ?? {});

          // Combine caller's abort signal with the session-level close signal.
          const combinedSignal = opts.signal
            ? AbortSignal.any([opts.signal, sessionAc.signal])
            : sessionAc.signal;
          pendingSignals.set(turn, combinedSignal);

          const chunkQueue = new ChunkQueue();
          pendingStreams.set(turn, chunkQueue);
          if (opts.onThinking) pendingThinkingCallbacks.set(turn, opts.onThinking);

          const answerPromise = new Promise<string>((resolve, reject) =>
            pendingResponses.set(turn, { resolve, reject }),
          );

          try {
            state.inDelivery = true;
            try {
              await completeSignal({
                storage: journalStorage,
                workflowId: sessionId,
                stepName: "conversation",
                signalName: `task-${turn}`,
                value: { task },
              });
            } finally {
              // inDelivery = false while runSafe is still in flight. status()
              // returns "thinking" via the runner.getStatus() fallback (inTurn=true),
              // which is correct — the agent is actively processing, just past the
              // signal-delivery phase.
              state.inDelivery = false;
            }

            // Start workflow run concurrently — chunks arrive via chunkQueue during this call.
            // On failure, close the queue to unblock the for-await below.
            let runError: unknown = undefined;
            const runPromise = runner
              .runSafe({
                workflow: builtWorkflow,
                workflowId: sessionId,
                input: undefined,
              })
              .then(({ error }) => {
                if (error && !isWorkflowSuspension(error)) {
                  runError = unwrapFiberFailure(error);
                  chunkQueue.close();
                }
              });

            try {
              for await (const chunk of chunkQueue) yield chunk;
              if (runError) throw runError;
            } finally {
              pendingStreams.delete(turn);
              pendingThinkingCallbacks.delete(turn);
              pendingResponses.delete(turn);
              pendingSignals.delete(turn);
              await runPromise;
              // Invariant: if the run succeeded, emit-N has resolved answerPromise.
              // Skip if the session was closed (close() already rejected the promise and
              // the caller should not see a "Session closed" error propagated here).
              if (!runError && !state.closed) await answerPromise;
              resetIdleTimer();
            }
          } finally {
            state.inTurn = false;
          }
        },

        async approve(toolCallId: string): Promise<boolean> {
          // Concurrency invariant: the toolCallId is generated inside the workflow and
          // only becomes visible externally after the workflow suspends at ctx.signal.
          // By that point, any concurrent runSafe from stream() has already returned,
          // so the fresh runSafe below is never truly concurrent with another run.
          //
          // Race guard: if approve() is called before the workflow has reached ctx.signal
          // (e.g. UI sends the decision before the suspension is registered), retry up to
          // 5 times with 50ms gaps to let the workflow reach its suspension point.
          for (let attempt = 0; attempt < 5; attempt++) {
            const delivered = await completeSignal({
              storage: journalStorage,
              workflowId: sessionId,
              stepName: "conversation",
              signalName: `approve:${toolCallId}`,
              value: { approved: true },
            });
            if (delivered) {
              const { error } = await runner.runSafe({
                workflow: builtWorkflow,
                workflowId: sessionId,
                input: undefined,
              });
              if (error && !isWorkflowSuspension(error)) throw unwrapFiberFailure(error);
              return true;
            }
            if (attempt < 4) await new Promise<void>((r) => clock.setTimeout(r, 50));
          }
          return false;
        },

        async reject(toolCallId: string, reason?: string): Promise<boolean> {
          for (let attempt = 0; attempt < 5; attempt++) {
            const delivered = await completeSignal({
              storage: journalStorage,
              workflowId: sessionId,
              stepName: "conversation",
              signalName: `approve:${toolCallId}`,
              value: { approved: false, reason },
            });
            if (delivered) {
              const { error } = await runner.runSafe({
                workflow: builtWorkflow,
                workflowId: sessionId,
                input: undefined,
              });
              if (error && !isWorkflowSuspension(error)) throw unwrapFiberFailure(error);
              return true;
            }
            if (attempt < 4) await new Promise<void>((r) => clock.setTimeout(r, 50));
          }
          return false;
        },

        async status(): Promise<AgentStatus> {
          if (state.closed || !state.inTurn) return "idle";
          if (state.inDelivery) return "thinking";
          const info = await runner.getStatus(sessionId, {});
          if (!info || info.state === "completed" || info.state === "failed") return "idle";
          if (info.state === "suspended") return "waiting_approval";
          return "thinking";
        },

        messages(): Message[] {
          return state.latestMessages;
        },

        usage(): { inputTokens: number; outputTokens: number } {
          return { inputTokens: state.totalInputTokens, outputTokens: state.totalOutputTokens };
        },

        lifecycleState() {
          return { current: state.lifecycleState, context: state.lifecycleContext };
        },

        lifecycleHistory() {
          return [...state.lifecycleHistory];
        },

        async close() {
          state.closed = true;
          state.idleTimer?.clear();
          state.idleTimer = null;
          // Cancel any in-flight LLM fetch so runSafe returns promptly.
          sessionAc.abort();
          // Unblock callers awaiting send() or the answerPromise inside stream().
          const closeErr = new Error("Session closed");
          pendingResponses.forEach(({ reject }) => reject(closeErr));
          pendingResponses.clear();
          // Unblock any for-await loops on chunk streams.
          pendingStreams.forEach((q) => q.close());
          pendingStreams.clear();
          pendingThinkingCallbacks.clear();
          pendingSignals.clear();
          await config.hooks?.onClose?.();
        },
      };
    },
  };
}
