import { z } from "zod";
import { tool } from "./tool.ts";
import { agentLoop } from "./agent-loop.ts";
import { createMemoryTools } from "./tools/memory-tools.ts";
import type { WorkflowRunner } from "@promin/workflow";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool } from "./tool.ts";
import type { AgentSession, HooksConfig } from "./agent-loop.ts";
import type { ToolCall } from "./message.ts";
import type { MemoryStore } from "./memory-store.ts";
import type { SessionLogger } from "./session-logger.ts";

// ---- AsyncQueue ----

class AsyncQueue<T> {
  private readonly _items: T[] = [];
  private readonly _waiters: Array<(item: T) => void> = [];

  push(item: T): void {
    const waiter = this._waiters.shift();
    if (waiter) {
      waiter(item);
    } else {
      this._items.push(item);
    }
  }

  pop(): Promise<T> {
    if (this._items.length > 0) {
      return Promise.resolve(this._items.shift()!);
    }
    return new Promise<T>((resolve) => this._waiters.push(resolve));
  }

  get length(): number {
    return this._items.length;
  }

  /** True when a `pop()` call is currently blocked waiting for a message. */
  get hasPendingPop(): boolean {
    return this._waiters.length > 0;
  }
}

// ---- types ----

interface InboxMessage {
  from: string;
  content: string;
}

export interface AgentDefinition {
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  /** System prompt for this agent. The town injects peer awareness on top. */
  prompt: string;
  /** Private memory store. Injects searchMemory / saveMemory tools scoped to this agent. */
  memory?: MemoryStore;
  /** Per-agent hooks (beforeTurn, afterTurn, onApprovalRequired, etc.). */
  hooks?: HooksConfig;
  /**
   * When true, all user-defined tools for this agent require approval before execution,
   * routing through `AgentTownConfig.onToolApproval`.
   */
  requireApprovalForAllTools?: boolean;
  /** Structured event logger for this agent's session. */
  logger?: SessionLogger;
}

export interface AgentTownConfig {
  runner: WorkflowRunner;
  /** Name of the agent that acts as the human-facing entry point. */
  mayor: string;
  agents: Record<string, AgentDefinition>;
  /** Shared memory visible to all agents via searchSharedMemory / saveSharedMemory. */
  sharedMemory?: MemoryStore;
  /**
   * Maximum milliseconds a daemon agent's turn may run before it is force-timed-out.
   * When a turn exceeds this limit the sender receives an error reply so their
   * readInbox() unblocks rather than hanging forever.
   * Default: 120 000 ms (2 min).
   */
  daemonTurnTimeoutMs?: number;
  /**
   * Fires whenever a daemon agent starts or finishes a turn.
   * Use this to update a spinner or status line in the UI.
   *
   * @replay
   * NOT journaled — fires from the in-process daemon driver, not from
   * inside any agent's workflow body. UI-only signal; not durable across
   * worker restarts.
   */
  onAgentActivity?: (event: { agent: string; state: "thinking" | "idle" }) => void;
  /**
   * Called when a daemon agent's tool has `requireApproval: true` (or when
   * `requireApprovalForAllTools` is set on the agent definition).
   * Return `{ approved: true }` to allow execution, `{ approved: false }` to skip it.
   * When omitted, tools run without prompting (auto-approved).
   */
  onToolApproval?: (event: {
    agent: string;
    call: ToolCall;
  }) => Promise<{ approved: boolean; reason?: string }>;
}

export interface AgentTown {
  /** Send a task to the Mayor and wait for its full reply. */
  ask(task: string): Promise<string>;
  /** Stream the Mayor's reply token-by-token. */
  stream(
    task: string,
    options?: AbortSignal | import("./agent-loop.ts").StreamOptions,
  ): AsyncIterable<string>;
  /**
   * Unblock the mayor if it is currently waiting in `readInbox()`.
   * Call this when the user interrupts a turn (e.g. Ctrl+C) so the session
   * can become idle quickly rather than waiting for a daemon to reply.
   */
  interruptMayorInbox(): void;
  /** Return the mayor's AgentSession (for inspecting history, etc.). */
  getMayorSession(): Promise<AgentSession>;
  /** Return any agent's session by name (for eventLog, messages, etc.). */
  getAgentSession(name: string): Promise<AgentSession>;
  /** Shut down all agents and release resources. */
  close(): Promise<void>;
}

// ---- factory ----

/**
 * Start a town of cooperating agents that communicate via message-passing.
 *
 * Every agent automatically receives two tools:
 *   - `sendMessage(to, content)` — push a message to a peer's inbox.
 *   - `readInbox()` — block until the next message arrives in this agent's inbox.
 *
 * The Mayor is the sole human-facing agent. Call `town.ask()` or `town.stream()`
 * to interact with it. All other agents run as background daemons: they wait for
 * messages in their inbox, process them (using their tools and LLM), and reply
 * by calling `sendMessage` back to the sender.
 *
 * Shutdown: `town.close()` sets a closed flag, sends a wake-up sentinel to each
 * inbox so blocked `readInbox()` calls return, then closes all sessions.
 *
 * ## Durability
 *
 * Each agent's LLM turns are individually journaled because `agentLoop.session()`
 * runs each `session.send()` as a workflow. Using a durable storage backend (e.g.
 * `PgWorkflowStorage`) makes those turns crash-safe and replayable.
 *
 * What is **not** journaled:
 * - The daemon loops (`while (!closed) { inbox.pop(); session.send() }`) — they
 *   are plain async loops and will not restart after a process crash.
 * - The `AsyncQueue` inboxes — in-memory only; messages in flight are lost on crash.
 *
 * To make the town fully restartable, replace `AsyncQueue` with a durable message
 * queue (e.g. `PgStepQueue` or a Kafka topic) and wrap each daemon iteration in a
 * workflow activity. Session history survives automatically as long as `sessionId`
 * stays stable (`town-<name>`) and the workflow storage is persistent.
 *
 * @example
 * ```ts
 * const town = createAgentTown({
 *   runner,
 *   mayor: "coordinator",
 *   agents: {
 *     coordinator: { llm: claude, tools: {}, prompt: "Dispatch tasks to peers." },
 *     researcher:  { llm: claude, tools: searchTools, prompt: "Research topics." },
 *     coder:       { llm: claude, tools: codeTools,   prompt: "Write code." },
 *   },
 * });
 *
 * const answer = await town.ask("Research PRPH2 and write a summary page.");
 * await town.close();
 * ```
 */
export function createAgentTown(config: AgentTownConfig): AgentTown {
  const { runner, mayor: mayorName, agents } = config;
  const daemonTurnTimeoutMs = config.daemonTurnTimeoutMs ?? 120_000;
  const agentNames = Object.keys(agents);

  if (!agents[mayorName]) {
    throw new Error(`AgentTown: mayor "${mayorName}" is not in the agents map`);
  }

  // One inbox per agent + "human" as a named participant (consumed only by the mayor via readInbox).
  const inboxes = new Map<string, AsyncQueue<InboxMessage>>();
  for (const name of agentNames) inboxes.set(name, new AsyncQueue<InboxMessage>());
  inboxes.set("human", new AsyncQueue<InboxMessage>());

  // The mayor can address "human" (its own readInbox reads from its own inbox, not the human one —
  // "human" is listed so the mayor's sendMessage description is accurate for its UX).
  // Daemons exclude "human": they should reply to the sender (usually the mayor), not directly to
  // the end user. Allowing daemon→human would create a dead-letter queue since no code consumes it.
  const peers = (self: string) => {
    const others = agentNames.filter((n) => n !== self);
    return self === mayorName ? others.concat("human") : others;
  };

  // Per-daemon turn tracker: which inboxes did this agent push to this turn?
  // Used to auto-reply if the agent's LLM forgets to call sendMessage.
  const daemonSentTo = new Map<string, Set<string>>();

  function buildInjectedTools(
    agentName: string,
    def: AgentDefinition,
  ): Record<string, AgentTool<any, any>> {
    const isMayor = agentName === mayorName;
    const sentTo = isMayor ? null : daemonSentTo.get(agentName)!;

    const messaging: Record<string, AgentTool<any, any>> = {
      sendMessage: tool({
        name: "sendMessage",
        description:
          `Send a message to a peer agent or back to the human. ` +
          `Recipients: ${peers(agentName).join(", ")}.`,
        parameters: z.object({
          to: z.string().describe("Recipient name"),
          content: z.string().describe("Message body"),
        }),
        execute: async ({ to, content }) => {
          const inbox = inboxes.get(to);
          if (!inbox)
            return `Unknown recipient "${to}". Available: ${peers(agentName).join(", ")}.`;
          inbox.push({ from: agentName, content });
          sentTo?.add(to);
          return `Delivered to ${to}.`;
        },
      }),
    };

    // readInbox is only available to the mayor.
    // Daemon agents receive their next task via the daemon loop (inbox.pop()),
    // so giving them readInbox too would cause deadlocks: both sides block waiting
    // for the other to send first.
    if (isMayor) {
      messaging.readInbox = tool({
        name: "readInbox",
        description:
          "Block until a peer agent sends a message to your inbox and return it. " +
          "Use this after sendMessage to wait for a specialist's reply.",
        parameters: z.object({}),
        execute: async () => {
          const inbox = inboxes.get(agentName)!;
          const msg = await inbox.pop();
          if (msg.from === "__interrupted__") return "(turn interrupted by user)";
          return `[from ${msg.from}] ${msg.content}`;
        },
      });
    }

    const privateMemory: Record<string, AgentTool<any, any>> = def.memory
      ? createMemoryTools({ store: def.memory })
      : {};

    const sharedMemoryTools: Record<string, AgentTool<any, any>> = config.sharedMemory
      ? {
          searchSharedMemory: tool({
            name: "searchSharedMemory",
            description:
              "Search the shared town memory for entries relevant to a query. " +
              "All agents can read and write here. Use for cross-agent coordination facts.",
            parameters: z.object({
              query: z.string().describe("Natural-language search query"),
              limit: z.number().int().min(1).max(20).default(5).describe("Max entries to return"),
            }),
            execute: async ({ query, limit }) => {
              const entries = await config.sharedMemory!.search(query, limit);
              if (entries.length === 0) return "No shared memories found matching that query.";
              return entries
                .map((e, i) => `${i + 1}. [${e.id.slice(0, 8)}] ${e.content}`)
                .join("\n");
            },
          }),
          saveSharedMemory: tool({
            name: "saveSharedMemory",
            description:
              "Save a fact or insight to the shared town memory so all agents can recall it. " +
              "Use for globally important information: decisions, discovered facts, coordination state.",
            parameters: z.object({
              content: z.string().min(1).describe("The fact or insight to remember"),
            }),
            execute: async ({ content }) => {
              const id = await config.sharedMemory!.save({ content });
              return `Saved to shared memory (id: ${id.slice(0, 8)}).`;
            },
          }),
        }
      : {};

    return { ...messaging, ...privateMemory, ...sharedMemoryTools };
  }

  // Sessions keyed by agent name.
  const sessionPromises = new Map<string, Promise<AgentSession>>();
  let closed = false;

  for (const [name, def] of Object.entries(agents)) {
    const isMayor = name === mayorName;

    // Initialize per-daemon sent-tracker before building tools (tools close over it).
    if (!isMayor) daemonSentTo.set(name, new Set());

    const injected = buildInjectedTools(name, def);
    const memoryLines: string[] = [];
    if (def.memory) {
      memoryLines.push(
        "You have private memory (searchMemory / saveMemory). Use it to persist facts across sessions.",
      );
    }
    if (config.sharedMemory) {
      memoryLines.push(
        "You share a town memory with all agents (searchSharedMemory / saveSharedMemory). Use it for cross-agent coordination facts.",
      );
    }

    const roleLines = isMayor
      ? [
          `To delegate: call sendMessage with the task, then call readInbox to block until the specialist replies.`,
          `Reply directly to the user when done — do not call sendMessage for the final answer.`,
        ]
      : [
          `When your task is complete, ALWAYS call sendMessage to deliver your result to the sender.`,
          `Do not end your turn without calling sendMessage — the sender is blocked waiting for your reply.`,
        ];

    // Tool approval only applies to daemon agents, not the mayor.
    // The mayor streams directly to the human user who can exercise judgment in-band;
    // daemons run headlessly and need an out-of-band approval callback.
    const userTools: Record<string, AgentTool<any, any>> = def.tools ?? {};
    const toolsForAgent =
      !isMayor && (config.onToolApproval || def.requireApprovalForAllTools)
        ? Object.fromEntries(
            Object.entries(userTools).map(([k, t]) => [
              k,
              t.requireApproval !== undefined ? t : { ...t, requireApproval: true },
            ]),
          )
        : userTools;

    // Similarly, onApprovalRequired hook is only wired for daemons.
    const agentHooks: typeof def.hooks =
      !isMayor && config.onToolApproval
        ? {
            ...def.hooks,
            onApprovalRequired: async (call) => config.onToolApproval!({ agent: name, call }),
          }
        : def.hooks;

    const loop = agentLoop({
      name: `town-${name}`,
      llm: def.llm,
      tools: { ...toolsForAgent, ...injected },
      hooks: agentHooks,
      logger: def.logger,
      systemPrompt: [
        def.prompt,
        `You are agent "${name}" in a multi-agent town.`,
        `Peers you can message: ${peers(name).join(", ")}.`,
        ...roleLines,
        ...memoryLines,
      ].join("\n"),
    });
    sessionPromises.set(name, loop.session({ runner, sessionId: `town-${name}` }));
  }

  // Daemon loops for all non-Mayor agents: wait for inbox → process → reply.
  const daemons: Promise<void>[] = [];

  for (const [name] of Object.entries(agents)) {
    if (name === mayorName) continue;

    const daemon = (async () => {
      const session = await sessionPromises.get(name)!;
      const inbox = inboxes.get(name)!;
      const sentTo = daemonSentTo.get(name)!;

      while (!closed) {
        const msg = await inbox.pop();
        if (closed) break;
        if (msg.from === "__shutdown__") break;

        sentTo.clear();
        config.onAgentActivity?.({ agent: name, state: "thinking" });

        try {
          const timeoutSignal = AbortSignal.timeout(daemonTurnTimeoutMs);
          const timeoutPromise = new Promise<never>((_, reject) =>
            timeoutSignal.addEventListener("abort", () =>
              reject(new Error(`Agent "${name}" turn timed out after ${daemonTurnTimeoutMs}ms`)),
            ),
          );
          // Attach .catch() before the race so that if timeoutPromise wins and then
          // session.close() later rejects sendPromise, it doesn't become an unhandled rejection.
          // The result is discarded either way — the sender is unblocked via the catch block below.
          const sendPromise = session.send(`[from ${msg.from}] ${msg.content}`);
          sendPromise.catch(() => {});
          const answer = await Promise.race([sendPromise, timeoutPromise]);

          config.onAgentActivity?.({ agent: name, state: "idle" });

          // If the agent's LLM didn't call sendMessage back to the sender,
          // auto-reply with its text output so the sender's readInbox unblocks.
          if (!sentTo.has(msg.from) && inboxes.has(msg.from)) {
            inboxes.get(msg.from)!.push({ from: name, content: answer });
          }
        } catch (err) {
          config.onAgentActivity?.({ agent: name, state: "idle" });
          // On error, unblock the sender with an error notice.
          if (inboxes.has(msg.from)) {
            inboxes
              .get(msg.from)!
              .push({ from: name, content: `Error: ${(err as Error).message}` });
          }
        }
      }
    })();

    daemons.push(daemon);
  }

  return {
    async ask(task: string): Promise<string> {
      const session = await sessionPromises.get(mayorName)!;
      return session.send(task);
    },

    stream(
      task: string,
      options?: AbortSignal | import("./agent-loop.ts").StreamOptions,
    ): AsyncIterable<string> {
      return (async function* () {
        const session = await sessionPromises.get(mayorName)!;
        yield* session.stream(task, options);
      })();
    },

    interruptMayorInbox(): void {
      const inbox = inboxes.get(mayorName);
      // Only push the sentinel when the mayor is actually blocked in readInbox.
      // If nobody is waiting, we'd leave a stale message that would confuse the next turn.
      if (inbox?.hasPendingPop) {
        inbox.push({ from: "__interrupted__", content: "" });
      }
    },

    getMayorSession(): Promise<AgentSession> {
      return sessionPromises.get(mayorName)!;
    },

    getAgentSession(name: string): Promise<AgentSession> {
      const sp = sessionPromises.get(name);
      if (!sp) throw new Error(`No agent named "${name}" in this town.`);
      return sp;
    },

    async close(): Promise<void> {
      closed = true;
      // Unblock any daemon blocked in inbox.pop().
      for (const [name, inbox] of inboxes) {
        if (name !== "human") inbox.push({ from: "__shutdown__", content: "" });
      }
      await Promise.allSettled(daemons);
      for (const [, sp] of sessionPromises) {
        try {
          await (await sp).close();
        } catch {
          // ignore
        }
      }
    },
  };
}
