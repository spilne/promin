import { z } from "zod";
import { tool } from "./tool.ts";
import { agentLoop } from "./agent-loop.ts";
import { createMemoryTools } from "./tools/memory-tools.ts";
import type { WorkflowRunner } from "@promin/workflow";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool } from "./tool.ts";
import type { AgentSession } from "./agent-loop.ts";
import type { MemoryStore } from "./memory-store.ts";

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
}

export interface AgentTownConfig {
  runner: WorkflowRunner;
  /** Name of the agent that acts as the human-facing entry point. */
  mayor: string;
  agents: Record<string, AgentDefinition>;
  /** Shared memory visible to all agents via searchSharedMemory / saveSharedMemory. */
  sharedMemory?: MemoryStore;
}

export interface AgentTown {
  /** Send a task to the Mayor and wait for its full reply. */
  ask(task: string): Promise<string>;
  /** Stream the Mayor's reply token-by-token. */
  stream(task: string, signal?: AbortSignal): AsyncIterable<string>;
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
  const agentNames = Object.keys(agents);

  if (!agents[mayorName]) {
    throw new Error(`AgentTown: mayor "${mayorName}" is not in the agents map`);
  }

  // One inbox per agent + "human" as a named participant.
  const inboxes = new Map<string, AsyncQueue<InboxMessage>>();
  for (const name of agentNames) inboxes.set(name, new AsyncQueue<InboxMessage>());
  inboxes.set("human", new AsyncQueue<InboxMessage>());

  const peers = (self: string) => agentNames.filter((n) => n !== self).concat("human");

  function buildInjectedTools(
    agentName: string,
    def: AgentDefinition,
  ): Record<string, AgentTool<any, any>> {
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
          return `Delivered to ${to}.`;
        },
      }),

      readInbox: tool({
        name: "readInbox",
        description: "Block until a message arrives in your inbox and return it.",
        parameters: z.object({}),
        execute: async () => {
          const inbox = inboxes.get(agentName)!;
          const msg = await inbox.pop();
          return `[from ${msg.from}] ${msg.content}`;
        },
      }),
    };

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
    const loop = agentLoop({
      name: `town-${name}`,
      llm: def.llm,
      tools: { ...def.tools, ...injected },
      systemPrompt: [
        def.prompt,
        `You are agent "${name}" in a multi-agent town.`,
        `Peers you can message: ${peers(name).join(", ")}.`,
        `To collaborate: call sendMessage then readInbox to get the reply.`,
        `When done with a task for another agent, call sendMessage to deliver your result.`,
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

      while (!closed) {
        const msg = await inbox.pop();
        if (closed) break;
        if (msg.from === "__shutdown__") break;

        try {
          // Run the agent's LLM turn. The agent is responsible for calling
          // sendMessage to deliver its result — the daemon does not auto-reply.
          await session.send(`[from ${msg.from}] ${msg.content}`);
        } catch (err) {
          console.error(`[agentTown] ${name} error:`, err);
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

    stream(task: string, signal?: AbortSignal): AsyncIterable<string> {
      return (async function* () {
        const session = await sessionPromises.get(mayorName)!;
        yield* session.stream(task, signal);
      })();
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
