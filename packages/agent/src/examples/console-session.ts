// ---------------------------------------------------------------------------
// A console conversation: a thin wrapper over one `LocalAgent` thread that
// runs one turn at a time and keeps the recent run events for the /log pane.
// ---------------------------------------------------------------------------

import type { LocalAgent } from "../lib/agent/local-agent.ts";
import type { AgentEvent, AgentInput, AgentThread } from "../lib/agent/types.ts";
import type { Message } from "../lib/message.ts";

/** One run event as the /log pane shows it. */
export type ConsoleEvent = AgentEvent & { readonly ts: number };

export interface ConsoleSession {
  readonly id: string;
  /** Run a turn and return its answer. */
  send(task: string): Promise<string>;
  /** Run a turn, yielding its text as it streams. Throws the turn's error. */
  stream(task: string, options?: { signal?: AbortSignal }): AsyncIterable<string>;
  /** Whether a turn is running. */
  busy(): boolean;
  /** The thread's persisted messages. */
  messages(): Promise<Message[]>;
  /** Recent run events, oldest first. */
  events(): ConsoleEvent[];
  /** Roll the older messages of the thread into an episode; its summary. */
  compact(): Promise<string>;
  /** Cancel the running turn, if any. */
  close(): Promise<void>;
}

export async function openConsoleSession(params: {
  agent: LocalAgent;
  threadId: string;
  maxEvents?: number;
}): Promise<ConsoleSession> {
  const { agent, threadId } = params;
  const maxEvents = params.maxEvents ?? 2_000;
  const thread: AgentThread<AgentInput, unknown> = await agent.thread(threadId);
  const log: ConsoleEvent[] = [];
  let running: AbortController | null = null;

  const record = (event: AgentEvent): void => {
    if (event.type === "text-delta") return;
    if (log.length >= maxEvents) log.shift();
    log.push({ ...event, ts: Date.now() });
  };

  const begin = (signal?: AbortSignal): AbortController => {
    if (running) throw new Error("Session is busy — only one turn at a time");
    const abort = new AbortController();
    if (signal) {
      if (signal.aborted) abort.abort(signal.reason);
      else signal.addEventListener("abort", () => abort.abort(signal.reason), { once: true });
    }
    running = abort;
    return abort;
  };

  return {
    id: threadId,
    async send(task) {
      const abort = begin();
      try {
        const out = await thread.send({ task }, { signal: abort.signal });
        for await (const event of out.fullStream) record(event);
        return await out.text;
      } finally {
        running = null;
      }
    },
    async *stream(task, options) {
      const abort = begin(options?.signal);
      try {
        const out = thread.stream({ task }, { signal: abort.signal });
        const logged = (async () => {
          for await (const event of out.fullStream) record(event);
        })();
        for await (const delta of out.textStream) yield delta;
        await logged;
        await out.text;
      } finally {
        running = null;
      }
    },
    busy: () => running !== null,
    messages: () => thread.messages(),
    events: () => [...log],
    async compact() {
      if (running) throw new Error("Cannot compact while a turn is in progress");
      const episode = await agent.compactThread(threadId);
      return episode.summary;
    },
    async close() {
      running?.abort();
    },
  };
}
