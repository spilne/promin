/**
 * Town Chat — a multi-agent research town built with createAgentTown.
 *
 * Three agents collaborate to answer questions:
 *   director (mayor) — receives user questions, orchestrates peers, presents answers
 *   researcher       — searches the web and fetches URLs; replies to director
 *   writer           — polishes research notes into clear prose; replies to director
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/town-chat.ts
 *
 * Type "exit" or press Ctrl+C at the prompt to quit.
 * Ctrl+C during a response interrupts the current turn.
 */

import { createInterface } from "node:readline";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { z } from "zod";
import { anthropic, createAgentTown, InMemoryMemoryStore, tool } from "../lib/index.ts";
import { Terminal } from "./common/terminal.ts";
import { MarkdownRenderer } from "./common/terminal-markdown.ts";

const fetchUrl = tool({
  name: "fetchUrl",
  description: "Fetch the text content of a URL.",
  parameters: z.object({ url: z.string().url() }),
  execute: async ({ url }) => {
    const resp = await fetch(url);
    return resp.text();
  },
});

const webSearch = tool({
  name: "webSearch",
  description: "Search the web using DuckDuckGo and return results.",
  parameters: z.object({ query: z.string() }),
  execute: async ({ query }) => {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const resp = await fetch(url);
    const data = (await resp.json()) as Record<string, unknown>;
    const topics = (data.RelatedTopics as { Text?: string }[] | undefined) ?? [];
    const results = [
      data.AbstractText && `**Summary:** ${data.AbstractText}`,
      ...topics
        .slice(0, 5)
        .map((t) => t.Text)
        .filter(Boolean),
    ].filter(Boolean);
    return results.length ? results.join("\n\n") : "No results found.";
  },
});

const apiKey = process.env.ANTHROPIC_API_KEY;
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
const claude = anthropic("claude-sonnet-4-6", { apiKey });

const town = createAgentTown({
  runner,
  mayor: "director",
  sharedMemory: new InMemoryMemoryStore(),
  agents: {
    director: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      prompt: [
        "You are the director of a research town. You receive questions from the user and coordinate your specialist agents.",
        "",
        "Workflow for research questions:",
        "1. Send the question to the researcher via sendMessage.",
        "2. Call readInbox to wait for the researcher's findings.",
        "3. Send the original question and the researcher's findings to the writer via sendMessage.",
        "4. Call readInbox to wait for the writer's polished answer.",
        "5. Present the polished answer to the user.",
        "",
        "For simple or conversational questions, answer directly without delegating.",
        "Always reply with markdown formatting.",
      ].join("\n"),
    },

    researcher: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      tools: { fetchUrl, webSearch },
      prompt: [
        "You are a researcher in a multi-agent town. You receive research tasks from the director.",
        "Use webSearch to find relevant pages and fetchUrl to read them for details.",
        "Summarize your findings as concise bullet points. Include source URLs for specific facts.",
        "When finished, send your findings back to the director via sendMessage.",
      ].join("\n"),
    },

    writer: {
      llm: claude,
      memory: new InMemoryMemoryStore(),
      prompt: [
        "You are a writer in a multi-agent town. You receive a user question and research notes from the director.",
        "Transform the raw notes into clear, well-structured markdown prose.",
        "Be concise — a focused answer, not an essay. Preserve any source links.",
        "When finished, send your polished answer back to the director via sendMessage.",
      ].join("\n"),
    },
  },
});

// ---- terminal ----
const rl = createInterface({ input: process.stdin, output: process.stdout, historySize: 100 });
const term = new Terminal(rl);

// ---- abortable stream wrapper ----
async function* abortable(
  source: AsyncIterable<string>,
  signal: AbortSignal,
): AsyncGenerator<string> {
  if (signal.aborted) return;
  const iter = source[Symbol.asyncIterator]();
  const abortPromise = new Promise<void>((r) =>
    signal.addEventListener("abort", () => r(), { once: true }),
  );
  while (true) {
    let aborted = false;
    const result = await Promise.race([
      iter.next(),
      abortPromise.then(() => {
        aborted = true;
        return { done: true as const, value: "" };
      }),
    ]);
    if (aborted || result.done) break;
    yield result.value;
  }
}

// ---- SIGINT: interrupt turn or exit ----
let currentAc: AbortController | null = null;
let lastCtrlC = 0;
let savedPlaceholder = "";

rl.on("SIGINT", () => {
  if (currentAc) {
    term.stopSpinner();
    if (term.agentHasTextOnLine) process.stdout.write("\n");
    process.stdout.write("\x1b[2m(interrupted)\x1b[0m\n");
    currentAc.abort();
    currentAc = null;
  } else {
    const now = Date.now();
    if (now - lastCtrlC < 2_000) {
      process.stdout.write("\n");
      town.close().finally(() => {
        term.close();
        rl.close();
        process.exit(0);
      });
    } else {
      lastCtrlC = now;
      // biome-ignore lint/suspicious/noExplicitAny: readline internals
      savedPlaceholder = (rl as any).line ?? "";
      process.stdout.write("\n\x1b[2m(Ctrl+C again to exit)\x1b[0m\n");
      prompt();
    }
  }
});

// ---- REPL ----
function prompt(): void {
  term.printRule();
  rl.question(term.promptStr, async (input) => {
    input = input.trim();
    if (!input) return prompt();

    if (input === "exit" || input === "quit") {
      await town.close();
      term.close();
      rl.close();
      return;
    }

    const ac = new AbortController();
    currentAc = ac;
    term.agentHasTextOnLine = false;
    term.startSpinner("director thinking...");

    const md = new MarkdownRenderer({ width: process.stdout.columns ?? 80 });
    let labelShown = false;
    let streamError: Error | undefined;

    // Retry up to 5 times with 500ms gaps if the session is still settling
    // after a previous interrupted turn.
    let attempts = 0;
    while (attempts < 5) {
      try {
        for await (const chunk of abortable(town.stream(input, ac.signal), ac.signal)) {
          term.stopSpinner();
          if (!labelShown) {
            process.stdout.write("\nDirector:\n");
            labelShown = true;
          }
          const rendered = md.push(chunk);
          if (rendered) term.writeChunk(rendered);
          term.agentHasTextOnLine = true;
        }
        term.flushChunks();
        const tail = md.flush();
        if (tail) process.stdout.write(tail);
        streamError = undefined;
        break;
      } catch (err) {
        term.flushChunks();
        const e = err instanceof Error ? err : new Error(String(err));
        if (e.message.includes("busy") && attempts < 4 && !ac.signal.aborted) {
          attempts++;
          term.startSpinner(`settling… (${attempts})`);
          await new Promise((r) => setTimeout(r, 500));
          continue;
        }
        streamError = e;
        break;
      }
    }

    currentAc = null;
    term.stopSpinner();

    if (!ac.signal.aborted) {
      if (streamError) {
        if (term.agentHasTextOnLine) process.stdout.write("\n");
        process.stdout.write(`\x1b[31mError: ${streamError.message}\x1b[0m\n`);
      } else {
        process.stdout.write("\n");
      }
    }

    term.agentHasTextOnLine = false;
    prompt();
  });

  if (savedPlaceholder) {
    rl.write(savedPlaceholder);
    savedPlaceholder = "";
  }
}

console.log("\nTown Chat  agents=director,researcher,writer");
console.log("Type a question or 'exit' to quit. Ctrl+C to interrupt a turn.\n");
prompt();
