import { z } from "zod";
import { tool } from "../tool.ts";

export interface WebToolsConfig {
  /** Fetch timeout in milliseconds. Default: 15 000 ms. */
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 15_000;

export function createFetchUrlTool(config: WebToolsConfig = {}) {
  const { timeoutMs = DEFAULT_TIMEOUT_MS } = config;
  return tool({
    name: "fetchUrl",
    description: "Fetch the text content of a URL.",
    parameters: z.object({ url: z.string().url() }),
    execute: async ({ url }) => {
      const resp = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      return resp.text();
    },
  });
}

export function createWebSearchTool(config: WebToolsConfig = {}) {
  const { timeoutMs = DEFAULT_TIMEOUT_MS } = config;
  return tool({
    name: "webSearch",
    description: "Search the web using DuckDuckGo and return results.",
    parameters: z.object({ query: z.string() }),
    execute: async ({ query }) => {
      const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
      const resp = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      const data = (await resp.json()) as Record<string, unknown>;
      const topics = (data.RelatedTopics as { Text?: string }[] | undefined) ?? [];
      const results = [
        data.AbstractText && `**Summary:** ${data.AbstractText}`,
        ...topics
          .slice(0, 5)
          .map((t) => t.Text)
          .filter(Boolean),
      ].filter(Boolean);
      return results.length ? (results as string[]).join("\n\n") : "No results found.";
    },
  });
}

/** Prebuilt fetchUrl tool with default 15 s timeout. */
export const fetchUrl = createFetchUrlTool();

/** Prebuilt webSearch tool (DuckDuckGo) with default 15 s timeout. */
export const webSearch = createWebSearchTool();
