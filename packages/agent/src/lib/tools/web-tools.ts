import { z } from "zod";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { tool } from "../tool.ts";
import {
  OutboundRequestError,
  outboundFetch,
  type OutboundFetchTransport,
} from "../outbound/outbound-fetch.ts";
import type { OutboundPolicy } from "../outbound/outbound-policy.ts";

export interface WebToolsConfig {
  /** Fetch timeout in milliseconds, body included. Default: 15 000 ms. */
  timeoutMs?: number;
  /** Time source for the timeout. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export interface FetchUrlToolConfig extends WebToolsConfig {
  /**
   * Which hosts the model may fetch. Default: public http(s) hosts only —
   * loopback, private, link-local (cloud metadata) and internal names are
   * refused, every redirect hop is checked, and the connection goes to the
   * address the check vetted.
   */
  policy?: OutboundPolicy;
  /** Bytes of body returned before the rest is cut off. Default: 200 000. */
  maxBytes?: number;
  /** Redirects followed, each checked against `policy`. Default: 3. */
  maxRedirects?: number;
  /**
   * Ask a human before each fetch. Default: `true` — the URL comes from the
   * model, which may be steered by untrusted content.
   */
  requireApproval?: boolean;
  /** Underlying transport. Default: global `fetch`. Tests pass a fake. */
  fetch?: OutboundFetchTransport;
}

const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_MAX_BYTES = 200_000;

/**
 * A tool that fetches one http(s) URL and returns its text. Guarded
 * against server-side request forgery by `policy` (see `FetchUrlToolConfig`),
 * capped in size and time, and approval-gated by default.
 */
export function createFetchUrlTool(config: FetchUrlToolConfig = {}) {
  const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxBytes = config.maxBytes ?? DEFAULT_MAX_BYTES;
  const clock = config.clock ?? SystemWallClock;
  return tool({
    name: "fetchUrl",
    description: `Fetch the text content of a public http(s) URL. Responses over ${maxBytes} bytes are truncated.`,
    parameters: z.object({ url: z.string().url() }),
    requireApproval: config.requireApproval ?? true,
    execute: async ({ url }) => {
      const controller = new AbortController();
      let timedOut = false;
      const timer = clock.setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, timeoutMs);
      try {
        const { response, url: finalUrl } = await outboundFetch({
          url,
          init: { signal: controller.signal },
          ...(config.policy !== undefined && { policy: config.policy }),
          ...(config.maxRedirects !== undefined && { maxRedirects: config.maxRedirects }),
          ...(config.fetch !== undefined && { fetch: config.fetch }),
        });
        if (!response.ok) {
          await response.body?.cancel().catch(() => {});
          return `Error: GET ${finalUrl} answered HTTP ${response.status}${response.statusText ? ` ${response.statusText}` : ""}.`;
        }
        const mimeType = (response.headers.get("content-type") ?? "text/plain")
          .split(";")[0]!
          .trim()
          .toLowerCase();
        if (!isTextual(mimeType)) {
          await response.body?.cancel().catch(() => {});
          return `Error: ${finalUrl} is ${mimeType}; only text responses can be read.`;
        }
        const { text, truncated } = await readCapped({ response, maxBytes });
        return truncated ? `${text}\n[…response truncated at ${maxBytes} bytes]` : text;
      } catch (err) {
        if (err instanceof OutboundRequestError) return `Refused: ${err.message}`;
        if (timedOut) return `Error: ${url} did not answer within ${timeoutMs} ms.`;
        throw err;
      } finally {
        timer.clear();
      }
    },
  });
}

function isTextual(mimeType: string): boolean {
  return (
    mimeType.startsWith("text/") ||
    mimeType === "application/json" ||
    mimeType === "application/xml" ||
    mimeType === "application/javascript" ||
    mimeType === "application/xhtml+xml" ||
    mimeType.endsWith("+json") ||
    mimeType.endsWith("+xml")
  );
}

/** Read at most `maxBytes` of a body as UTF-8, cancelling the rest. */
async function readCapped(params: {
  response: Response;
  maxBytes: number;
}): Promise<{ text: string; truncated: boolean }> {
  const { response, maxBytes } = params;
  if (!response.body) return { text: "", truncated: false };
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    const room = maxBytes - total;
    if (value.byteLength > room) {
      text += decoder.decode(value.subarray(0, room), { stream: true });
      await reader.cancel().catch(() => {});
      return { text: text + decoder.decode(), truncated: true };
    }
    total += value.byteLength;
    text += decoder.decode(value, { stream: true });
  }
  return { text: text + decoder.decode(), truncated: false };
}

export function createWebSearchTool(config: WebToolsConfig = {}) {
  const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const clock = config.clock ?? SystemWallClock;
  return tool({
    name: "webSearch",
    description: "Search the web using DuckDuckGo and return results.",
    parameters: z.object({ query: z.string() }),
    execute: async ({ query }) => {
      const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
      const controller = new AbortController();
      const timer = clock.setTimeout(() => controller.abort(), timeoutMs);
      try {
        const resp = await fetch(url, { signal: controller.signal });
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
      } finally {
        timer.clear();
      }
    },
  });
}

/** Prebuilt fetchUrl tool: public hosts only, approval required, 15 s timeout. */
export const fetchUrl = createFetchUrlTool();

/** Prebuilt webSearch tool (DuckDuckGo) with default 15 s timeout. */
export const webSearch = createWebSearchTool();
