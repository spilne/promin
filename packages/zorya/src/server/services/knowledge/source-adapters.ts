// ---------------------------------------------------------------------------
// Built-in knowledge source adapters: fetch a URL, or read a file under a
// configured root. Neither is registered by default — each turns an operator
// request into server-side I/O, so a host opts in and bounds it:
//
//   - the URL adapter checks every hop (redirects included) with the same
//     outbound policy remote deployments use (public http(s) hosts only by
//     default) and connects to the address that check resolved, so a DNS
//     answer that changes in between (rebinding) is never followed; it caps
//     the body size and the time the fetch may take;
//   - the file adapter resolves paths inside its root only (symlinks are
//     resolved before the check) and caps the file size.
// ---------------------------------------------------------------------------

import { realpath, stat, readFile } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  OutboundRequestError,
  outboundFetch,
  type OutboundFetchTransport,
  type OutboundPolicy,
} from "@promin/agent";
import { KnowledgeSourceImportError, errorMessage } from "./errors.ts";
import type { KnowledgeSourceAdapter, KnowledgeSourceInput } from "./zorya-knowledge-bases.ts";

/** Default cap on a fetched or read document, in bytes. */
export const DEFAULT_MAX_SOURCE_BYTES = 2 * 1_048_576;

const SourceFieldsSchema = {
  id: z.string().trim().min(1).max(512).optional(),
  title: z.string().max(512).optional(),
  tags: z.array(z.string().max(128)).max(64).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
};

/** `config` of the `url` kind. */
export const UrlSourceConfigSchema = z
  .object({ url: z.string().min(1).max(4096), ...SourceFieldsSchema })
  .strict();

/** `config` of the `file` kind; `path` is relative to the adapter's root. */
export const FileSourceConfigSchema = z
  .object({ path: z.string().min(1).max(4096), ...SourceFieldsSchema })
  .strict();

export interface UrlKnowledgeSourceAdapterConfig {
  /** Which hosts may be fetched. Default: public http(s) hosts only. */
  policy?: OutboundPolicy;
  /** Largest body accepted, in bytes. Default 2 MiB. */
  maxBytes?: number;
  /** Deadline for the whole fetch, redirects and body included. Default 15 000 ms. */
  timeoutMs?: number;
  /** Redirects followed (each target is checked against `policy`). Default 3. */
  maxRedirects?: number;
  /**
   * Transport. Default: global `fetch`. It receives the URL with the host
   * replaced by the vetted IP (the name travels in `Host` and TLS SNI).
   * Tests pass a fake.
   */
  fetch?: OutboundFetchTransport;
  /** Time source for the deadline. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/** `kind: "url"` — fetch one HTTP(S) document; HTML is reduced to its text. */
export function createUrlKnowledgeSourceAdapter(
  config: UrlKnowledgeSourceAdapterConfig = {},
): KnowledgeSourceAdapter {
  const maxBytes = config.maxBytes ?? DEFAULT_MAX_SOURCE_BYTES;
  const timeoutMs = config.timeoutMs ?? 15_000;
  const maxRedirects = config.maxRedirects ?? 3;
  const clock = config.clock ?? SystemWallClock;

  return {
    kind: "url",
    async load({ config: raw }) {
      const parsed = parseConfig({ schema: UrlSourceConfigSchema, raw });
      const controller = new AbortController();
      let timedOut = false;
      const timer = clock.setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, timeoutMs);
      try {
        const { response: res, url } = await outboundFetch({
          url: parsed.url,
          init: { signal: controller.signal },
          maxRedirects,
          ...(config.policy !== undefined && { policy: config.policy }),
          ...(config.fetch !== undefined && { fetch: config.fetch }),
        });
        if (!res.ok) {
          await res.body?.cancel().catch(() => {});
          throw unavailable(`GET ${url} answered HTTP ${res.status}.`);
        }
        const mimeType = (res.headers.get("content-type") ?? "text/plain")
          .split(";")[0]!
          .trim()
          .toLowerCase();
        if (!isTextual(mimeType)) {
          await res.body?.cancel().catch(() => {});
          throw invalid(`${url} is ${mimeType}; only text documents can be imported.`);
        }
        const body = await readCapped({ res, maxBytes, what: url });
        const html = mimeType === "text/html" || mimeType === "application/xhtml+xml";
        const text = html ? htmlToText(body) : body;
        const title = parsed.title ?? (html ? htmlTitle(body) : undefined) ?? url;
        return [
          sourceInput({
            parsed,
            id: parsed.id ?? url,
            text,
            title,
            uri: url,
            mimeType,
          }),
        ];
      } catch (err) {
        if (err instanceof KnowledgeSourceImportError) throw err;
        if (err instanceof OutboundRequestError) {
          throw err.code === "too_many_redirects" ? unavailable(err.message) : invalid(err.message);
        }
        if (timedOut) throw unavailable(`${parsed.url} did not answer within ${timeoutMs} ms.`);
        throw unavailable(`Fetching ${parsed.url} failed: ${errorMessage(err)}`, err);
      } finally {
        timer.clear();
      }
    },
  };
}

export interface FileKnowledgeSourceAdapterConfig {
  /** Directory imports are confined to. */
  root: string;
  /** Largest file accepted, in bytes. Default 2 MiB. */
  maxBytes?: number;
}

/** `kind: "file"` — read one UTF-8 text file under `root`. */
export function createFileKnowledgeSourceAdapter(
  config: FileKnowledgeSourceAdapterConfig,
): KnowledgeSourceAdapter {
  const maxBytes = config.maxBytes ?? DEFAULT_MAX_SOURCE_BYTES;
  return {
    kind: "file",
    async load({ config: raw }) {
      const parsed = parseConfig({ schema: FileSourceConfigSchema, raw });
      const root = await realpath(config.root);
      let file: string;
      try {
        file = await realpath(path.resolve(root, parsed.path));
      } catch {
        throw invalid(`File "${parsed.path}" does not exist under the import root.`);
      }
      if (file !== root && !file.startsWith(root + path.sep)) {
        throw invalid(`File "${parsed.path}" is outside the import root.`);
      }
      const info = await stat(file);
      if (!info.isFile()) throw invalid(`"${parsed.path}" is not a file.`);
      if (info.size > maxBytes) {
        throw invalid(`"${parsed.path}" is ${info.size} bytes; the limit is ${maxBytes}.`);
      }
      const bytes = await readFile(file);
      if (bytes.includes(0)) throw invalid(`"${parsed.path}" is not a text file.`);
      const relative = path.relative(root, file);
      const mimeType = mimeTypeFor(file);
      const decoded = new TextDecoder().decode(bytes);
      return [
        sourceInput({
          parsed,
          id: parsed.id ?? relative,
          text: mimeType === "text/html" ? htmlToText(decoded) : decoded,
          title: parsed.title ?? relative,
          uri: relative,
          mimeType,
        }),
      ];
    },
  };
}

function parseConfig<S extends z.ZodTypeAny>(params: { schema: S; raw: unknown }): z.output<S> {
  const result = params.schema.safeParse(params.raw);
  if (result.success) return result.data;
  throw invalid(
    result.error.issues
      .map((i) => (i.path.length > 0 ? `config.${i.path.join(".")}: ${i.message}` : i.message))
      .join("; "),
  );
}

function sourceInput(params: {
  parsed: z.output<typeof UrlSourceConfigSchema> | z.output<typeof FileSourceConfigSchema>;
  id: string;
  text: string;
  title: string;
  uri: string;
  mimeType: string;
}): KnowledgeSourceInput {
  const { parsed } = params;
  return {
    id: params.id,
    text: params.text,
    title: params.title,
    uri: params.uri,
    mimeType: params.mimeType,
    ...(parsed.tags !== undefined && { tags: parsed.tags }),
    ...(parsed.metadata !== undefined && { metadata: parsed.metadata }),
  };
}

function invalid(message: string): KnowledgeSourceImportError {
  return new KnowledgeSourceImportError({ reason: "invalid", message });
}

function unavailable(message: string, cause?: unknown): KnowledgeSourceImportError {
  return new KnowledgeSourceImportError({
    reason: "unavailable",
    message,
    ...(cause !== undefined && { cause }),
  });
}

function isTextual(mimeType: string): boolean {
  return (
    mimeType.startsWith("text/") ||
    mimeType === "application/json" ||
    mimeType === "application/xml" ||
    mimeType === "application/xhtml+xml" ||
    mimeType.endsWith("+json") ||
    mimeType.endsWith("+xml")
  );
}

/** Read a response body as UTF-8, refusing more than `maxBytes`. */
async function readCapped(params: {
  res: Response;
  maxBytes: number;
  what: string;
}): Promise<string> {
  const { res, maxBytes, what } = params;
  const declared = Number(res.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maxBytes) {
    await res.body?.cancel().catch(() => {});
    throw invalid(`${what} is ${declared} bytes; the limit is ${maxBytes}.`);
  }
  if (!res.body) return "";
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => {});
      throw invalid(`${what} is larger than the ${maxBytes}-byte limit.`);
    }
    text += decoder.decode(value, { stream: true });
  }
  return text + decoder.decode();
}

const BLOCK_TAGS =
  /<\/?(?:p|div|section|article|header|footer|li|ul|ol|tr|table|h[1-6]|pre|blockquote|br|hr)\b[^>]*>/gi;

/** Reduce an HTML page to paragraph-separated text. */
export function htmlToText(html: string): string {
  return decodeEntities(
    html
      .replace(/<!--[\s\S]*?-->/g, "")
      .replace(/<(script|style|noscript|template|svg|head)\b[\s\S]*?<\/\1\s*>/gi, "")
      .replace(BLOCK_TAGS, "\n\n")
      .replace(/<[^>]+>/g, ""),
  )
    .split("\n")
    .map((line) => line.replace(/[ \t\f\v ]+/g, " ").trim())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function htmlTitle(html: string): string | undefined {
  const match = /<title\b[^>]*>([\s\S]*?)<\/title\s*>/i.exec(html);
  const title = match ? decodeEntities(match[1]!).replace(/\s+/g, " ").trim() : "";
  return title.length > 0 ? title : undefined;
}

const NAMED_ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
  nbsp: " ",
};

function decodeEntities(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, ref: string) => {
    if (ref[0] === "#") {
      const code =
        ref[1] === "x" || ref[1] === "X" ? parseInt(ref.slice(2), 16) : parseInt(ref.slice(1), 10);
      return Number.isFinite(code) && code > 0 && code <= 0x10ffff
        ? String.fromCodePoint(code)
        : whole;
    }
    return NAMED_ENTITIES[ref.toLowerCase()] ?? whole;
  });
}

function mimeTypeFor(file: string): string {
  const ext = path.extname(file).toLowerCase();
  if (ext === ".md" || ext === ".markdown") return "text/markdown";
  if (ext === ".json") return "application/json";
  if (ext === ".csv") return "text/csv";
  if (ext === ".html" || ext === ".htm") return "text/html";
  return "text/plain";
}
