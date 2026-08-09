import { readFile } from "node:fs/promises";
import type { KnowledgeSourceAdapter, KnowledgeSourceInput } from "./knowledge-bases.ts";

export interface FileKnowledgeSourceConfig {
  readonly path: string;
  readonly id?: string;
  readonly title?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface UrlKnowledgeSourceConfig {
  readonly url: string;
  readonly id?: string;
  readonly title?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Host-side adapter for explicitly configured local files. */
export const fileKnowledgeSourceAdapter: KnowledgeSourceAdapter = {
  kind: "file",
  async load(raw): Promise<ReadonlyArray<KnowledgeSourceInput>> {
    const config = parseFileConfig(raw);
    const text = await readFile(config.path, "utf8");
    return [
      {
        id: config.id ?? config.path,
        text,
        title: config.title ?? config.path,
        uri: config.path,
        tags: config.tags,
        metadata: config.metadata,
      },
    ];
  },
};

/** Host-side adapter for explicitly configured HTTP(S) documents. */
export const urlKnowledgeSourceAdapter: KnowledgeSourceAdapter = {
  kind: "url",
  async load(raw): Promise<ReadonlyArray<KnowledgeSourceInput>> {
    const config = parseUrlConfig(raw);
    const response = await fetch(config.url);
    if (!response.ok) throw new Error(`source_fetch_failed:${response.status}`);
    const text = await response.text();
    return [
      {
        id: config.id ?? config.url,
        text,
        title: config.title ?? config.url,
        uri: config.url,
        tags: config.tags,
        metadata: config.metadata,
      },
    ];
  },
};

function parseFileConfig(raw: unknown): FileKnowledgeSourceConfig {
  if (!isRecord(raw) || typeof raw.path !== "string" || !raw.path.trim()) {
    throw new Error("file_source_path_required");
  }
  return raw as FileKnowledgeSourceConfig;
}

function parseUrlConfig(raw: unknown): UrlKnowledgeSourceConfig {
  if (!isRecord(raw) || typeof raw.url !== "string") throw new Error("url_source_url_required");
  const url = new URL(raw.url);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("url_source_protocol_unsupported");
  }
  return { ...(raw as UrlKnowledgeSourceConfig), url: url.toString() };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
