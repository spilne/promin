// ---------------------------------------------------------------------------
// Errors the managed knowledge-base service throws. The HTTP routes map each
// to a status (see routes/knowledge-bases.ts); embedders catch them by class.
// ---------------------------------------------------------------------------

/** No managed knowledge base has this id. */
export class KnowledgeBaseNotFoundError extends Error {
  readonly id: string;
  constructor(id: string) {
    super(`Knowledge base "${id}" does not exist.`);
    this.name = "KnowledgeBaseNotFoundError";
    this.id = id;
  }
}

/** The id is taken, by a managed knowledge base or a host-wired retriever. */
export class KnowledgeBaseExistsError extends Error {
  readonly id: string;
  constructor(params: { id: string; message?: string }) {
    super(params.message ?? `Knowledge base "${params.id}" already exists.`);
    this.name = "KnowledgeBaseExistsError";
    this.id = params.id;
  }
}

/** The knowledge base has no source with this id. */
export class KnowledgeSourceNotFoundError extends Error {
  readonly baseId: string;
  readonly sourceId: string;
  constructor(params: { baseId: string; sourceId: string }) {
    super(`Knowledge base "${params.baseId}" has no source "${params.sourceId}".`);
    this.name = "KnowledgeSourceNotFoundError";
    this.baseId = params.baseId;
    this.sourceId = params.sourceId;
  }
}

/** An id, a source or an import config the service refuses before doing any work. */
export class KnowledgeInputError extends Error {
  readonly code: string;
  constructor(params: { code: string; message: string }) {
    super(params.message);
    this.name = "KnowledgeInputError";
    this.code = params.code;
  }
}

/**
 * Chunking, embedding or storing a source's chunks failed. The source is
 * recorded with status `failed` and this message, and its partial chunks
 * are dropped.
 */
export class KnowledgeIngestError extends Error {
  readonly sourceId: string;
  constructor(params: { sourceId: string; cause: unknown }) {
    super(`Indexing source "${params.sourceId}" failed: ${errorMessage(params.cause)}`, {
      cause: params.cause,
    });
    this.name = "KnowledgeIngestError";
    this.sourceId = params.sourceId;
  }
}

/**
 * A source adapter could not load its documents. `invalid`: the config or
 * the target is refused (bad URL, private host, path outside the root,
 * unsupported content). `unavailable`: the target failed to answer (HTTP
 * error, timeout, network error).
 */
export class KnowledgeSourceImportError extends Error {
  readonly reason: "invalid" | "unavailable";
  constructor(params: { reason: "invalid" | "unavailable"; message: string; cause?: unknown }) {
    super(params.message, params.cause === undefined ? undefined : { cause: params.cause });
    this.name = "KnowledgeSourceImportError";
    this.reason = params.reason;
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
