// ---------------------------------------------------------------------------
// Errors the managed knowledge-base service throws. The HTTP routes map each
// to a status (see routes/knowledge-bases.ts); embedders catch them by class.
// ---------------------------------------------------------------------------

/** No managed knowledge base has this id in this namespace. */
export class KnowledgeBaseNotFoundError extends Error {
  readonly namespaceId: string;
  readonly id: string;
  constructor(params: { namespaceId: string; id: string }) {
    super(`Knowledge base "${params.id}" does not exist in namespace "${params.namespaceId}".`);
    this.name = "KnowledgeBaseNotFoundError";
    this.namespaceId = params.namespaceId;
    this.id = params.id;
  }
}

/** The id is taken in the namespace, by a managed knowledge base or a host-wired retriever. */
export class KnowledgeBaseExistsError extends Error {
  readonly namespaceId: string;
  readonly id: string;
  constructor(params: { namespaceId: string; id: string; message?: string }) {
    super(
      params.message ??
        `Knowledge base "${params.id}" already exists in namespace "${params.namespaceId}".`,
    );
    this.name = "KnowledgeBaseExistsError";
    this.namespaceId = params.namespaceId;
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
