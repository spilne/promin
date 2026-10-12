// ---------------------------------------------------------------------------
// HTTP conventions shared by every versioned registry resource (agents,
// roles, skills, DAGs, fragments):
//
//   - A response carrying one entry has `etag: W/"<version>"` — the version
//     it shows. Versions are immutable, so the tag names the content.
//   - An edit (PATCH) creates a new version from the active one and must
//     send `If-Match` with the active version's tag (428 without it; `*`
//     opts out). The tag goes to the store as `expectedActive`, so the
//     check and the write are one compare-and-set: a concurrent edit
//     answers 412 and writes nothing.
//   - `POST /:id/promote` makes a version active; `DELETE` removes one
//     version or the whole id. Both honour an optional `If-Match`; removing
//     the active version while others remain needs `If-Match` and
//     `?promote=<version>`.
//   - File-managed (the active version's `source` is `"file"`) is stored,
//     so every replica answers `/_sources` and the 409s the same.
//   - `POST` creates a new id only (409 when it exists), atomically.
// ---------------------------------------------------------------------------

import {
  FILE_SOURCE,
  isRegistryConflict,
  isRegistryInputError,
  isRegistryNotFound,
  listAllVersioned,
  type Versioned,
  type VersionedRegistry,
} from "@promin/agent";
import { HttpError, json, jsonError, readJson } from "../router.ts";

/** Entity tag of a version. */
export function versionEtag(version: string): string {
  return `W/"${version}"`;
}

/** `res` with the entity tag of `version`. */
export function withVersionEtag(res: Response, version: string): Response {
  res.headers.set("etag", versionEtag(version));
  return res;
}

/** One entry as a JSON response tagged with its version. */
export function entryResponse(params: {
  status: number;
  entry: { readonly version: string };
  body?: unknown;
}): Response {
  return withVersionEtag(json(params.status, params.body ?? params.entry), params.entry.version);
}

/**
 * The versions `If-Match` names (weak and strong tags alike). `undefined`
 * for `If-Match: *`, and for an absent header when it is optional. Throws
 * 428 when `required` and the header is missing.
 */
export function ifMatchVersions(params: {
  req: Request;
  required: boolean;
}): ReadonlyArray<string> | undefined {
  const header = params.req.headers.get("if-match");
  if (header === null) {
    if (!params.required) return undefined;
    throw new HttpError({
      status: 428,
      code: "precondition_required",
      message: "Send If-Match with the etag of the version you edited (or `*`).",
    });
  }
  const tags = header.split(",").map((t) => t.trim());
  if (tags.includes("*")) return undefined;
  return tags.map((tag) => {
    const opaque = tag.startsWith("W/") ? tag.slice(2) : tag;
    return opaque.startsWith('"') && opaque.endsWith('"') && opaque.length >= 2
      ? opaque.slice(1, -1)
      : opaque;
  });
}

/**
 * Check `If-Match` against the active version just read: 412 when no tag
 * names it. Returns the version to pass on as `expectedActive` — the store
 * then refuses the write if another one moved the active version since —
 * or `undefined` when the caller sent no condition.
 */
export function matchActiveVersion(params: {
  tags: ReadonlyArray<string> | undefined;
  active: string;
}): string | undefined {
  if (params.tags === undefined) return undefined;
  if (params.tags.includes(params.active)) return params.active;
  throw new HttpError({
    status: 412,
    code: "precondition_failed",
    message: `The active version is ${params.active}; fetch it again and retry.`,
  });
}

/**
 * Run a registry call, mapping its errors to HTTP: a lost compare-and-set
 * → 412, an immutable-version clash → 409, an unknown id or version → 404,
 * a malformed input → 400.
 */
export async function registryCall<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    throw toHttpError(err);
  }
}

/** A registry error as the `HttpError` it answers; any other error unchanged. */
function toHttpError(err: unknown): unknown {
  if (isRegistryConflict(err)) {
    return err.code === "active_mismatch"
      ? new HttpError({
          status: 412,
          code: "precondition_failed",
          message: `The active version is ${err.actual ?? "gone"}; fetch it again and retry.`,
        })
      : new HttpError({ status: 409, code: "version_conflict", message: err.message });
  }
  if (isRegistryNotFound(err)) {
    return new HttpError({ status: 404, code: "version_not_found", message: err.message });
  }
  if (isRegistryInputError(err)) {
    return new HttpError({ status: 400, code: "invalid_input", message: err.message });
  }
  return err;
}

/**
 * Register a brand-new id: `register` passes `expectedActive: null`, which
 * makes the existence check and the write one step. `onExists` answers
 * when the id is taken; other registry errors map like `registryCall`.
 */
export async function createOnly<T>(params: {
  register: () => Promise<T>;
  onExists: () => Response;
}): Promise<T | Response> {
  try {
    return await params.register();
  } catch (err) {
    if (isRegistryConflict(err) && err.code === "active_mismatch") return params.onExists();
    throw toHttpError(err);
  }
}

/** Optional `version` label from a request body field. */
export function optionalVersion(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

export interface VersionedRouteDeps<TBody> {
  /** The routes only read, promote and remove, so filter and input types don't matter. */
  readonly registry: VersionedRegistry<TBody, unknown, unknown>;
  readonly notFound: (id: string) => Response;
  /** What a client sees of an entry (e.g. secrets redacted). Default: as stored. */
  readonly present?: (entry: Versioned<TBody>) => unknown;
  /**
   * DELETE refuses an entry whose active version a file wrote (the next scan
   * would bring it back). Promote is always allowed: a scan never undoes it.
   */
  readonly refuseFileManaged?: boolean;
}

/** Whether `entry` (an active version) is managed by a file on disk. */
export function isFileManagedEntry(entry: { readonly source?: string } | null): boolean {
  return entry?.source === FILE_SOURCE;
}

/** `GET /_sources` — ids whose active version a file wrote; stored, so every replica agrees. */
export function listFileManagedRoute<TBody>(deps: {
  readonly registry: VersionedRegistry<TBody, unknown, unknown>;
}) {
  return async (): Promise<Response> => {
    const managed = await listAllVersioned({ registry: deps.registry, source: FILE_SOURCE });
    return json(200, { fileManaged: managed.map((e) => e.id) });
  };
}

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `"${id}" is managed by a file on disk; edit the file instead.`,
  );
}

/** `GET /:id/versions` — every version, oldest first, each flagged `active`. */
export function listVersionsRoute<TBody>(deps: VersionedRouteDeps<TBody>) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions({ id });
    if (versions.length === 0) return deps.notFound(id);
    return json(200, { versions: versions.map((v) => (deps.present ? deps.present(v) : v)) });
  };
}

/**
 * `POST /:id/promote` `{ version }` — make a version active; optional
 * `If-Match`. `beforePromote` may refuse (e.g. a failing eval gate); it
 * sees the request body for its own flags.
 */
export function promoteRoute<TBody>(
  deps: VersionedRouteDeps<TBody> & {
    readonly beforePromote?: (params: {
      req: Request;
      id: string;
      version: string;
      body: Readonly<Record<string, unknown>>;
    }) => Promise<Response | undefined>;
  },
) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<Record<string, unknown>>(req);
    const version = optionalVersion(body?.version);
    if (version === undefined) return jsonError(400, "missing_version");
    const ifMatch = ifMatchVersions({ req, required: false });
    const active = await deps.registry.get({ id });
    if (!active) return deps.notFound(id);
    const expectedActive = matchActiveVersion({ tags: ifMatch, active: active.version });
    // An unknown version is left to `promote`, which answers 404.
    if (
      deps.beforePromote &&
      version !== active.version &&
      (await deps.registry.get({ id, version }))
    ) {
      const refused = await deps.beforePromote({ req, id, version, body: body ?? {} });
      if (refused) return refused;
    }
    const entry = await registryCall(() =>
      deps.registry.promote({
        id,
        version,
        ...(expectedActive !== undefined && { expectedActive }),
      }),
    );
    return entryResponse({
      status: 200,
      entry,
      body: deps.present ? deps.present(entry) : entry,
    });
  };
}

/**
 * `DELETE /:id[?version=][&promote=]` — one version, or the whole id;
 * optional `If-Match`. Removing the active version while others remain
 * needs `If-Match` (428) and `?promote=<version>` naming the replacement
 * (409 `active_version` without it), so a delete never silently activates
 * a draft. `beforeDelete` may refuse (e.g. while something references it).
 */
export function deleteRoute<TBody>(
  deps: VersionedRouteDeps<TBody> & {
    readonly beforeDelete?: (params: {
      req: Request;
      id: string;
      version?: string;
    }) => Promise<Response | undefined>;
  },
) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const promote = optionalVersion(url.searchParams.get("promote"));
    const active = await deps.registry.get({ id });
    if (!active || (version !== undefined && !(await deps.registry.get({ id, version })))) {
      return deps.notFound(id);
    }
    if (deps.refuseFileManaged && isFileManagedEntry(active)) return fileManaged409(id);
    const removingActive = version === active.version;
    const others = removingActive ? (await deps.registry.versions({ id })).length > 1 : false;
    const expectedActive = matchActiveVersion({
      tags: ifMatchVersions({ req, required: others }),
      active: active.version,
    });
    if (others && promote === undefined) {
      return jsonError(
        409,
        "active_version",
        `${active.version} is the active version; name the version to activate instead with ?promote=<version>.`,
      );
    }
    const refused = await deps.beforeDelete?.({
      req,
      id,
      ...(version !== undefined && { version }),
    });
    if (refused) return refused;
    await registryCall(() =>
      deps.registry.unregister({
        id,
        ...(version !== undefined && { version }),
        ...(expectedActive !== undefined && { expectedActive }),
        ...(removingActive && promote !== undefined && { promote }),
      }),
    );
    return new Response(null, { status: 204 });
  };
}
