// ---------------------------------------------------------------------------
// Fragment registry routes — author / edit / browse prompt fragments. The
// dashboard's fragments manager calls these; the agent editor's layered-
// prompt picker reads /api/agents/_catalog/fragments (a parallel surface
// over the same registry).
//
// Routes:
//   GET    /api/fragments              — list all
//   GET    /api/fragments/:key         — get one
//   POST   /api/fragments              — create a custom fragment
//   PATCH  /api/fragments/:key         — update content
//   DELETE /api/fragments/:key         — delete
//   GET    /api/fragments/_sources     — { fileManaged: string[] }
// ---------------------------------------------------------------------------

import type { FragmentRegistry } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";

export interface FragmentDto {
  readonly key: string;
  readonly content: string;
}

export interface FragmentsListResponse {
  fragments: FragmentDto[];
}

export interface FragmentSourcesResponse {
  /** Fragment keys currently backed by a file on disk — read-only in the UI. */
  fileManaged: string[];
}

export interface FragmentGatewayDeps {
  readonly registry: FragmentRegistry;
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Same `_`-prefixed reserved namespace as agents/skills — protects `_sources`.
const KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_\-./]{0,127}$/;
function isValidKey(key: string): boolean {
  return KEY_PATTERN.test(key);
}

// ---------------------------------------------------------------------------
// List + get + sources
// ---------------------------------------------------------------------------

export function listFragments(deps: FragmentGatewayDeps) {
  return async (): Promise<Response> => {
    const fragments = deps.registry.list().map((f) => ({ key: f.key, content: f.content }));
    return json(200, { fragments } satisfies FragmentsListResponse);
  };
}

export function getFragment(deps: FragmentGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    const content = deps.registry.get(key);
    if (content === undefined) {
      return jsonError(404, "fragment_not_found", `Fragment "${key}" is not registered.`);
    }
    return json(200, { key, content } satisfies FragmentDto);
  };
}

export function listFragmentSources(deps: { readonly fileManagedIds: () => string[] }) {
  return async (): Promise<Response> => {
    return json(200, { fileManaged: deps.fileManagedIds() } satisfies FragmentSourcesResponse);
  };
}

// ---------------------------------------------------------------------------
// Create / update / delete
// ---------------------------------------------------------------------------

interface FragmentBody {
  readonly key?: unknown;
  readonly content?: unknown;
}

export function createFragment(deps: FragmentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<FragmentBody>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.key !== "string" || body.key.length === 0) {
      return jsonError(400, "missing_key");
    }
    if (body.key.startsWith("_")) {
      return jsonError(400, "reserved_key_prefix", "Fragment keys starting with `_` are reserved.");
    }
    if (!isValidKey(body.key)) {
      return jsonError(
        400,
        "invalid_key",
        "Fragment keys must start with a letter and contain only letters, digits, `_`, `-`, `.`, `/`.",
      );
    }
    if (typeof body.content !== "string" || body.content.length === 0) {
      return jsonError(400, "missing_content");
    }
    if (deps.registry.get(body.key) !== undefined) {
      return jsonError(
        409,
        "fragment_exists",
        `Fragment "${body.key}" already exists — use PATCH to update.`,
      );
    }
    try {
      deps.registry.set(body.key, body.content);
      return json(201, { key: body.key, content: body.content } satisfies FragmentDto);
    } catch (err) {
      return jsonError(500, "create_failed", asMessage(err));
    }
  };
}

export function updateFragment(deps: FragmentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    const body = await readJson<FragmentBody>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.content !== "string" || body.content.length === 0) {
      return jsonError(400, "missing_content");
    }
    if (deps.registry.get(key) === undefined) {
      return jsonError(404, "fragment_not_found", `Fragment "${key}" is not registered.`);
    }
    try {
      deps.registry.set(key, body.content);
      return json(200, { key, content: body.content } satisfies FragmentDto);
    } catch (err) {
      return jsonError(500, "update_failed", asMessage(err));
    }
  };
}

export function deleteFragment(deps: FragmentGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    try {
      deps.registry.delete(key);
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "delete_failed", asMessage(err));
    }
  };
}
