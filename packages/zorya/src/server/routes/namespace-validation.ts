// ---------------------------------------------------------------------------
// Namespace route helpers — keep Zorya's HTTP boundary strict while the
// workflow/agent engines continue treating namespace ids as opaque strings.
// ---------------------------------------------------------------------------

import { jsonError } from "../router.ts";
import {
  NamespaceArchivedError,
  NamespaceNotFoundError,
  normalizeNamespaceId,
  type NamespaceService,
} from "../services/namespaces.ts";

export async function resolveRequiredNamespaceId(
  namespaces: NamespaceService | undefined,
  input: string | undefined | null,
): Promise<{ namespaceId: string } | { response: Response }> {
  if (!input || input.length === 0) return { response: jsonError(400, "missing_namespaceId") };
  return resolveExplicitNamespaceId(namespaces, input);
}

export async function resolveOptionalNamespaceId(
  namespaces: NamespaceService | undefined,
  input: string | undefined | null,
): Promise<{ namespaceId?: string } | { response: Response }> {
  if (!input || input.length === 0) return {};
  return resolveExplicitNamespaceId(namespaces, input);
}

export async function resolveBodyNamespace(
  namespaces: NamespaceService | undefined,
  input: string | undefined | null,
): Promise<{ namespace: string } | { response: Response }> {
  try {
    if (namespaces) {
      return { namespace: (await namespaces.resolve(input)).id };
    }
    return { namespace: input && input.length > 0 ? normalizeNamespaceId(input) : "default" };
  } catch (err) {
    return { response: namespaceErrorResponse(err) };
  }
}

function resolveExplicitNamespaceId(
  namespaces: NamespaceService | undefined,
  input: string,
): Promise<{ namespaceId: string } | { response: Response }> {
  return Promise.resolve()
    .then(async () => {
      if (namespaces) return { namespaceId: (await namespaces.resolve(input)).id };
      return { namespaceId: normalizeNamespaceId(input) };
    })
    .catch((err) => ({ response: namespaceErrorResponse(err) }));
}

export function namespaceErrorResponse(err: unknown): Response {
  if (err instanceof NamespaceNotFoundError) return jsonError(404, "namespace_not_found");
  if (err instanceof NamespaceArchivedError) return jsonError(409, "namespace_archived");
  const message = err instanceof Error ? err.message : String(err);
  if (message === "invalid_namespace_id") return jsonError(400, "invalid_namespace_id");
  return jsonError(500, "namespace_failed", message);
}
