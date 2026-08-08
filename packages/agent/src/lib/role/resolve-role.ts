// ---------------------------------------------------------------------------
// `resolveRoleBinding` — turn an agent's `RoleBinding` into a concrete
// `RoleDefinition`. The async step (registry lookup for a `ref`) lives here
// so the host resolves it BEFORE calling the sync `resolveLocalAgent`, the
// same pattern `resolveCredentialRef` / `resolveSkillCatalog` follow.
//
//   - `inline` resolves with no I/O — just hand back the embedded definition.
//   - `ref` reads the RoleRegistry. Throws when no registry is wired or the
//     referenced role/version doesn't exist, so a dangling binding surfaces
//     loudly at resolve time rather than silently dropping the persona.
// ---------------------------------------------------------------------------

import type { RoleBinding, RoleDefinition, RoleRegistry } from "./types.ts";

/**
 * The role definition when it's knowable synchronously — i.e. an `inline`
 * binding. A `ref`'s definition lives in the RoleRegistry, so this returns
 * `undefined` for refs; callers that need a ref's definition must resolve it
 * via `resolveRoleBinding`. Handy for read-only, sync contexts (listings,
 * tool-ref reconciliation, UI) that shouldn't do I/O.
 */
export function inlineRoleDefinition(
  binding: RoleBinding | null | undefined,
): RoleDefinition | undefined {
  if (!binding || typeof binding !== "object") return undefined;
  return "inline" in binding ? binding.inline : undefined;
}

export interface ResolveRoleBindingDeps {
  /** Required to resolve `ref` bindings. `inline` bindings don't need it. */
  readonly roles?: RoleRegistry;
}

export async function resolveRoleBinding(
  binding: RoleBinding,
  deps: ResolveRoleBindingDeps = {},
): Promise<RoleDefinition> {
  if ("inline" in binding) return binding.inline;

  const { id, version } = binding.ref;
  if (!deps.roles) {
    throw new Error(
      `resolveRoleBinding: role ref "${id}" needs a RoleRegistry, but none was wired. ` +
        "Pass `roles` so the binding can be resolved.",
    );
  }
  const role = await deps.roles.get(id, version);
  if (!role) {
    throw new Error(
      `resolveRoleBinding: role "${id}"${version ? `@${version}` : ""} not found in the registry.`,
    );
  }
  return role.definition;
}
