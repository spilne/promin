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
