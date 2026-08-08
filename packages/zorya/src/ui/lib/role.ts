// ---------------------------------------------------------------------------
// role.ts — UI-local mirror of `@promin/agent`'s `inlineRoleDefinition`.
//
// The UI must never import `@promin/agent` as a *value*: that barrel drags
// the whole server-side module graph (down to `cross-spawn`/`child_process`)
// into the browser bundle, which Bun rejects with "Browser build cannot
// require() Node.js builtin". Role/agent *types* may still flow into the UI
// through type-only imports (erased at build time); only the runtime helper
// needs a browser-safe stand-in.
//
// The body is identical to the source helper — narrow on the `inline` arm of
// the binding and hand back the embedded definition, `undefined` for a `ref`.
// ---------------------------------------------------------------------------

import type { RoleBinding, RoleDefinition } from "@promin/agent";

/**
 * The role definition when it's knowable synchronously — i.e. an `inline`
 * binding. A `ref`'s definition lives in the RoleRegistry, so this returns
 * `undefined` for refs. See `inlineRoleDefinition` in
 * `packages/agent/src/lib/role/resolve-role.ts`.
 */
export function inlineRoleDefinition(
  binding: RoleBinding | null | undefined,
): RoleDefinition | undefined {
  if (!binding || typeof binding !== "object") return undefined;
  return "inline" in binding ? binding.inline : undefined;
}
