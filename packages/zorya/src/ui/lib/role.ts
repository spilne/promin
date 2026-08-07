// ---------------------------------------------------------------------------
// role.ts — UI-local mirror of `@promin/agent`'s `inlineRoleDefinition`.
//
// The UI must never import `@promin/agent` as a *value*: that barrel drags
// the whole server-side module graph (down to `cross-spawn`/`child_process`)
// into the browser bundle, which Bun rejects with "Browser build cannot
// require() Node.js builtin". Role/agent *types* still flow into the UI via
// type-only imports from the server routes (erased at build time); only the
// runtime helper needs a browser-safe stand-in.
//
// The body is identical to the source helper — narrow on the `inline` arm of
// the binding and hand back the embedded definition, `undefined` for a `ref`.
// It's generic over the inline definition so the caller keeps the precise
// `RoleDefinition` type inferred from its own (type-only) binding shape.
// ---------------------------------------------------------------------------

/**
 * The role definition when it's knowable synchronously — i.e. an `inline`
 * binding. A `ref`'s definition lives in the RoleRegistry, so this returns
 * `undefined` for refs. See `inlineRoleDefinition` in
 * `packages/agent/src/lib/role/resolve-role.ts`.
 */
export function inlineRoleDefinition<D>(
  binding: { readonly inline: D } | { readonly ref: unknown },
): D | undefined {
  return "inline" in binding ? binding.inline : undefined;
}
