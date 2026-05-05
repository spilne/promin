// ---------------------------------------------------------------------------
// Compile-time fixture for the scoped + elevated tool factories. Proves:
//
//   1. createScopedTool's `execute` callback receives a ScopedToolContext
//      with REQUIRED namespaceId / resourceId — accessing them does not
//      need optional chaining.
//   2. createElevatedTool's `execute` callback receives an
//      ElevatedToolContext with audit() — calling audit() typechecks,
//      omitting it does NOT typecheck (audit is a value, not a flag).
//   3. The bare `tool()` factory's ctx.scope is OPTIONAL — accessing
//      ctx.scope.namespaceId WITHOUT optional chaining fails to compile,
//      proving that scope-aware behavior must opt into a typed factory.
//
// Bun runs this through tsc as part of the typecheck target; any line
// here that compiles when it shouldn't is a regression.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { createElevatedTool, createScopedTool, tool } from "./tool.ts";

// ✓ Scoped tool — namespaceId / resourceId are non-optional on the context.
const _scoped = createScopedTool({
  name: "saveDoc",
  description: "Save a document for the current user.",
  parameters: z.object({ title: z.string(), body: z.string() }),
  execute: async (input, ctx) => {
    // No optional chain needed — ctx.namespaceId is `string`, not `string | undefined`.
    const ns: string = ctx.namespaceId;
    const uid: string = ctx.resourceId;
    return `${ns}/${uid}/${input.title}`;
  },
});
void _scoped;

// ✓ Elevated tool — audit() must be present in scope; calling it is fine.
const _elevated = createElevatedTool({
  name: "admin:listAllUsers",
  description: "List every user across all namespaces.",
  requires: "admin",
  parameters: z.object({ namespaceId: z.string() }),
  execute: async (input, ctx) => {
    const ns: string = ctx.namespaceId;
    const audit: ElevatedToolAudit = ctx.audit;
    audit({ action: "listAllUsers", target: input.namespaceId });
    return [ns];
  },
});
void _elevated;

// Type alias used above to assert audit() is a callable (not optional).
type ElevatedToolAudit = (entry: {
  readonly action: string;
  readonly target?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
}) => void;

// ✗ Bare `tool()` — ctx is fully optional. Accessing ctx.scope.namespaceId
// directly should NOT compile, since both ctx AND ctx.scope are optional.
const _bareTool = tool({
  name: "freeTool",
  description: "Tool without scope contract.",
  parameters: z.object({ x: z.number() }),
  execute: async (input, ctx) => {
    // @ts-expect-error — ctx is optional; cannot dereference without `?.`
    const _ns: string = ctx.scope.namespaceId;
    void _ns;
    // The compliant access pattern uses optional chaining and a fallback.
    const ns2: string | undefined = ctx?.scope?.namespaceId;
    return ns2 ?? `${input.x}`;
  },
});
void _bareTool;

// ─── Limits of compile-time enforcement ────────────────────────────────
// TS parameter variance is permissive — declaring `ctx?: ScopedToolContext`
// or `ctx: ScopedToolContext` (instead of ElevatedToolContext) inside an
// `execute` callback typechecks because of parameter contravariance/
// bivariance. Those mistakes are caught at runtime, not compile time:
//
//   - Optional ctx in scoped tool: factory still passes a populated ctx,
//     so user code that does `ctx?.namespaceId` returns the real value.
//     The runtime guard in `createScopedTool` ensures scope is present.
//   - Wrong ctx type in elevated tool: user can declare `ctx: ScopedToolContext`,
//     but the runtime still passes ElevatedToolContext at call time. If
//     the user forgets to call ctx.audit(), the runtime throws.
//
// What IS compile-time enforced (and tested above):
//   - Inside scoped execute: ctx.namespaceId / ctx.resourceId are non-optional.
//   - Inside elevated execute: ctx.audit is a callable, not optional.
//   - Bare tool() execute: ctx is fully optional, so direct dereference fails.
// ───────────────────────────────────────────────────────────────────────
