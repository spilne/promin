// ---------------------------------------------------------------------------
// defineSignal — first-class signal-type artifact.
//
// Today's `ctx.signal<T>(name)` carries a TS generic that evaporates at
// compile time; the only runtime state is `signalName: string` + opaque
// `payload: unknown`. `defineSignal` introduces an addressable artifact
// that ships through the runtime — defined once, imported wherever the
// signal is suspended on / delivered to / inspected. Mirrors how tools
// work in this codebase (`tool({name, parameters, execute})`).
//
// ```ts
// const ReviewSignal = defineSignal({
//   name: "review",
//   schema: s.object({
//     approved: s.boolean().describe("Approve the request"),
//     reason:   s.string().optional(),
//   }),
// });
//
// const result = yield* ctx.validatedSignal(ReviewSignal);
// // result: { approved: boolean; reason?: string }
// ```
//
// The schema gets snapshotted to the pending journal entry at suspend
// time, so the server can validate any future delivery against the shape
// the workflow actually waited on — even if the SignalType definition
// later evolves.
// ---------------------------------------------------------------------------

import { type Infer, type Schema, s } from "../schema/builder.ts";

export interface SignalType<T = unknown> {
  /** Wire-format signal name. Matches what `ctx.signal(name)` would use. */
  readonly name: string;
  /** Validated shape of the delivered payload. */
  readonly schema: Schema<T>;
  /**
   * Optional version tag for evolving the schema later without breaking
   * pending suspensions (the journal snapshots schema at suspend time,
   * so this is purely informational for operators).
   */
  readonly version?: string;
  /** Phantom — exposes the parsed-payload type via `Infer<typeof Sig>`. */
  readonly _payload?: T;
}

/** Extract the parsed payload type from a `SignalType<T>`. */
export type SignalPayload<S> = S extends SignalType<infer T> ? T : never;

export function defineSignal<S extends Schema<unknown>>(params: {
  readonly name: string;
  readonly schema: S;
  readonly version?: string;
}): SignalType<Infer<S>> {
  if (!params.name || typeof params.name !== "string") {
    throw new TypeError("defineSignal: name must be a non-empty string");
  }
  return {
    name: params.name,
    schema: params.schema as Schema<Infer<S>>,
    ...(params.version !== undefined && { version: params.version }),
  };
}

// ---------------------------------------------------------------------------
// Canonical ApprovalSignal — the shape `ctx.approval(id)` waits on.
//
// `approve:<id>` name prefix preserved for backward compatibility with the
// existing convention agentLoop already uses (so SignalScanner / dashboard
// filters / external delivery callers don't have to change). Authors who
// want a custom approval shape can `defineSignal` their own; this one is
// just the preset.
//
// `metadata` is an open `Record<string, unknown>` slot so domain callers
// (cost estimates, diff summaries, links) can stash extra context without
// extending the canonical shape.
// ---------------------------------------------------------------------------

export const APPROVAL_NAME_PREFIX = "approve:";

export const ApprovalSchema = s.object({
  approved: s.boolean({ description: "Approve the request" }),
  by: s.string({ description: "Who delivered the decision" }).optional(),
  reason: s.string({ description: "Free-form rationale" }).optional(),
  // Open slot for caller-supplied context (cost estimates, diff summaries,
  // links). `s.unknown()` accepts any JSON value — the canonical preset
  // shouldn't dictate metadata shape.
  metadata: s.unknown({ description: "Caller-supplied context" }).optional(),
});

export type ApprovalDecision = Infer<typeof ApprovalSchema>;

/**
 * Build a SignalType for an approval keyed by `<id>`. The wire-format
 * name is `approve:<id>` — same convention agentLoop and SignalScanner
 * already use, so this drops in transparently.
 */
export function approvalSignal(id: string): SignalType<ApprovalDecision> {
  return defineSignal({
    name: `${APPROVAL_NAME_PREFIX}${id}`,
    schema: ApprovalSchema,
  });
}
