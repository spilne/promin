// ---------------------------------------------------------------------------
// TaggedError constraint — the shape typed step errors must have
// ---------------------------------------------------------------------------

/** Constraint for discriminated-union error types. */
export type TaggedError = { readonly _tag: string };
