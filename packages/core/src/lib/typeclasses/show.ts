// ---------------------------------------------------------------------------
// Show<T> — Displayable typeclass
// "I can produce a human-readable representation of T"
// ---------------------------------------------------------------------------

export interface Show<T> {
  show(value: T): string;
}

/** Default: truncated JSON representation. */
export const JsonShow: Show<unknown> = {
  show: (v) => JSON.stringify(v)?.slice(0, 200) ?? "undefined",
};
