// Shared output stringification for the text-based scorers.

/** Render an output value as a string: pass strings through, JSON the rest. */
export function stringifyValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}
