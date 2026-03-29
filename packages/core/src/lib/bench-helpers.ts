// ---------------------------------------------------------------------------
// Shared benchmark helpers — reused across bench files
// ---------------------------------------------------------------------------

/** Create an array of sequential numbers [0, 1, ..., n-1]. */
export function makeArray(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

/** Standard map function used across benchmarks. */
export const mapFn = (x: number) => x * 2 + 1;

/** Standard filter function used across benchmarks (drops multiples of 3). */
export const filterFn = (x: number) => x % 3 !== 0;
