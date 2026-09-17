// ---------------------------------------------------------------------------
// exactMatch — 1.0 when the stringified output equals `expected`
// (case-insensitive, trimmed), 0.0 otherwise.
// ---------------------------------------------------------------------------

import type { Scorer } from "../types.ts";
import { stringifyValue } from "./stringify.ts";

export const exactMatch: Scorer = {
  id: "exactMatch",
  threshold: 1,
  async score({ expected, output }) {
    if (expected === undefined) {
      return { scorerId: "exactMatch", value: 0, reason: "case has no `expected` value" };
    }
    const got = stringifyValue(output.output).trim().toLowerCase();
    const want = stringifyValue(expected).trim().toLowerCase();
    const value = got === want ? 1 : 0;
    return {
      scorerId: "exactMatch",
      value,
      ...(value === 0 && {
        reason: `expected ${JSON.stringify(want)}, got ${JSON.stringify(got)}`,
      }),
    };
  },
};
