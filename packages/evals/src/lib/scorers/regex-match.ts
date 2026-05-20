// ---------------------------------------------------------------------------
// regexMatch — 1.0 when the stringified output matches a pattern.
// ---------------------------------------------------------------------------

import type { Scorer } from "../types.ts";
import { stringifyValue } from "./stringify.ts";

export interface RegexMatchConfig {
  readonly pattern: RegExp | string;
  /** Flags applied when `pattern` is a string. Ignored for a RegExp. */
  readonly flags?: string;
  /** Scorer id — default `"regexMatch"`. */
  readonly id?: string;
}

/** Build a scorer that tests the stringified output against `pattern`. */
export function regexMatch(config: RegexMatchConfig): Scorer {
  const id = config.id ?? "regexMatch";
  const regex =
    config.pattern instanceof RegExp ? config.pattern : new RegExp(config.pattern, config.flags);
  return {
    id,
    threshold: 1,
    async score({ output }) {
      // Reset lastIndex — a global-flag RegExp is stateful across calls.
      regex.lastIndex = 0;
      const value = regex.test(stringifyValue(output.output)) ? 1 : 0;
      return {
        scorerId: id,
        value,
        ...(value === 0 && { reason: `output did not match ${regex.toString()}` }),
      };
    },
  };
}
