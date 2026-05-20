// ---------------------------------------------------------------------------
// jsonReporter — writes the full run summary as JSON to a file.
// ---------------------------------------------------------------------------

import type { EvalReporter } from "../types.ts";

/** A reporter that writes the completed run summary to `path` as JSON. */
export function jsonReporter(path: string): EvalReporter {
  return {
    async onComplete(summary) {
      await Bun.write(path, `${JSON.stringify(summary, null, 2)}\n`);
    },
  };
}
