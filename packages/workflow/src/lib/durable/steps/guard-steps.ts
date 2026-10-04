// ---------------------------------------------------------------------------
// `.guard()` and `.tripwire()` — synchronous predicates over `prev` that
// pass it through, or stop the run: a guard fails it, a tripwire ends it
// early with a structured reason.
// ---------------------------------------------------------------------------

import { fail, succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { GuardError } from "../durable-pipeline-error.ts";
import { readPrev, type StepDefinition } from "../step-definition.ts";

export function createGuardStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly predicate: (prev: never) => boolean;
  readonly failureMessage: string | undefined;
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { name, dependsOn } = params;
  const predicate = params.predicate as (prev: unknown) => boolean;
  return {
    name,
    dependsOn,
    kind: "guard",
    codec: params.codec,
    execute: (exec) => {
      const prev = readPrev({ dependsOn, results: exec.results, input: exec.input });
      if (predicate(prev)) return succeed(prev);
      return fail(
        new GuardError({
          workflowId: exec.workflowId,
          stepName: name,
          message: params.failureMessage ?? `Guard "${name}" failed`,
        }),
      );
    },
  };
}

export function createTripwireStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly when: (prev: never) => boolean;
  readonly reason: (prev: never) => unknown;
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { name, dependsOn } = params;
  const when = params.when as (prev: unknown) => boolean;
  const reasonOf = params.reason as (prev: unknown) => unknown;
  return {
    name,
    dependsOn,
    kind: "tripwire",
    codec: params.codec,
    execute: (exec) => {
      const prev = readPrev({ dependsOn, results: exec.results, input: exec.input });
      if (when(prev)) {
        const reason = reasonOf(prev);
        // Signal the runner: workflow should terminate with tripwire status.
        // Runner reads this off `metadataRef.current` after execute returns.
        exec.metadataRef.current = { tripwireFired: true, reason };
        return succeed(reason);
      }
      return succeed(prev);
    },
  };
}
