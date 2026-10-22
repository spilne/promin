// ---------------------------------------------------------------------------
// `.parallelSteps()` — fork the current head into named branches that run as
// distinct DAG steps (`<block>.<branch>`), joined into a keyed record by a
// synthetic `parallel` step named after the block.
// ---------------------------------------------------------------------------

import { succeed, type Eff, type ErrorsOf } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { WorkflowError } from "../durable-pipeline-error.ts";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  linearStepContext,
  type ParallelStepsOptions,
  type StepDefinition,
  type StepOptions,
} from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";

/** Extract the success type from a parallel branch function. */
export type BranchOutput<B> = B extends (ctx: any) => Eff<infer T, any> ? T : never;

/** Union of all branch error types — flows into the builder's typed Error channel. */
export type BranchError<Branches extends Record<string, unknown>> = {
  [K in keyof Branches]: Branches[K] extends (ctx: any) => Eff<any, infer S>
    ? ErrorsOf<S> extends infer E
      ? E extends TaggedError
        ? E
        : never
      : never
    : never;
}[keyof Branches];

/**
 * The branch steps followed by the join step. Scoped branch names may still
 * collide with existing steps; the builder rejects those on append.
 */
export function createParallelSteps(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly branches: Readonly<Record<string, (ctx: never) => unknown>>;
  readonly options: ParallelStepsOptions<Record<string, unknown>> | undefined;
  /** The workflow's default codec. */
  readonly defaultCodec: Codec<unknown>;
  /** Default step-cache namespace (the workflow name). */
  readonly cacheNamespace: string;
}): StepDefinition[] {
  const { name, dependsOn, branches, options } = params;
  const branchKeys = Object.keys(branches);
  if (branchKeys.length === 0) {
    throw new WorkflowError({
      workflowId: "",
      message: `.parallelSteps("${name}", ...) requires at least one branch`,
    });
  }
  const perBranch = (options?.branches ?? {}) as Record<string, StepOptions<unknown> | undefined>;
  for (const key of Object.keys(perBranch)) {
    if (!Object.hasOwn(branches, key)) {
      throw new WorkflowError({
        workflowId: "",
        message: `.parallelSteps("${name}", ...): options.branches has no branch "${key}"`,
      });
    }
  }

  // Block-level fields are defaults for every branch; a branch's own
  // options override them field by field.
  const blockDefaults: StepOptions<unknown> = {
    ...(options?.timeoutMs !== undefined && { timeoutMs: options.timeoutMs }),
    ...(options?.retry !== undefined && { retry: options.retry }),
    ...(options?.needs !== undefined && { needs: options.needs }),
    ...(options?.priority !== undefined && { priority: options.priority }),
    ...(options?.queue !== undefined && { queue: options.queue }),
    ...(options?.cache !== undefined && { cache: options.cache }),
  };

  // One StepDefinition per branch. Scoped names (block.branch) keep the
  // label record free for TypeScript while the physical queue rows stay
  // globally unique per workflow.
  const steps: StepDefinition[] = [];
  const scopedNames: string[] = [];
  const branchCodecs: Record<string, Codec<unknown>> = {};

  for (const key of branchKeys) {
    const scopedName = `${name}.${key}`;
    scopedNames.push(scopedName);

    const branchFn = branches[key] as (ctx: unknown) => unknown;
    const branchOptions: StepOptions<unknown> = { ...blockDefaults, ...perBranch[key] };
    const codec = (branchOptions.codec ?? params.defaultCodec) as Codec<unknown>;
    branchCodecs[key] = codec;

    steps.push({
      name: scopedName,
      dependsOn,
      kind: "normal",
      codec,
      ...toStepPolicy(branchOptions),
      execute: (exec) => {
        const ctx = linearStepContext({ dependsOn, exec });
        return withOptionalStepCache({
          cache: branchOptions.cache,
          ctx,
          runBody: () => asStepEff({ result: branchFn(ctx), stepName: scopedName }),
          stepName: scopedName,
          namespace: params.cacheNamespace,
          codec,
        });
      },
    });
  }

  // Synthetic join. Kind "parallel" marks it for visualization; its
  // body is a zero-effort assembler that reads the branch results and
  // shapes them into { [label]: result } using the original (unscoped)
  // keys. Its codec is `options.codec`; else, when a branch sets its own
  // codec, the branch codecs per key; else the workflow codec.
  const anyBranchCodec = branchKeys.some((key) => perBranch[key]?.codec !== undefined);
  const joinCodec = (options?.codec ??
    (anyBranchCodec ? recordCodec(branchCodecs) : params.defaultCodec)) as Codec<unknown>;
  steps.push({
    name,
    dependsOn: scopedNames,
    kind: "parallel",
    codec: joinCodec,
    execute: (exec) => {
      const out: Record<string, unknown> = {};
      for (let i = 0; i < branchKeys.length; i++) {
        out[branchKeys[i]!] = exec.results[scopedNames[i]!];
      }
      return succeed(out);
    },
  });
  return steps;
}

/**
 * Combine per-key codecs into a codec for the record of their values (the
 * `parallelSteps` join). Keys without a codec are encoded as-is.
 */
function recordCodec(codecs: Readonly<Record<string, Codec<unknown>>>): Codec<unknown> {
  const mapRecord = (value: unknown, pick: (codec: Codec<unknown>, v: unknown) => unknown) => {
    const out: Record<string, unknown> = {};
    for (const [key, v] of Object.entries(value as Record<string, unknown>)) {
      const codec = Object.hasOwn(codecs, key) ? codecs[key] : undefined;
      out[key] = codec ? pick(codec, v) : v;
    }
    return out;
  };
  return {
    encode: (value) => mapRecord(value, (codec, v) => codec.encode(v)),
    decode: (raw) => mapRecord(raw, (codec, v) => codec.decode(v)),
  };
}
