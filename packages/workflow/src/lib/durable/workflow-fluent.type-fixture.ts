// ---------------------------------------------------------------------------
// Compile-time fixtures for fluent authoring:
//
//   1. Step bodies: an `Eff`, a Promise or a plain value; the output is
//      unwrapped and only an `Eff`'s typed failures join the error channel.
//   2. `.parallel()` (head and `dependsOn` forms), DAG `.branch()` and
//      `.sleep()`: `prev` is the first dependency's value.
//   3. `.approval()`: default payload type, addressable by name.
//   4. Fragments: `use()` adds exactly the fragment's named steps, and a
//      fragment that does not fit the workflow is rejected.
//   5. Bound workflows type their input and output.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import type { WorkflowTimeoutError } from "./durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { createWorkflowApp } from "./workflow-app.ts";
import { workflow, type WorkflowBuilder } from "./workflow-builder.ts";
import { step } from "./workflow-fragment.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const assertType = <T extends true>(_: T): void => {};

type StepsOf<B> = B extends WorkflowBuilder<any, infer S, any, any> ? S : never;
type CurrentOf<B> = B extends WorkflowBuilder<any, any, infer C, any> ? C : never;
type ErrorOf<B> = B extends WorkflowBuilder<any, any, any, infer E> ? E : never;

class Declined extends TaggedError("Declined")<{ readonly reason: string }>() {}

const base = workflow<{ id: string }>({ name: "fluent-fixture" });

// ---------------------------------------------------------------------------
// 1. Step bodies
// ---------------------------------------------------------------------------

const promiseStep = base.step("p", async ({ input }) => input.id.length);
assertType<Equal<CurrentOf<typeof promiseStep>, number>>(true);
assertType<Equal<ErrorOf<typeof promiseStep>, never>>(true);

const plainStep = base.step("v", ({ input }) => ({ id: input.id }));
assertType<Equal<CurrentOf<typeof plainStep>, { id: string }>>(true);

const effStep = base.step("e", ({ input }) =>
  input.id === "" ? fail(new Declined({ reason: "empty" })) : succeed(input.id),
);
assertType<Equal<CurrentOf<typeof effStep>, string>>(true);
assertType<Equal<ErrorOf<typeof effStep>, Declined>>(true);

// ---------------------------------------------------------------------------
// 2. parallel / DAG branch / DAG sleep
// ---------------------------------------------------------------------------

const forked = base
  .step("n", () => 1)
  .parallel("fork", {
    a: async ({ prev }) => prev + 1,
    b: ({ prev }) => String(prev),
    c: () => fail(new Declined({ reason: "c" })),
  });
assertType<Equal<CurrentOf<typeof forked>, { a: number; b: string; c: never }>>(true);
assertType<Equal<ErrorOf<typeof forked>, Declined>>(true);

const twoHeads = base.step("n", () => 1).step("s", { dependsOn: [] }, () => "s");

// The head is "s" (a string); the branches see "n" (a number) as `prev`.
const dagFork = twoHeads.parallel(
  "fork",
  { dependsOn: ["n", "s"] },
  { fixed: ({ prev }) => prev.toFixed(0), plain: async ({ prev }) => prev },
);
assertType<Equal<CurrentOf<typeof dagFork>, { fixed: string; plain: number }>>(true);

// @ts-expect-error — `dependsOn` names must be existing steps
twoHeads.parallel("fork", { dependsOn: ["missing"] }, { a: () => 1 });

const dagBranch = twoHeads.branch("route", {
  dependsOn: ["n", "s"],
  condition: (n) => n > 0,
  ifTrue: ({ prev }) => prev.toFixed(0),
  ifFalse: async () => "neg",
});
assertType<Equal<CurrentOf<typeof dagBranch>, string>>(true);

const dagSleep = twoHeads.sleep("pause", 10, { dependsOn: ["s"] });
assertType<Equal<CurrentOf<typeof dagSleep>, string>>(true);

// ---------------------------------------------------------------------------
// 3. approval
// ---------------------------------------------------------------------------

const approved = base.approval("manager");
assertType<Equal<CurrentOf<typeof approved>, { approved: boolean }>>(true);
assertType<Equal<ErrorOf<typeof approved>, WorkflowTimeoutError>>(true);
assertType<Equal<StepsOf<typeof approved>, Record<"manager", { approved: boolean }>>>(true);

// ---------------------------------------------------------------------------
// 4. fragments
// ---------------------------------------------------------------------------

const normalize = step("trim", ({ input }: { input: { name: string } }) =>
  input.name.trim(),
).andThen("upper", async ({ prev }) => prev.toUpperCase());

const used = workflow<{ name: string; age: number }>({ name: "use" }).use(normalize);
assertType<Equal<CurrentOf<typeof used>, string>>(true);
assertType<Equal<StepsOf<typeof used>, Record<"trim", string> & Record<"upper", string>>>(true);
used.step("after", { dependsOn: ["trim", "upper"] }, ({ deps }) => deps.trim + deps.upper);
// @ts-expect-error — the fragment adds only its own step names
used.step("after", { dependsOn: ["nope"] }, () => 1);

// @ts-expect-error — the workflow input lacks the fragment's `name`
workflow<{ id: string }>({ name: "misfit" }).use(normalize);

const needsNumber = step<unknown, number, never, number>("inc", ({ prev }) => prev + 1);
workflow<number>({ name: "fits" }).use(needsNumber);
const stringHead = workflow<number>({ name: "misfit-prev" }).step("s", () => "s");
// @ts-expect-error — the head's value (a string) is not the `prev` the fragment expects
stringHead.use(needsNumber);

// ---------------------------------------------------------------------------
// 5. Bound workflows
// ---------------------------------------------------------------------------

const bound = createWorkflowApp({ storage: new InMemoryWorkflowStorage() }).workflow(
  base.step("len", ({ input }) => input.id.length).build(),
);
export const _boundRun: Promise<number> = bound.run({ workflowId: "w", input: { id: "x" } });
// @ts-expect-error — input is typed by the workflow
bound.run({ workflowId: "w", input: { id: 1 } });
