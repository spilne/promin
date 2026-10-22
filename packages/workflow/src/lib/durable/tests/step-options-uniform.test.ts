// ---------------------------------------------------------------------------
// Step options apply uniformly: every step kind × every option its options
// type admits behaves the same way it does on `.step()`. The compile-time
// half (options a kind cannot honour are rejected) lives in
// `../step-options.type-fixture.ts`.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed, suspend, tryPromise } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { StepEff, StepOptions } from "../step-definition.ts";
import { workflow, type WorkflowBuilder } from "../workflow-builder.ts";
import type { Workflow } from "../workflow-types.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { MemoryCache } from "../../shared/cache-store.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

/**
 * Scripted step body. Each call is one attempt: the first `failTimes`
 * attempts fail with a typed `Boom`, later ones return `value`; `hang`
 * attempts never settle.
 */
class Script {
  calls = 0;
  constructor(private readonly opts: { failTimes?: number; hang?: boolean; value?: number } = {}) {}
  /** Synchronous attempt: throws `Boom` while failing. */
  next(): number {
    this.calls++;
    if (this.calls <= (this.opts.failTimes ?? 0)) {
      throw new Boom({ message: `attempt ${this.calls} failed` });
    }
    return this.opts.value ?? 42;
  }
  /** The attempt as a step `Eff`. */
  eff(): StepEff<number, Boom> {
    if (this.opts.hang) {
      return suspend(() => {
        this.calls++;
        return tryPromise(
          () => new Promise<number>(() => {}),
          (e) => e as Boom,
        );
      });
    }
    return suspend(() => {
      try {
        return succeed(this.next());
      } catch (e) {
        return fail(e as Boom);
      }
    });
  }
}

type AnyOptions = StepOptions<any>;
type AnyBuilder = WorkflowBuilder<number, any, any, any>;

interface KindCase {
  readonly kind: string;
  /** Options this kind's options type admits (and so must honour). */
  readonly supports: ReadonlySet<OptionName>;
  /** Physical step carrying the policy. */
  readonly policyStep: string;
  /** Wrap the body's value the way this kind shapes its result. */
  readonly shape: (v: unknown) => unknown;
  /** Append the step under test (named "k") to `wf`, driven by `script`. */
  readonly add: (params: {
    readonly wf: AnyBuilder;
    readonly script: Script;
    readonly options: AnyOptions;
    readonly uid: string;
  }) => AnyBuilder;
}

type OptionName =
  | "retry"
  | "timeoutMs"
  | "onFailure.skip"
  | "onFailure.fallback"
  | "compensate"
  | "skipWhen"
  | "needs"
  | "priority"
  | "queue"
  | "cache";

const ALL: ReadonlySet<OptionName> = new Set<OptionName>([
  "retry",
  "timeoutMs",
  "onFailure.skip",
  "onFailure.fallback",
  "compensate",
  "skipWhen",
  "needs",
  "priority",
  "queue",
  "cache",
]);
const without = (...names: OptionName[]) =>
  new Set<OptionName>([...ALL].filter((n) => !names.includes(n)));

const identity = (v: unknown) => v;

const KINDS: readonly KindCase[] = [
  {
    kind: "step",
    supports: ALL,
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options }) => wf.step("k", () => script.eff(), options),
  },
  {
    kind: "branch",
    supports: ALL,
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options }) =>
      wf.branch(
        "k",
        { condition: () => true, ifTrue: () => script.eff(), ifFalse: () => script.eff() },
        options,
      ),
  },
  {
    kind: "match",
    supports: ALL,
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options }) =>
      wf.match("k", { on: () => "a", cases: { a: () => script.eff() } }, options),
  },
  {
    kind: "mapOver",
    supports: ALL,
    policyStep: "k",
    shape: (v) => (v === undefined ? undefined : [v]),
    add: ({ wf, script, options }) =>
      wf
        .step("arr", () => succeed([1]))
        .mapOver("k", { array: "arr" }, () => script.eff(), options as never),
  },
  {
    kind: "parallelSteps (per branch)",
    supports: ALL,
    policyStep: "k.a",
    shape: (v) => ({ a: v }),
    add: ({ wf, script, options }) =>
      wf.parallelSteps("k", { a: () => script.eff() }, { branches: { a: options } }),
  },
  {
    kind: "journaled",
    supports: without("timeoutMs", "cache"),
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options }) =>
      wf.journaled(
        "k",
        function* (ctx) {
          // A journaled activity before the scripted body: a retry replays it.
          yield* ctx.activity("before", async () => 0);
          return script.next();
        },
        options as never,
      ),
  },
  {
    kind: "subworkflow",
    supports: without("timeoutMs", "cache"),
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options, uid }) => {
      const child = workflow<number>({ name: `child-${uid}` })
        .step("c", () => script.eff())
        .build();
      return wf.subworkflow(
        "k",
        child as Workflow<number, number>,
        { input: (p: unknown) => p as number, workflowId: () => `child-${uid}` },
        options as never,
      );
    },
  },
  {
    kind: "dowhile",
    supports: without("cache"),
    policyStep: "k",
    shape: identity,
    add: ({ wf, script, options }) =>
      wf.dowhile(
        "k",
        () => script.eff(),
        () => false,
        options as never,
      ),
  },
];

let seq = 0;
function setup() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return { storage, runner, uid: `u${++seq}` };
}

function base(uid: string): AnyBuilder {
  return workflow<number>({ name: `wf-${uid}` }) as AnyBuilder;
}

describe("step options apply to every step kind", () => {
  for (const k of KINDS) {
    describe(k.kind, () => {
      const when = (name: OptionName) => (k.supports.has(name) ? it : it.skip);

      when("retry")("retry re-runs the body on typed failures", async () => {
        const { runner, uid } = setup();
        const script = new Script({ failTimes: 2 });
        const wf = k
          .add({
            wf: base(uid),
            script,
            options: { retry: { maxRetries: 2, baseDelayMs: 1 } },
            uid,
          })
          .build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect(r.error).toBeNull();
        expect(r.data).toEqual(k.shape(42));
        expect(script.calls).toBe(3);
      });

      when("retry")("without retry one typed failure fails the run", async () => {
        const { runner, uid } = setup();
        const script = new Script({ failTimes: 1 });
        const wf = k.add({ wf: base(uid), script, options: {}, uid }).build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect(r.error).not.toBeNull();
        expect(script.calls).toBe(1);
      });

      when("timeoutMs")("timeoutMs fails a hanging attempt with StepTimeoutError", async () => {
        const { runner, uid } = setup();
        const script = new Script({ hang: true });
        const wf = k.add({ wf: base(uid), script, options: { timeoutMs: 20 }, uid }).build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect((r.error as { _tag?: string } | null)?._tag).toBe("StepTimeoutError");
      });

      when("onFailure.skip")("onFailure 'skip' completes the step with undefined", async () => {
        const { runner, uid } = setup();
        const script = new Script({ failTimes: 99 });
        const wf = k
          .add({ wf: base(uid), script, options: { onFailure: "skip" }, uid })
          .step("after", ({ prev }: { prev: unknown }) => succeed({ prev }))
          .build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect(r.error).toBeNull();
        expect(r.data).toEqual({
          prev: k.kind.startsWith("parallel") ? { a: undefined } : undefined,
        });
      });

      when("onFailure.fallback")("onFailure fallback supplies the step result", async () => {
        const { runner, uid } = setup();
        const script = new Script({ failTimes: 99 });
        const seen: unknown[] = [];
        const wf = k
          .add({
            wf: base(uid),
            script,
            options: {
              onFailure: {
                fallback: (e: unknown) => {
                  seen.push(e);
                  return k.kind === "mapOver" ? [7] : 7;
                },
              },
            },
            uid,
          })
          .build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect(r.error).toBeNull();
        expect(r.data).toEqual(k.shape(7));
        expect(seen).toHaveLength(1);
      });

      when("compensate")("compensate runs with the result when a later step fails", async () => {
        const { runner, uid } = setup();
        const script = new Script();
        const compensated: unknown[] = [];
        const wf = k
          .add({
            wf: base(uid),
            script,
            options: {
              compensate: ({ result }: { result: unknown }) => {
                compensated.push(result);
                return succeed(undefined);
              },
            },
            uid,
          })
          .step("later", () => fail(new Boom({ message: "later" })))
          .build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect((r.error as { _tag?: string } | null)?._tag).toBe("Boom");
        // A parallel branch compensates with its own value, not the record.
        expect(compensated).toEqual([k.kind.startsWith("parallel") ? 42 : k.shape(42)]);
      });

      when("skipWhen")("skipWhen skips the body and uses skipValue", async () => {
        const { runner, uid } = setup();
        const script = new Script();
        const skipValue = k.kind === "mapOver" ? [5] : 5;
        const wf = k
          .add({
            wf: base(uid),
            script,
            options: { skipWhen: () => true, skipValue: () => skipValue },
            uid,
          })
          .build();
        const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
        expect(r.error).toBeNull();
        expect(r.data).toEqual(k.shape(5));
        expect(script.calls).toBe(0);
      });

      it.each([
        ["needs", { needs: ["gpu"] }, (d: any) => expect(d.needs).toEqual(["gpu"])],
        ["priority", { priority: 9 }, (d: any) => expect(d.priority).toBe(9)],
        [
          "queue",
          { queue: { concurrencyLimit: 2 } },
          (d: any) => expect(d.queue).toEqual({ concurrencyLimit: 2 }),
        ],
      ] as const)("%s reaches the step definition", (name, options, check) => {
        if (!k.supports.has(name as OptionName)) return;
        const { uid } = setup();
        const wf = k.add({ wf: base(uid), script: new Script(), options, uid }).build();
        check(wf._definition.steps.find((s) => s.name === k.policyStep));
        if (name !== "queue") {
          check(wf.dag.steps.find((s) => s.name === k.policyStep));
        }
      });

      when("cache")("cache serves a second run without running the body", async () => {
        const { runner, uid } = setup();
        const script = new Script();
        const store = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
        const wf = k
          .add({
            wf: base(uid),
            script,
            options: { cache: { key: () => "same", ttlMs: 60_000, store } },
            uid,
          })
          .build();
        const r1 = await runner.runSafe({ workflow: wf, workflowId: `${uid}-1`, input: 1 });
        const r2 = await runner.runSafe({ workflow: wf, workflowId: `${uid}-2`, input: 1 });
        expect(r1.data).toEqual(k.shape(42));
        expect(r2.data).toEqual(k.shape(42));
        expect(script.calls).toBe(1);
      });
    });
  }
});

describe("ctx.attempt reflects the step attempt", () => {
  const cases: readonly [string, (wf: AnyBuilder, seen: number[], s: Script) => AnyBuilder][] = [
    [
      "branch",
      (wf, seen, s) =>
        wf.branch(
          "k",
          {
            condition: () => true,
            ifTrue: ({ attempt }: { attempt: number }) => (seen.push(attempt), s.eff()),
            ifFalse: () => s.eff(),
          },
          { retry: { maxRetries: 1, baseDelayMs: 1 } },
        ),
    ],
    [
      "match",
      (wf, seen, s) =>
        wf.match(
          "k",
          {
            on: () => "a",
            cases: { a: ({ attempt }: { attempt: number }) => (seen.push(attempt), s.eff()) },
          },
          { retry: { maxRetries: 1, baseDelayMs: 1 } },
        ),
    ],
    [
      "mapOver",
      (wf, seen, s) =>
        wf
          .step("arr", () => succeed([1]))
          .mapOver(
            "k",
            { array: "arr" },
            (_el: unknown, { attempt }: { attempt: number }) => (seen.push(attempt), s.eff()),
            { retry: { maxRetries: 1, baseDelayMs: 1 } },
          ),
    ],
  ];
  for (const [kind, add] of cases) {
    it(`${kind}: the retry sees attempt 2`, async () => {
      const { runner, uid } = setup();
      const seen: number[] = [];
      const wf = add(base(uid), seen, new Script({ failTimes: 1 })).build();
      const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
      expect(r.error).toBeNull();
      expect(seen).toEqual([1, 2]);
    });
  }
});

describe("mapOver element options", () => {
  it("element.retry retries only the failing element", async () => {
    const { runner, uid } = setup();
    const calls: Record<number, number> = {};
    const attempts: number[] = [];
    const wf = workflow<number>({ name: `m-${uid}` })
      .step("arr", () => succeed([0, 1, 2]))
      .mapOver(
        "k",
        { array: "arr" },
        (el, ctx) => {
          calls[el] = (calls[el] ?? 0) + 1;
          if (el === 1) attempts.push(ctx.attempt);
          return el === 1 && calls[el] === 1
            ? fail(new Boom({ message: "flaky" }))
            : succeed(el * 10);
        },
        { element: { retry: { maxRetries: 2, baseDelayMs: 1 } } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect(r.error).toBeNull();
    expect(r.data).toEqual([0, 10, 20]);
    expect(calls).toEqual({ 0: 1, 1: 2, 2: 1 });
    expect(attempts).toEqual([1, 2]);
  });

  it("element.timeoutMs fails a hanging element, naming its index", async () => {
    const { runner, storage, uid } = setup();
    const wf = workflow<number>({ name: `m-${uid}` })
      .step("arr", () => succeed([0, 1]))
      .mapOver(
        "k",
        { array: "arr" },
        (el) =>
          el === 1
            ? tryPromise(
                () => new Promise<number>(() => {}),
                (e) => e as Boom,
              )
            : succeed(el),
        { element: { timeoutMs: 20 } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    const err = r.error as { _tag?: string; stepName?: string; message?: string } | null;
    expect(err?._tag).toBe("StepTimeoutError");
    // Attributed to the map step (its failure row), the element named in the message.
    expect(err?.stepName).toBe("k");
    expect(err?.message).toContain('"k[1]"');
    expect((await storage.loadWorkflow(uid))?.steps["k"]?.status).toBe("failed");
  });

  it("element.codec encodes task rows and, lifted to arrays, the step result", async () => {
    const { runner, storage, uid } = setup();
    const tagged: Codec<{ n: number }> = {
      encode: (v) => `n=${v.n}`,
      decode: (raw) => ({ n: Number(String(raw).slice(2)) }),
    };
    const wf = workflow<number>({ name: `m-${uid}` })
      .step("arr", () => succeed([1, 2]))
      .mapOver("k", { array: "arr" }, (el) => succeed({ n: el }), { element: { codec: tagged } })
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect(r.data).toEqual([{ n: 1 }, { n: 2 }]);
    const state = await storage.loadWorkflow(uid);
    expect(state?.steps["k"]?.result).toEqual(["n=1", "n=2"]);
    expect(state?.steps["k"]?.tasks?.map((t) => t.result)).toEqual(["n=1", "n=2"]);
  });
});

describe("parallelSteps options", () => {
  it("block-level retry is the default for every branch; a branch can override it", async () => {
    const { runner, uid } = setup();
    const a = new Script({ failTimes: 1 });
    const b = new Script({ failTimes: 2, value: 7 });
    const wf = workflow<number>({ name: `p-${uid}` })
      .parallelSteps(
        "k",
        { a: () => a.eff(), b: () => b.eff() },
        {
          retry: { maxRetries: 1, baseDelayMs: 1 },
          branches: { b: { retry: { maxRetries: 2, baseDelayMs: 1 } } },
        },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect(r.error).toBeNull();
    expect(r.data).toEqual({ a: 42, b: 7 });
    expect([a.calls, b.calls]).toEqual([2, 3]);
  });

  it("a branch codec encodes that branch and its key of the joined record", async () => {
    const { runner, storage, uid } = setup();
    const upper: Codec<string> = {
      encode: (v) => v.toUpperCase(),
      decode: (raw) => String(raw).toLowerCase(),
    };
    const wf = workflow<number>({ name: `p-${uid}` })
      .parallelSteps(
        "k",
        { a: () => succeed("x"), b: () => succeed(1) },
        { branches: { a: { codec: upper } } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect(r.data).toEqual({ a: "x", b: 1 });
    const state = await storage.loadWorkflow(uid);
    expect(state?.steps["k.a"]?.result).toBe("X");
    expect(state?.steps["k"]?.result).toEqual({ a: "X", b: 1 });
  });

  it("rejects options for a branch that does not exist", () => {
    expect(() =>
      workflow<number>({ name: "p-bad" }).parallelSteps(
        "k",
        { a: () => succeed(1) },
        { branches: { nope: { retry: {} } } as never },
      ),
    ).toThrow(/options\.branches has no branch "nope"/);
  });
});

describe("policies never handle engine control flow", () => {
  it("onFailure 'skip' and retry on a journaled step leave a sleep suspended", async () => {
    const { runner, storage, uid } = setup();
    let bodyRuns = 0;
    const wf = workflow<number>({ name: `j-${uid}` })
      .journaled(
        "k",
        function* (ctx) {
          bodyRuns++;
          yield* ctx.sleep(60_000);
          return 1;
        },
        { onFailure: "skip", retry: { maxRetries: 3, baseDelayMs: 1 } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect((r.error as { _tag?: string } | null)?._tag).toBe("WorkflowSuspendedError");
    expect(bodyRuns).toBe(1);
    expect((await storage.loadWorkflow(uid))?.status).not.toBe("completed");
  });

  it("a child that suspends suspends the parent, untouched by retry / onFailure", async () => {
    const { runner, uid } = setup();
    const child = workflow<number>({ name: `c-${uid}` })
      .sleep("nap", 60_000)
      .build();
    const wf = workflow<number>({ name: `p-${uid}` })
      .subworkflow(
        "k",
        child,
        { input: (p) => p, workflowId: () => `c-${uid}` },
        { onFailure: "skip", retry: { maxRetries: 3, baseDelayMs: 1 } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect((r.error as { _tag?: string } | null)?._tag).toBe("WorkflowSuspendedError");
  });
});

describe("failure typing per kind", () => {
  it("a failed child is a typed StepError on the subworkflow step", async () => {
    const { runner, uid } = setup();
    const child = workflow<number>({ name: `c-${uid}` })
      .step("c", () => fail(new Boom({ message: "child broke" })))
      .build();
    const wf = workflow<number>({ name: `p-${uid}` })
      .subworkflow("k", child, { input: (p) => p, workflowId: () => `c-${uid}` })
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    const err = r.error as { _tag?: string; stepName?: string; message?: string } | null;
    expect(err?._tag).toBe("StepError");
    expect(err?.stepName).toBe("k");
    expect(err?.message).toContain("child broke");
  });

  it("MatchError is handled by step-level retry and onFailure", async () => {
    const { runner, uid } = setup();
    let selections = 0;
    const wf = workflow<string>({ name: `m-${uid}` })
      .match(
        "k",
        { on: () => (selections++ === 0 ? "missing" : "a"), cases: { a: () => succeed("A") } },
        { retry: { maxRetries: 1, baseDelayMs: 1 } },
      )
      .match(
        "k2",
        { on: () => "missing", cases: { a: () => succeed("A") } },
        { onFailure: { fallback: (e) => `fallback:${(e as { _tag: string })._tag}` } },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: "x" });
    expect(r.error).toBeNull();
    expect(r.data).toBe("fallback:MatchError");
    expect(selections).toBe(2);
  });

  it("a throw from a synchronous callback is a defect: no retry, no onFailure", async () => {
    const { runner, uid } = setup();
    let conditionCalls = 0;
    const wf = workflow<number>({ name: `b-${uid}` })
      .branch(
        "k",
        {
          condition: () => {
            conditionCalls++;
            throw new Error("bad condition");
          },
          ifTrue: () => succeed(1),
          ifFalse: () => succeed(2),
        },
        { retry: { maxRetries: 3, baseDelayMs: 1 }, onFailure: "skip" },
      )
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: uid, input: 1 });
    expect(r.error).not.toBeNull();
    expect(String((r.error as Error).message)).toContain("bad condition");
    expect(conditionCalls).toBe(1);
  });
});
