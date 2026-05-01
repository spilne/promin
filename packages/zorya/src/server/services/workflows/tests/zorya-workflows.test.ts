// ---------------------------------------------------------------------------
// ZoryaWorkflows base / chain composition tests.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { Workflow, WorkflowStorage, TriggerOptions as _TO } from "@promin/workflow";
import { ZoryaWorkflows, UnknownWorkflowError } from "../index.ts";
import type { TriggerOptions, TriggerResult } from "../zorya-workflows.ts";

class FakeWorkflows extends ZoryaWorkflows {
  storage = {} as WorkflowStorage;
  override readonly definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  readonly handled: string[] = [];

  constructor(opts: { knownNames: string[]; fallback?: ZoryaWorkflows }) {
    const definitions = Object.fromEntries(
      opts.knownNames.map((n) => [n, { name: n } as unknown as Workflow<unknown, unknown>]),
    );
    const baseConfig = opts.fallback ? { definitions, fallback: opts.fallback } : { definitions };
    super(baseConfig);
    this.definitions = definitions;
  }

  protected canHandle(name: string): boolean {
    return name in this.definitions;
  }

  protected async dispatch(
    name: string,
    _input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    this.handled.push(name);
    return { workflowId: opts?.workflowId ?? `id-${name}` };
  }
}

describe("ZoryaWorkflows — chain composition", () => {
  it("dispatches at the first matching layer", async () => {
    const inner = new FakeWorkflows({ knownNames: ["b"] });
    const outer = new FakeWorkflows({ knownNames: ["a"], fallback: inner });

    const r = await outer.trigger("a", { x: 1 });
    expect(r.workflowId).toBe("id-a");
    expect(outer.handled).toEqual(["a"]);
    expect(inner.handled).toEqual([]);
  });

  it("walks past unmatched layers to the next fallback", async () => {
    const inner = new FakeWorkflows({ knownNames: ["b"] });
    const outer = new FakeWorkflows({ knownNames: ["a"], fallback: inner });

    const r = await outer.trigger("b", { x: 1 });
    expect(r.workflowId).toBe("id-b");
    expect(outer.handled).toEqual([]);
    expect(inner.handled).toEqual(["b"]);
  });

  it("throws UnknownWorkflowError when no layer accepts", async () => {
    const inner = new FakeWorkflows({ knownNames: ["b"] });
    const outer = new FakeWorkflows({ knownNames: ["a"], fallback: inner });

    await expect(outer.trigger("z", {})).rejects.toBeInstanceOf(UnknownWorkflowError);
  });

  it("3-deep chain dispatches at the third layer", async () => {
    const c = new FakeWorkflows({ knownNames: ["c"] });
    const b = new FakeWorkflows({ knownNames: ["b"], fallback: c });
    const a = new FakeWorkflows({ knownNames: ["a"], fallback: b });

    await a.trigger("c", {});
    expect(a.handled).toEqual([]);
    expect(b.handled).toEqual([]);
    expect(c.handled).toEqual(["c"]);
  });

  it("start() / stop() are idempotent", async () => {
    const layer = new FakeWorkflows({ knownNames: ["a"] });
    await layer.start();
    await layer.start(); // second call no-ops
    await layer.stop();
    await layer.stop(); // second call no-ops
  });

  it("start() cascades bottom-up; stop() top-down", async () => {
    const order: string[] = [];

    class Tracking extends FakeWorkflows {
      constructor(
        private readonly label: string,
        opts: { knownNames: string[]; fallback?: ZoryaWorkflows },
      ) {
        super(opts);
      }
      protected override async onStart(): Promise<void> {
        order.push(`start:${this.label}`);
      }
      protected override async onStop(): Promise<void> {
        order.push(`stop:${this.label}`);
      }
    }

    const inner = new Tracking("inner", { knownNames: ["b"] });
    const outer = new Tracking("outer", { knownNames: ["a"], fallback: inner });

    await outer.start();
    await outer.stop();

    expect(order).toEqual(["start:inner", "start:outer", "stop:outer", "stop:inner"]);
  });
});
