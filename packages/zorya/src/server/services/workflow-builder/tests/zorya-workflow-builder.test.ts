// ---------------------------------------------------------------------------
// ZoryaWorkflowBuilder — drafts, validation, publishing through the version
// registry and LocalWorkflows, delete guards, reload at start, and sample
// inputs. Runs go through `LocalWorkflows.trigger`, the same path the HTTP
// trigger route uses.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowSchemaStore,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  MapActivityCatalog,
  createWorkflowRunner,
  workflow,
  type ActivityDefinition,
  type WorkflowSchema,
  type WorkflowSchemaStore,
  type WorkflowState,
  type WorkflowStatus,
} from "@promin/workflow";
import { LocalWorkflows } from "../../workflows/local-workflows.ts";
import { UnknownWorkflowError } from "../../workflows/errors.ts";
import {
  BuilderWorkflowConflictError,
  BuilderWorkflowInUseError,
  BuilderWorkflowNameTakenError,
  BuilderWorkflowNotFoundError,
  InvalidBuilderWorkflowError,
  ZoryaWorkflowBuilder,
} from "../index.ts";

const ACTIVITIES: ActivityDefinition[] = [
  {
    id: "text.upper",
    title: "Uppercase",
    inputSchema: { type: "string" },
    factory: () => (ctx) => succeed(String(ctx.prev).toUpperCase()),
  },
  {
    id: "text.exclaim",
    title: "Exclaim",
    factory: () => (ctx) => succeed(`${String(ctx.prev)}!`),
  },
  {
    id: "text.wrap",
    title: "Wrap",
    configSchema: { type: "object", properties: { mode: { enum: ["brackets"] } } },
    factory: (config) => {
      if (config?.mode !== undefined && config.mode !== "brackets") {
        throw new Error(`text.wrap: unknown mode "${String(config.mode)}"`);
      }
      return (ctx) => succeed(`[${String(ctx.prev)}]`);
    },
  },
];

function harness(params: { store?: WorkflowSchemaStore; activities?: ActivityDefinition[] } = {}) {
  const storage = new InMemoryWorkflowStorage();
  const registry = new InMemoryWorkflowVersionRegistry();
  const clock = FakeWallClock.create(1_000);
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {
      "from-code": workflow<string>({ name: "from-code" })
        .step("a", ({ input }) => input)
        .build(),
    },
    versionRegistry: registry,
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
    orphanRecovery: false,
  });
  const store = params.store ?? new InMemoryWorkflowSchemaStore();
  const builder = new ZoryaWorkflowBuilder({
    workflows,
    activities: new MapActivityCatalog(params.activities ?? ACTIVITIES),
    store,
    clock,
  });
  return { storage, registry, clock, workflows, store, builder };
}

function schema(name: string, steps?: WorkflowSchema["steps"]): WorkflowSchema {
  return {
    version: 1,
    name,
    steps: steps ?? [{ type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" }],
  };
}

const exclaimed = (name: string): WorkflowSchema =>
  schema(name, [
    { type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" },
    { type: "step", name: "exclaim", dependsOn: ["upper"], activityRef: "text.exclaim" },
  ]);

/** Yield to the event loop until the run reaches `status` (bounded by iterations). */
async function waitForStatus(params: {
  storage: InMemoryWorkflowStorage;
  workflowId: string;
  status: WorkflowStatus;
}): Promise<WorkflowState> {
  for (let i = 0; i < 500; i++) {
    const state = await params.storage.loadWorkflow(params.workflowId);
    if (state?.status === params.status) return state;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error(`run ${params.workflowId} never reached ${params.status}`);
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected a rejection");
}

describe("ZoryaWorkflowBuilder — drafts", () => {
  it("creates a draft at revision 1 with nothing published", async () => {
    const { builder } = harness();
    const created = await builder.create({ schema: schema("greet") });
    expect(created).toMatchObject({
      name: "greet",
      revision: 1,
      createdAt: 1_000,
      versions: [],
      unpublishedChanges: true,
    });
    expect(await builder.get("greet")).toEqual(created);
    expect(await builder.list()).toEqual([
      { name: "greet", revision: 1, updatedAt: 1_000, stepCount: 1, unpublishedChanges: true },
    ]);
  });

  it("refuses a name another builder workflow, a code workflow or the registry holds", async () => {
    const { builder, registry } = harness();
    await builder.create({ schema: schema("greet") });
    await registry.register(
      workflow<string>({ name: "registered", version: "1" })
        .step("a", ({ input }) => input)
        .build(),
    );

    for (const [name, reason] of [
      ["greet", "exists"],
      ["from-code", "reserved"],
      ["registered", "reserved"],
    ] as const) {
      const err = await rejection(builder.create({ schema: schema(name) }));
      expect(err).toBeInstanceOf(BuilderWorkflowNameTakenError);
      expect((err as BuilderWorkflowNameTakenError).reason).toBe(reason);
    }
  });

  it("saves from the revision it read and refuses a stale editor", async () => {
    const { builder, clock } = harness();
    await builder.create({ schema: schema("greet") });
    clock.advance(500);
    const saved = await builder.saveDraft({
      name: "greet",
      schema: exclaimed("greet"),
      revision: 1,
    });
    expect(saved).toMatchObject({ revision: 2, updatedAt: 1_500, createdAt: 1_000 });

    const err = await rejection(
      builder.saveDraft({ name: "greet", schema: schema("greet"), revision: 1 }),
    );
    expect(err).toBeInstanceOf(BuilderWorkflowConflictError);
    expect((err as BuilderWorkflowConflictError).currentRevision).toBe(2);
    expect((await builder.get("greet"))!.schema.steps).toHaveLength(2);
  });

  it("lets only one of two concurrent saves from the same revision win", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    const results = await Promise.allSettled([
      builder.saveDraft({ name: "greet", schema: exclaimed("greet"), revision: 1 }),
      builder.saveDraft({ name: "greet", schema: schema("greet"), revision: 1 }),
    ]);
    expect(results.map((r) => r.status).sort()).toEqual(["fulfilled", "rejected"]);
  });

  it("refuses a save whose schema names another workflow (renames create a new one)", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    const err = await rejection(
      builder.saveDraft({ name: "greet", schema: schema("renamed"), revision: 1 }),
    );
    expect((err as InvalidBuilderWorkflowError).issues).toEqual([
      { path: "name", message: 'must be "greet"; save a renamed workflow as a new one' },
    ]);
  });

  it("refuses saving a workflow that does not exist", async () => {
    const { builder } = harness();
    expect(
      await rejection(builder.saveDraft({ name: "ghost", schema: schema("ghost"), revision: 1 })),
    ).toBeInstanceOf(BuilderWorkflowNotFoundError);
  });
});

describe("ZoryaWorkflowBuilder — validation", () => {
  const cases: Array<{ title: string; raw: unknown; issue: { path: string; message: RegExp } }> = [
    {
      title: "a structurally invalid schema",
      raw: { version: 1, name: "bad", steps: [] },
      issue: { path: "steps", message: /at least one step/ },
    },
    {
      title: "a dependency cycle",
      raw: schema("cyclic", [
        { type: "step", name: "a", dependsOn: ["b"], activityRef: "text.upper" },
        { type: "step", name: "b", dependsOn: ["a"], activityRef: "text.upper" },
      ]),
      issue: { path: "", message: /Dependency cycle between steps: "a", "b"/ },
    },
    {
      title: "a dangling dependency",
      raw: schema("dangling", [
        { type: "step", name: "a", dependsOn: ["ghost"], activityRef: "text.upper" },
      ]),
      issue: { path: "", message: /depends on "ghost" which does not exist/ },
    },
    {
      title: "an unknown activity",
      raw: schema("unknown", [{ type: "step", name: "a", dependsOn: [], activityRef: "nope" }]),
      issue: { path: "", message: /unknown activity "nope"/ },
    },
    {
      title: "config its activity rejects",
      raw: schema("bad-config", [
        { type: "step", name: "a", dependsOn: [], activityRef: "text.wrap", config: { mode: "x" } },
      ]),
      issue: { path: "", message: /unknown mode "x"/ },
    },
    {
      title: "a name that cannot be a URL segment",
      raw: schema("has space"),
      issue: { path: "name", message: /letters, digits/ },
    },
  ];

  for (const c of cases) {
    it(`rejects ${c.title} with a clear issue`, async () => {
      const { builder } = harness();
      const err = await rejection(builder.create({ schema: c.raw }));
      expect(err).toBeInstanceOf(InvalidBuilderWorkflowError);
      const issues = (err as InvalidBuilderWorkflowError).issues;
      expect(issues.some((i) => i.path === c.issue.path && c.issue.message.test(i.message))).toBe(
        true,
      );
      expect(builder.validate(c.raw)).toEqual([...issues]);
    });
  }

  it("caps the number of steps", async () => {
    const { workflows } = harness();
    const builder = new ZoryaWorkflowBuilder({
      workflows,
      activities: new MapActivityCatalog(ACTIVITIES),
      maxSteps: 2,
    });
    const steps = ["a", "b", "c"].map((name) => ({
      type: "step" as const,
      name,
      dependsOn: [],
      activityRef: "text.upper",
    }));
    expect(builder.validate(schema("big", steps))).toEqual([
      { path: "steps", message: "at most 2 steps" },
    ]);
  });

  it("accepts a valid schema", () => {
    expect(harness().builder.validate(exclaimed("ok"))).toEqual([]);
  });
});

describe("ZoryaWorkflowBuilder — publishing", () => {
  it("publishes v1, promotes it, and runs it through the trigger path", async () => {
    const { builder, workflows, registry, storage } = harness();
    await builder.create({ schema: exclaimed("greet") });

    const published = await builder.publish({ name: "greet" });
    expect(published).toMatchObject({ version: "1", created: true });
    expect(published.workflow).toMatchObject({
      activeVersion: "1",
      unpublishedChanges: false,
      versions: [{ version: "1", sequence: 1, publishedAt: 1_000, status: "active" }],
    });
    expect(await registry.versions("greet")).toEqual(["1"]);

    const { workflowId } = await workflows.trigger("greet", "hello");
    const state = await waitForStatus({ storage, workflowId, status: "completed" });
    expect(state.result).toBe("HELLO!");
    expect(state.version).toBe("1");
    expect(state.workflowType).toBe("builder");
  });

  it("publishes nothing new when the content did not change, canvas moves included", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    await builder.publish({ name: "greet" });
    await builder.saveDraft({
      name: "greet",
      schema: { ...schema("greet"), ui: { upper: { x: 300, y: 40 } } },
      revision: 1,
    });

    const again = await builder.publish({ name: "greet" });
    expect(again).toMatchObject({ version: "1", created: false });
    expect(again.workflow.unpublishedChanges).toBe(false);
  });

  it("publishes changed content as the next version; old runs keep their version", async () => {
    const { builder, workflows, storage } = harness();
    await builder.create({ schema: schema("greet") });
    await builder.publish({ name: "greet" });
    const v1Run = await workflows.trigger("greet", "a");
    await waitForStatus({ storage, workflowId: v1Run.workflowId, status: "completed" });

    await builder.saveDraft({ name: "greet", schema: exclaimed("greet"), revision: 1 });
    expect((await builder.get("greet"))!.unpublishedChanges).toBe(true);
    const v2 = await builder.publish({ name: "greet", revision: 2 });
    expect(v2).toMatchObject({ version: "2", created: true });
    expect(v2.workflow.activeVersion).toBe("2");
    expect(v2.workflow.versions.map((v) => [v.version, v.status])).toEqual([
      ["1", "inactive"],
      ["2", "active"],
    ]);

    const latest = await workflows.trigger("greet", "b");
    const latestState = await waitForStatus({
      storage,
      workflowId: latest.workflowId,
      status: "completed",
    });
    expect([latestState.version, latestState.result]).toEqual(["2", "B!"]);

    // An explicit older version still resolves through the registry.
    const pinned = await workflows.trigger("greet", "c", { version: "1" });
    const pinnedState = await waitForStatus({
      storage,
      workflowId: pinned.workflowId,
      status: "completed",
    });
    expect([pinnedState.version, pinnedState.result]).toEqual(["1", "C"]);
  });

  it("leaves the active version alone when asked not to promote", async () => {
    const { builder, workflows, storage } = harness();
    await builder.create({ schema: schema("greet") });
    await builder.publish({ name: "greet" });
    await builder.saveDraft({ name: "greet", schema: exclaimed("greet"), revision: 1 });
    const v2 = await builder.publish({ name: "greet", promote: false });
    expect(v2.workflow.activeVersion).toBe("1");

    const run = await workflows.trigger("greet", "x");
    const state = await waitForStatus({ storage, workflowId: run.workflowId, status: "completed" });
    expect([state.version, state.result]).toEqual(["1", "X"]);
  });

  it("refuses to publish from a stale revision", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    await builder.saveDraft({ name: "greet", schema: exclaimed("greet"), revision: 1 });
    expect(await rejection(builder.publish({ name: "greet", revision: 1 }))).toBeInstanceOf(
      BuilderWorkflowConflictError,
    );
  });

  it("publishes once when two publishes race", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    const [a, b] = await Promise.all([
      builder.publish({ name: "greet" }),
      builder.publish({ name: "greet" }),
    ]);
    expect([a.created, b.created].sort()).toEqual([false, true]);
    expect((await builder.get("greet"))!.versions).toHaveLength(1);
  });
});

describe("ZoryaWorkflowBuilder — delete", () => {
  const withApproval = (name: string): WorkflowSchema =>
    schema(name, [
      { type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" },
      { type: "approval", name: "review", dependsOn: ["upper"] },
    ]);

  it("refuses while a run is active, keeps serving it, and deletes once runs ended", async () => {
    const { builder, workflows, storage, registry } = harness();
    await builder.create({ schema: withApproval("review") });
    await builder.publish({ name: "review" });
    const run = await workflows.trigger("review", "x");
    await waitForStatus({ storage, workflowId: run.workflowId, status: "suspended" });

    const err = await rejection(builder.remove("review"));
    expect(err).toBeInstanceOf(BuilderWorkflowInUseError);
    expect((err as BuilderWorkflowInUseError).activeRuns).toBe(1);
    expect(Object.hasOwn(workflows.definitions, "review")).toBe(true);

    await storage.cancelWorkflow({ workflowId: run.workflowId });
    await builder.remove("review");
    expect(await builder.get("review")).toBeUndefined();
    expect(await registry.versions("review")).toEqual([]);
    expect(Object.hasOwn(workflows.definitions, "review")).toBe(false);
    expect(await rejection(workflows.trigger("review", "y"))).toBeInstanceOf(UnknownWorkflowError);
  });

  it("deletes a draft that was never published, and 404s an unknown one", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("draft-only") });
    await builder.remove("draft-only");
    expect(await builder.list()).toEqual([]);
    expect(await rejection(builder.remove("draft-only"))).toBeInstanceOf(
      BuilderWorkflowNotFoundError,
    );
  });

  it("frees the name: a deleted workflow can be created again", async () => {
    const { builder } = harness();
    await builder.create({ schema: schema("greet") });
    await builder.publish({ name: "greet" });
    await builder.remove("greet");
    const again = await builder.create({ schema: exclaimed("greet") });
    expect(again.versions).toEqual([]);
  });
});

describe("ZoryaWorkflowBuilder — start", () => {
  it("recompiles, registers, defines and promotes stored versions in a fresh process", async () => {
    const first = harness();
    await first.builder.create({ schema: schema("greet") });
    await first.builder.publish({ name: "greet" });
    await first.builder.saveDraft({ name: "greet", schema: exclaimed("greet"), revision: 1 });
    await first.builder.publish({ name: "greet" });

    const second = harness({ store: first.store });
    await second.builder.start();
    expect(await second.registry.versions("greet")).toEqual(["1", "2"]);
    expect((await second.registry.findActive("greet"))?.version).toBe("2");

    const run = await second.workflows.trigger("greet", "hi");
    const state = await waitForStatus({
      storage: second.storage,
      workflowId: run.workflowId,
      status: "completed",
    });
    expect(state.result).toBe("HI!");
  });

  it("skips versions that no longer compile and reports why", async () => {
    const first = harness();
    await first.builder.create({ schema: exclaimed("greet") });
    await first.builder.publish({ name: "greet" });

    const second = harness({
      store: first.store,
      activities: ACTIVITIES.filter((a) => a.id !== "text.exclaim"),
    });
    await second.builder.start();
    const loaded = await second.builder.get("greet");
    expect(loaded?.loadError).toMatch(/version 1: .*unknown activity "text.exclaim"/);
    expect(Object.hasOwn(second.workflows.definitions, "greet")).toBe(false);
    expect((await second.builder.list())[0]?.loadError).toBe(loaded?.loadError);
  });
});

describe("ZoryaWorkflowBuilder — sample input", () => {
  it("samples the declared input schema, else the root activity's input", async () => {
    const { builder } = harness();
    await builder.create({
      schema: {
        ...schema("declared"),
        inputSchema: {
          type: "object",
          properties: { text: { type: "string", default: "hi" }, urgent: { type: "boolean" } },
        },
      },
    });
    await builder.publish({ name: "declared" });
    await builder.create({ schema: schema("inferred") });
    await builder.publish({ name: "inferred" });
    await builder.create({ schema: schema("unpublished") });

    expect(builder.sampleInput("declared")).toEqual({ text: "hi", urgent: true });
    expect(builder.sampleInput("inferred")).toBe("hello workflow");
    expect(builder.sampleInput("unpublished")).toBeUndefined();
  });
});

describe("ZoryaWorkflowBuilder — configuration", () => {
  it("needs a LocalWorkflows layer with a version registry", () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    expect(
      () => new ZoryaWorkflowBuilder({ workflows, activities: new MapActivityCatalog(ACTIVITIES) }),
    ).toThrow(/needs a versionRegistry/);
  });
});
