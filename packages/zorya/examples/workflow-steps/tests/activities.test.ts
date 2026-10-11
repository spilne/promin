import { describe, expect, it } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  MapActivityCatalog,
  createWorkflowRunner,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaWorkflowBuilder } from "../../../src/index.ts";
import { demoActivities, type PostgresQueryClient } from "../index.ts";

describe("demo activity library", () => {
  it("composes every file's activities into one catalog", () => {
    const ids = new MapActivityCatalog(demoActivities()).list();
    for (const id of [
      "source.input",
      "json.pick",
      "text.uppercase",
      "text.template",
      "merge.join",
      "predicate.long",
      "system.now",
    ]) {
      expect(ids).toContain(id);
    }
  });

  it("offers dependency-backed activities only when the host provides the dependency", () => {
    expect(demoActivities().map((a) => a.id)).not.toContain("postgres.query");
    const postgres: PostgresQueryClient = { queryJson: async () => [{ ok: true }] };
    expect(demoActivities({ postgres }).map((a) => a.id)).toContain("postgres.query");
  });

  it("runs a builder workflow with a branch, a parallel fork and a join", async () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      versionRegistry: new InMemoryWorkflowVersionRegistry(),
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    const clock = FakeWallClock.create(Date.UTC(2026, 9, 11));
    const builder = new ZoryaWorkflowBuilder({
      workflows,
      activities: new MapActivityCatalog(demoActivities({ clock })),
    });
    await builder.create({
      schema: {
        version: 1,
        name: "shout-or-whisper",
        inputSchema: { type: "string", default: "a rather long message" },
        steps: [
          {
            type: "branch",
            name: "route",
            dependsOn: [],
            conditionRef: "predicate.long",
            ifTrue: { activityRef: "text.uppercase" },
            ifFalse: { activityRef: "text.case", config: { mode: "lower" } },
          },
          {
            type: "parallel",
            name: "fork",
            dependsOn: ["route"],
            branches: {
              framed: { activityRef: "text.template", config: { prefix: "<", suffix: ">" } },
              size: { activityRef: "text.length" },
            },
          },
          { type: "step", name: "at", dependsOn: [], activityRef: "system.now" },
          {
            type: "step",
            name: "join",
            dependsOn: ["fork", "at"],
            activityRef: "json.wrap",
            config: { key: "out" },
          },
        ],
      },
    });
    await builder.publish({ name: "shout-or-whisper" });

    const { workflowId } = await workflows.trigger("shout-or-whisper", "a rather long message");
    let result: unknown;
    for (let i = 0; i < 500 && result === undefined; i++) {
      const state = await storage.loadWorkflow(workflowId);
      if (state?.status === "completed") result = state.result;
      else await new Promise((r) => setTimeout(r, 0));
    }
    expect(result).toEqual({
      out: {
        fork: { framed: "<A RATHER LONG MESSAGE>", size: 21 },
        at: "2026-10-11T00:00:00.000Z",
      },
    });
  });
});
