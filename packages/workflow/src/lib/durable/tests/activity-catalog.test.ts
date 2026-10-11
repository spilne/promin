import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { MapActivityCatalog } from "../activity-catalog.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { compileWorkflow } from "../workflow-compiler.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("MapActivityCatalog", () => {
  const catalog = new MapActivityCatalog([
    {
      id: "text.wrap",
      title: "Wrap",
      category: "Text",
      inputSchema: { type: "string" },
      outputSchema: { type: "string" },
      configSchema: {
        type: "object",
        properties: { prefix: { type: "string" }, suffix: { type: "string" } },
      },
      defaultConfig: { prefix: "[", suffix: "]" },
      factory: (config) => (ctx) =>
        succeed(`${String(config?.prefix)}${String(ctx.prev)}${String(config?.suffix)}`),
    },
    {
      id: "a.first",
      title: "First",
      category: "A",
      factory: () => (ctx) => succeed(ctx.prev),
    },
  ]);

  it("describes activities without their factories, ordered by category then id", () => {
    expect(catalog.describe()).toEqual([
      { id: "a.first", title: "First", category: "A" },
      {
        id: "text.wrap",
        title: "Wrap",
        category: "Text",
        inputSchema: { type: "string" },
        outputSchema: { type: "string" },
        configSchema: {
          type: "object",
          properties: { prefix: { type: "string" }, suffix: { type: "string" } },
        },
        defaultConfig: { prefix: "[", suffix: "]" },
      },
    ]);
    expect(catalog.descriptor("a.first")).toEqual({ id: "a.first", title: "First", category: "A" });
    expect(catalog.descriptor("missing")).toBeUndefined();
  });

  it("resolves for compileWorkflow, overlaying step config on the defaults", async () => {
    const wf = compileWorkflow({
      schema: {
        version: 1,
        name: "catalog-wf",
        steps: [
          {
            type: "step",
            name: "wrap",
            dependsOn: [],
            activityRef: "text.wrap",
            config: { suffix: ">" },
          },
        ],
      },
      registry: catalog,
    });
    const result = await createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
      workflow: wf,
      workflowId: "catalog-1",
      input: "x",
    });
    expect(result).toBe("[x>");
  });

  it("answers has/list like a registry and names the available ids on a miss", () => {
    expect(catalog.has("text.wrap")).toBe(true);
    expect(catalog.list()).toEqual(["a.first", "text.wrap"]);
    expect(() => catalog.resolve({ ref: "nope" })).toThrow(/Available: a.first, text.wrap/);
  });

  it("rejects an activity without an id or title", () => {
    const empty = new MapActivityCatalog();
    expect(() => empty.register({ id: " ", title: "x", factory: () => () => succeed(1) })).toThrow(
      /needs an id/,
    );
    expect(() => empty.register({ id: "x", title: "", factory: () => () => succeed(1) })).toThrow(
      /needs a title/,
    );
  });

  it("keeps its own copy of the metadata", () => {
    const defaults = { n: 1 };
    const own = new MapActivityCatalog([
      { id: "x", title: "X", defaultConfig: defaults, factory: () => () => succeed(1) },
    ]);
    defaults.n = 2;
    expect(own.descriptor("x")?.defaultConfig).toEqual({ n: 1 });
  });
});
