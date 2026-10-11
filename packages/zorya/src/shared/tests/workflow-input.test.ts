import { describe, expect, it } from "bun:test";
import type { JsonSchema, WorkflowSchema } from "@promin/workflow";
import { sampleFromJsonSchema, sampleWorkflowInput, workflowInputType } from "../workflow-input.ts";

const activities: Record<string, { inputSchema?: JsonSchema }> = {
  "text.upper": { inputSchema: { type: "string" } },
  "is.long": { inputSchema: { type: "string", default: "a long enough text" } },
  "source.literal": {},
};
const activity = (ref: string) => activities[ref];

function wf(steps: WorkflowSchema["steps"], inputSchema?: JsonSchema): WorkflowSchema {
  return { version: 1, name: "w", steps, ...(inputSchema && { inputSchema }) };
}

describe("workflowInputType", () => {
  it("prefers the declared inputSchema", () => {
    const schema = wf([{ type: "step", name: "a", dependsOn: [], activityRef: "text.upper" }], {
      type: "object",
      properties: { text: { type: "string" } },
    });
    expect(workflowInputType({ schema, activity })).toEqual({
      schema: { type: "object", properties: { text: { type: "string" } } },
      source: { kind: "workflow" },
    });
  });

  it("falls back to the first root step whose activity declares an input", () => {
    const schema = wf([
      { type: "step", name: "lit", dependsOn: [], activityRef: "source.literal" },
      { type: "step", name: "upper", dependsOn: ["lit"], activityRef: "text.upper" },
      { type: "step", name: "root2", dependsOn: [], activityRef: "text.upper" },
    ]);
    expect(workflowInputType({ schema, activity })?.source).toEqual({
      kind: "step",
      step: "root2",
      activityRef: "text.upper",
    });
  });

  it("reads a root branch's condition input", () => {
    const schema = wf([
      {
        type: "branch",
        name: "route",
        dependsOn: [],
        conditionRef: "is.long",
        ifTrue: { activityRef: "text.upper" },
        ifFalse: { activityRef: "source.literal" },
      },
    ]);
    expect(sampleWorkflowInput({ schema, activity })).toBe("a long enough text");
  });

  it("is undefined when nothing declares an input", () => {
    const schema = wf([{ type: "sleep", name: "s", dependsOn: [], ms: 10 }]);
    expect(workflowInputType({ schema, activity })).toBeUndefined();
    expect(sampleWorkflowInput({ schema, activity })).toBeUndefined();
  });
});

describe("sampleFromJsonSchema", () => {
  it("uses defaults, then enums, then type placeholders", () => {
    expect(
      sampleFromJsonSchema({
        type: "object",
        properties: {
          text: { type: "string", default: "hi" },
          mode: { type: "string", enum: ["fast", "slow"] },
          count: { type: "integer" },
          ratio: { type: "number" },
          urgent: { type: "boolean" },
          tags: { type: "array", items: { type: "string" } },
          anything: {},
        },
      }),
    ).toEqual({
      text: "hi",
      mode: "fast",
      count: 1,
      ratio: 1,
      urgent: true,
      tags: ["hello workflow"],
    });
  });

  it("copies defaults so callers cannot mutate the schema", () => {
    const schema: JsonSchema = { type: "object", default: { a: 1 } };
    const sample = sampleFromJsonSchema(schema) as { a: number };
    sample.a = 2;
    expect(schema.default).toEqual({ a: 1 });
  });

  it("is undefined without a schema and stops at deep nesting", () => {
    expect(sampleFromJsonSchema(undefined)).toBeUndefined();
    let deep: JsonSchema = { type: "string" };
    for (let i = 0; i < 20; i++) deep = { type: "array", items: deep };
    expect(JSON.stringify(sampleFromJsonSchema(deep))).not.toContain("hello");
  });
});
