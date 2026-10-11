import { describe, expect, it } from "bun:test";
import type { ActivityDescriptor, WorkflowSchema } from "@promin/workflow";
import {
  DEFAULT_INPUT_SCHEMA_TEMPLATE,
  configFields,
  describeWorkflowInput,
  inputSchemaTemplate,
  parseJsonInput,
  suggestedTestInput,
} from "../workflow-builder-input.ts";

const activities = new Map<string, ActivityDescriptor>(
  [
    { id: "text.upper", title: "Upper", inputSchema: { type: "string" } },
    { id: "source.literal", title: "Literal" },
  ].map((a) => [a.id, a]),
);

const rootUpper: WorkflowSchema = {
  version: 1,
  name: "w",
  steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" }],
};

const rootLiteral: WorkflowSchema = {
  ...rootUpper,
  steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "source.literal" }],
};

describe("input node", () => {
  it("describes the declared input, else what the root step reads", () => {
    expect(describeWorkflowInput({ schema: rootUpper, activities })).toBe("string · read by upper");
    expect(
      describeWorkflowInput({
        schema: { ...rootUpper, inputSchema: { type: "object", properties: { id: {} } } },
        activities,
      }),
    ).toBe("object { id }");
    expect(
      describeWorkflowInput({
        schema: rootLiteral,
        activities,
      }),
    ).toBe("any value");
  });

  it("templates the input schema from the root activity, ignoring a declared one", () => {
    expect(
      inputSchemaTemplate({
        schema: { ...rootUpper, inputSchema: { type: "number" } },
        activities,
      }),
    ).toEqual({ type: "string" });
    expect(
      inputSchemaTemplate({
        schema: rootLiteral,
        activities,
      }),
    ).toEqual(DEFAULT_INPUT_SCHEMA_TEMPLATE);
  });

  it("suggests a test input from the input type, else an empty object", () => {
    expect(suggestedTestInput({ schema: rootUpper, activities })).toBe("hello workflow");
    expect(
      suggestedTestInput({
        schema: rootLiteral,
        activities,
      }),
    ).toEqual({});
  });
});

describe("configFields", () => {
  it("maps scalar properties to controls and skips structured ones", () => {
    expect(
      configFields({
        type: "object",
        required: ["mode"],
        properties: {
          mode: { type: "string", enum: ["a", "b"] },
          loud: { type: "boolean" },
          limit: { type: "integer" },
          prefix: { type: "string" },
          nested: { type: "object" },
        },
      }).map((f) => [f.name, f.control, f.required]),
    ).toEqual([
      ["mode", "select", true],
      ["loud", "toggle", false],
      ["limit", "number", false],
      ["prefix", "text", false],
    ]);
    expect(configFields(undefined)).toEqual([]);
  });
});

describe("parseJsonInput", () => {
  it("parses JSON, treats blank as undefined, and explains errors", () => {
    expect(parseJsonInput('{"a":1}')).toEqual({ ok: true, value: { a: 1 } });
    expect(parseJsonInput("  ")).toEqual({ ok: true, value: undefined });
    expect(parseJsonInput("{").ok).toBe(false);
  });
});
