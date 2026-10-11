import { describe, expect, it } from "bun:test";
import type { ActivityDescriptor, WorkflowSchema } from "@promin/workflow";
import {
  canSave,
  schemaTypeLabel,
  typesCompatible,
  validateDraft,
} from "../workflow-builder-validation.ts";

const activities = new Map<string, ActivityDescriptor>(
  [
    { id: "text.upper", title: "Upper", inputSchema: { type: "string" } },
    { id: "is.long", title: "Is long", outputSchema: { type: "boolean" } },
  ].map((a) => [a.id, a]),
);

function check(schema: Partial<WorkflowSchema>) {
  return validateDraft({
    schema: { version: 1, name: "ok", steps: [], ...schema } as WorkflowSchema,
    activities,
  });
}

describe("validateDraft", () => {
  it("accepts a well-formed draft", () => {
    const issues = check({
      steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "text.upper" }],
    });
    expect(issues).toEqual([]);
    expect(canSave(issues)).toBe(true);
  });

  it("flags names, duplicates, unknown activities, missing deps and cycles on their steps", () => {
    const issues = check({
      name: "bad name",
      steps: [
        { type: "step", name: "a", dependsOn: ["b"], activityRef: "nope" },
        { type: "step", name: "b", dependsOn: ["a"], activityRef: "text.upper" },
        { type: "step", name: "c", dependsOn: ["ghost"], activityRef: "text.upper" },
        { type: "step", name: "c", dependsOn: [], activityRef: "text.upper" },
        { type: "map", name: "m", arrayFrom: "", activityRef: "text.upper" },
        { type: "signal", name: "s", dependsOn: [], signalName: " " },
        { type: "parallel", name: "p", dependsOn: [], branches: {} },
      ],
    });
    const byStep = (step: string) => issues.filter((i) => i.step === step).map((i) => i.message);
    expect(issues.some((i) => !i.step && i.message.startsWith("Workflow name"))).toBe(true);
    expect(byStep("a")).toEqual(['Unknown activity "nope".', "Part of a dependency cycle."]);
    expect(byStep("b")).toEqual(["Part of a dependency cycle."]);
    expect(byStep("c")).toContain('Two steps are named "c".');
    expect(byStep("c")).toContain('Depends on "ghost", which does not exist.');
    expect(byStep("m")).toContain("Connect the step whose array output it maps over.");
    expect(byStep("s")).toEqual(["Name the signal it waits for."]);
    expect(byStep("p")).toEqual(["Add at least one branch."]);
    expect(canSave(issues)).toBe(false);
  });

  it("warns, without blocking a save, about mismatched input types and non-boolean conditions", () => {
    const issues = check({
      inputSchema: { type: "object", properties: { text: { type: "string" } } },
      steps: [
        { type: "step", name: "a", dependsOn: [], activityRef: "text.upper" },
        {
          type: "branch",
          name: "route",
          dependsOn: ["a"],
          conditionRef: "text.upper",
          ifTrue: { activityRef: "text.upper" },
          ifFalse: { activityRef: "text.upper" },
        },
      ],
    });
    expect(issues).toEqual([
      {
        severity: "warning",
        step: "route",
        message: 'Condition "text.upper" does not declare a boolean output.',
      },
      {
        severity: "warning",
        input: true,
        step: "a",
        message: 'The workflow input is object { text } but "a" reads string.',
      },
    ]);
    expect(canSave(issues)).toBe(true);
  });
});

describe("schema helpers", () => {
  it("labels schema types", () => {
    expect(schemaTypeLabel(undefined)).toBe("any value");
    expect(schemaTypeLabel({ type: "string" })).toBe("string");
    expect(schemaTypeLabel({ type: "string", enum: ["a", "b"] })).toBe('one of "a", "b"');
    expect(schemaTypeLabel({ type: "array", items: { type: "number" } })).toBe("array of number");
    expect(schemaTypeLabel({ type: "object" })).toBe("object");
  });

  it("treats untyped schemas and integer-for-number as compatible", () => {
    expect(typesCompatible({ actual: {}, expected: { type: "string" } })).toBe(true);
    expect(typesCompatible({ actual: { type: "integer" }, expected: { type: "number" } })).toBe(
      true,
    );
    expect(typesCompatible({ actual: { type: "string" }, expected: { type: "number" } })).toBe(
      false,
    );
  });
});
