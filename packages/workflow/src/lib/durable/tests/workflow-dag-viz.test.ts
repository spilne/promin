import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { dagToDot, dagToMermaid, type WorkflowDAG } from "../workflow-dag-viz.ts";

const dag = (steps: WorkflowDAG["steps"]): WorkflowDAG => ({ name: "viz", steps });

describe("dagToMermaid", () => {
  it("keeps steps whose names reduce to the same id apart", () => {
    const mermaid = dagToMermaid(
      dag([
        { name: "a-b", dependsOn: [], kind: "normal" },
        { name: "a_b", dependsOn: [], kind: "normal" },
        { name: "c", dependsOn: ["a-b", "a_b"], kind: "normal" },
      ]),
    );
    expect(mermaid).toContain('a_b["a-b"]');
    expect(mermaid).toContain('a_b_2["a_b"]');
    expect(mermaid).toContain("a_b --> c");
    expect(mermaid).toContain("a_b_2 --> c");
  });

  it("keeps a parallelSteps branch apart from a step with the reduced name", () => {
    const wf = workflow<number>({ name: "p" })
      .step("enrich_user", () => succeed(1))
      .parallelSteps("enrich", { user: () => succeed(2) })
      .build();
    const mermaid = dagToMermaid(wf.dag);
    expect(mermaid).toContain('enrich_user["enrich_user"]');
    expect(mermaid).toContain('enrich_user_2["enrich.user"]');
    expect(mermaid).toContain("enrich_user_2 --> enrich");
  });

  it("escapes quotes in step and case labels", () => {
    const mermaid = dagToMermaid(
      dag([
        { name: 'say "hi"', dependsOn: [], kind: "normal" },
        {
          name: "route",
          dependsOn: ['say "hi"'],
          kind: "match",
          cases: ['big "one"'],
          hasDefault: true,
        },
      ]),
    );
    expect(mermaid).toContain('say__hi_["say #quot;hi#quot;"]');
    expect(mermaid).toContain('route_big__one_(["big #quot;one#quot;"])');
    expect(mermaid).toContain('route -->|"big #quot;one#quot;"| route_big__one_');
  });

  it("does not let a case node take a later step's id", () => {
    const mermaid = dagToMermaid(
      dag([
        { name: "route", dependsOn: [], kind: "match", cases: ["x"] },
        { name: "route_x", dependsOn: ["route"], kind: "normal" },
      ]),
    );
    expect(mermaid).toContain('route_x["route_x"]');
    expect(mermaid).toContain('route_x_2(["x"])');
    expect(mermaid).toContain("route --> route_x");
  });
});

describe("dagToDot", () => {
  it("escapes quotes and backslashes", () => {
    const dot = dagToDot({
      name: 'wf "q"',
      steps: [
        { name: 'a"b', dependsOn: [], kind: "normal" },
        { name: "c\\d", dependsOn: ['a"b'], kind: "normal" },
      ],
    });
    expect(dot).toContain('digraph "wf \\"q\\"" {');
    expect(dot).toContain('"a\\"b";');
    expect(dot).toContain('"a\\"b" -> "c\\\\d";');
  });

  it("keeps a case node apart from a step with the same name", () => {
    const dot = dagToDot(
      dag([
        { name: "route", dependsOn: [], kind: "match", cases: ["user"], hasDefault: false },
        { name: "route.user", dependsOn: ["route"], kind: "normal" },
      ]),
    );
    expect(dot).toContain('"route" -> "route.user~2" [label="user"];');
    expect(dot).toContain('"route.user~2" [shape=ellipse];');
    expect(dot).toContain('"route" -> "route.user";');
  });
});
