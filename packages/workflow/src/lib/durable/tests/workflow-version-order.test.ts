import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../../../index.ts";
import { compareWorkflowVersions, latestWorkflowVersion } from "../workflow-version-order.ts";
import { workflowClosure } from "../workflow-closure.ts";

describe("compareWorkflowVersions", () => {
  it("orders numeric versions as numbers, not strings", () => {
    expect(compareWorkflowVersions("10", "9")).toBeGreaterThan(0);
    expect(["10", "9", "2", "1"].sort(compareWorkflowVersions)).toEqual(["1", "2", "9", "10"]);
  });

  it("orders dotted and v-prefixed versions part by part", () => {
    expect(compareWorkflowVersions("1.10.0", "1.9.3")).toBeGreaterThan(0);
    expect(compareWorkflowVersions("v2", "1.99")).toBeGreaterThan(0);
    expect(compareWorkflowVersions("1.2", "1.2.0")).toBe(0);
  });

  it("puts a pre-release before its release", () => {
    expect(compareWorkflowVersions("2.0.0-rc.1", "2.0.0")).toBeLessThan(0);
    expect(compareWorkflowVersions("2.0.0-rc.2", "2.0.0-rc.10")).toBeLessThan(0);
    expect(compareWorkflowVersions("2.0.0-rc.1", "1.9.9")).toBeGreaterThan(0);
  });

  it("latestWorkflowVersion picks the newest", () => {
    expect(latestWorkflowVersion(["9", "10", "3"])).toBe("10");
    expect(latestWorkflowVersion([])).toBeUndefined();
  });
});

describe("workflowClosure", () => {
  it("collects previous versions and subworkflow children transitively, once each", () => {
    const leaf = workflow<string>({ name: "leaf" })
      .step("x", () => succeed(1))
      .build();
    const mid = workflow<string>({ name: "mid" })
      .subworkflow("leaf", leaf, { input: () => "a", workflowId: () => "leaf-1" })
      .build();
    const v1 = workflow<string>({ name: "top", version: "1" })
      .step("x", () => succeed(1))
      .build();
    const top = workflow<string>({ name: "top", version: "2", previousVersions: [v1] })
      .subworkflow("mid", mid, { input: () => "a", workflowId: () => "mid-1" })
      .subworkflow("leaf-again", leaf, { input: () => "a", workflowId: () => "leaf-2" })
      .build();

    const names = workflowClosure([top, leaf]).map((w) => `${w.name}@${w.version ?? ""}`);
    expect(names).toEqual(["top@2", "leaf@", "top@1", "mid@"]);
  });
});
