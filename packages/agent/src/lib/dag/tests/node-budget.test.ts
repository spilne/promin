import { describe, expect, it } from "bun:test";

import { emptyDimension, splitShare, startShare, waitingWeight } from "../node-budget.ts";
import type { AgentNode, SubDagNode, TransformNode } from "../types.ts";

const agentNode: AgentNode = {
  id: "agent",
  kind: "agent",
  agentId: "worker",
  inputs: {},
};

const transformNode: TransformNode = {
  id: "transform",
  kind: "transform",
  inputs: {},
  ops: [{ op: "count" }],
};

const subDagNode: SubDagNode = {
  id: "sub",
  kind: "dag",
  dagId: "child",
  version: "v1",
  inputs: {},
};

describe("DAG node budget shares", () => {
  it("a fan-out node counts as its items", () => {
    const share = startShare({
      free: { maxLlmCalls: 19 },
      weight: 10,
      othersWaiting: 2,
    });

    expect(share.maxLlmCalls).toBe(10);
    expect(splitShare({ share, parts: 10 }).maxLlmCalls).toBe(1);
  });

  it("cost is divided exactly, counts are rounded down", () => {
    expect(
      startShare({
        free: { maxCostUsd: 1, maxLlmCalls: 5 },
        weight: 1,
        othersWaiting: 3,
      }),
    ).toEqual({ maxCostUsd: 0.25, maxLlmCalls: 1 });
  });

  it("names the dimension that has nothing left", () => {
    expect(emptyDimension({ maxLlmCalls: 0, maxTotalTokens: 500 })).toBe("maxLlmCalls");
    expect(emptyDimension({ maxTotalTokens: 500 })).toBeUndefined();
    expect(emptyDimension({})).toBeUndefined();
  });

  it("transform nodes wait with weight 0", () => {
    expect(waitingWeight(transformNode)).toBe(0);
    expect(waitingWeight(agentNode)).toBe(1);
    expect(waitingWeight(subDagNode)).toBe(1);
  });
});
