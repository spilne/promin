import { describe, expect, it } from "bun:test";
import { computeReadySet, createReadyTracker, type DagNode } from "../workflow-dag.ts";

const node = (name: string, ...dependsOn: string[]): DagNode => ({ name, dependsOn });

/** Deterministic PRNG (mulberry32) so the random DAGs are reproducible. */
function prng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A random DAG over `n` nodes; each node depends on up to 3 earlier ones. */
function randomDag(params: { n: number; seed: number }): DagNode[] {
  const rand = prng(params.seed);
  const nodes: DagNode[] = [];
  for (let i = 0; i < params.n; i++) {
    const deps = new Set<string>();
    const count = i === 0 ? 0 : Math.floor(rand() * 4);
    for (let k = 0; k < count; k++) deps.add(`n${Math.floor(rand() * i)}`);
    nodes.push(node(`n${i}`, ...deps));
  }
  // Shuffle the definition order so it differs from any topological order.
  for (let i = nodes.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [nodes[i], nodes[j]] = [nodes[j]!, nodes[i]!];
  }
  return nodes;
}

describe("createReadyTracker", () => {
  it("starts with the dependency-free nodes, in definition order", () => {
    const tracker = createReadyTracker({
      nodes: [node("c", "a"), node("b"), node("a"), node("d", "b")],
    });
    expect(tracker.ready()).toEqual(["b", "a"]);
    expect(tracker.completedCount).toBe(0);
  });

  it("orders newly ready nodes by definition, not by completion", () => {
    const tracker = createReadyTracker({
      nodes: [node("root"), node("x", "root", "y0"), node("y0", "root"), node("z", "root")],
    });
    tracker.markCompleted("root");
    expect(tracker.ready()).toEqual(["y0", "z"]);
    tracker.markCompleted("z");
    tracker.markCompleted("y0");
    expect(tracker.ready()).toEqual(["x"]);
  });

  it("releases a join only once every dependency has completed", () => {
    const tracker = createReadyTracker({
      nodes: [node("a"), node("b", "a"), node("c", "a"), node("join", "b", "c")],
    });
    tracker.markCompleted("a");
    tracker.markCompleted("b");
    expect(tracker.ready()).toEqual(["c"]);
    tracker.markCompleted("c");
    expect(tracker.ready()).toEqual(["join"]);
    tracker.markCompleted("join");
    expect(tracker.ready()).toEqual([]);
    expect(tracker.completedCount).toBe(4);
  });

  it("keeps a ready node ready until it is marked completed", () => {
    const tracker = createReadyTracker({ nodes: [node("a"), node("b", "a")] });
    expect(tracker.ready()).toEqual(["a"]);
    expect(tracker.ready()).toEqual(["a"]);
  });

  it("takes initially completed nodes into account and ignores unknown or repeated names", () => {
    const tracker = createReadyTracker({
      nodes: [node("a"), node("b", "a"), node("c", "b")],
      completed: ["a", "ghost"],
    });
    expect(tracker.completedCount).toBe(1);
    expect(tracker.ready()).toEqual(["b"]);
    tracker.markCompleted("a");
    tracker.markCompleted("ghost");
    expect(tracker.completedCount).toBe(1);
    expect(tracker.ready()).toEqual(["b"]);
  });

  it("never readies a node that depends on a name outside the DAG", () => {
    const tracker = createReadyTracker({ nodes: [node("a"), node("b", "missing")] });
    tracker.markCompleted("a");
    expect(tracker.ready()).toEqual([]);
  });

  it("matches computeReadySet wave by wave on random DAGs", () => {
    for (let seed = 1; seed <= 25; seed++) {
      const nodes = randomDag({ n: 60, seed });
      const tracker = createReadyTracker({ nodes });
      const completed = new Set<string>();
      for (;;) {
        const expected = computeReadySet({ nodes, completed, running: new Set() });
        expect(tracker.ready()).toEqual(expected);
        if (expected.length === 0) break;
        for (const name of expected) {
          completed.add(name);
          tracker.markCompleted(name);
        }
      }
      expect(tracker.completedCount).toBe(nodes.length);
    }
  });

  it("reads each node's dependencies once, however many waves run", () => {
    const n = 2_000;
    let reads = 0;
    const nodes: DagNode[] = Array.from({ length: n }, (_, i) => {
      const deps = i === 0 ? [] : [`s${i - 1}`];
      return {
        name: `s${i}`,
        get dependsOn() {
          reads++;
          return deps;
        },
      };
    });
    const tracker = createReadyTracker({ nodes });
    for (let wave = 0; wave < n; wave++) {
      const [next] = tracker.ready();
      expect(next).toBe(`s${wave}`);
      tracker.markCompleted(next!);
    }
    expect(tracker.completedCount).toBe(n);
    expect(reads).toBe(n);
  });
});
