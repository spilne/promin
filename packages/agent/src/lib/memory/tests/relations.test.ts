import { describe, expect, it } from "bun:test";
import { normalizeEntityName, relationKey, relationNeighborhood } from "../relations.ts";
import type { Relation } from "../types.ts";

function rel(id: string, subject: string, predicate: string, object: string): Relation {
  return { id, subject, predicate, object, source: "model", confidence: 0.6, createdAt: 0 };
}

const GRAPH = [
  rel("1", "Anton", "works at", "Acme"),
  rel("2", "Acme", "is based in", "Berlin"),
  rel("3", "Berlin", "is in", "Germany"),
  rel("4", "Maria", "lives in", "Lisbon"),
];

describe("relations", () => {
  it("normalizes names and triples", () => {
    expect(normalizeEntityName("  Acme   Corp ")).toBe("acme corp");
    expect(relationKey({ subject: "A ", predicate: "Knows", object: "b" })).toBe(
      relationKey({ subject: "a", predicate: "knows", object: "B" }),
    );
  });

  it("walks the graph hop by hop from the entities the query names", () => {
    const ids = (hops: number) =>
      relationNeighborhood({
        relations: GRAPH,
        query: "where does Anton work?",
        hops,
        limit: 10,
      }).map((r) => r.id);
    expect(ids(1)).toEqual(["1"]);
    expect(ids(2)).toEqual(["1", "2"]);
    expect(ids(3)).toEqual(["1", "2", "3"]);
  });

  it("follows relations against their direction, matches whole words only, and respects the limit", () => {
    const query = (q: string, limit = 10) =>
      relationNeighborhood({ relations: GRAPH, query: q, hops: 1, limit }).map((r) => r.id);
    expect(query("tell me about Acme")).toEqual(["1", "2"]);
    expect(query("tell me about Acme", 1)).toEqual(["1"]);
    expect(query("Antonio")).toEqual([]);
    expect(query("nothing relevant")).toEqual([]);
  });
});
