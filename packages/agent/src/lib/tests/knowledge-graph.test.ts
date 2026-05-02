import { describe, it, expect } from "bun:test";
import { InMemoryKnowledgeGraph } from "../knowledge-graph.ts";

describe("InMemoryKnowledgeGraph", () => {
  it("adds relationships and creates nodes implicitly", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob");

    const results = await kg.query("worked_with");
    expect(results).toHaveLength(1);
    expect(results[0].from).toBe("alice");
    expect(results[0].to).toBe("bob");
  });

  it("queries relationships with filters", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob");
    await kg.addRelationship("alice", "worked_with", "charlie");
    await kg.addRelationship("bob", "worked_with", "charlie");

    const fromAlice = await kg.query("worked_with", { from: "alice" });
    expect(fromAlice).toHaveLength(2);

    const toCharlie = await kg.query("worked_with", { to: "charlie" });
    expect(toCharlie).toHaveLength(2);

    const aliceToBob = await kg.query("worked_with", { from: "alice", to: "bob" });
    expect(aliceToBob).toHaveLength(1);
  });

  it("handles metadata on relationships", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob", { project: "X", year: 2024 });

    const results = await kg.query("worked_with");
    expect(results[0].metadata).toEqual({ project: "X", year: 2024 });
  });

  it("searches nodes by ID", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addNode({ id: "alice", type: "person", metadata: { email: "alice@example.com" } });
    await kg.addNode({ id: "bob", type: "person", metadata: { email: "bob@example.com" } });
    await kg.addRelationship("alice", "worked_with", "bob");

    const { nodes } = await kg.search("alice");
    expect(nodes).toHaveLength(1);
    expect(nodes[0].id).toBe("alice");
  });

  it("searches by metadata", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addNode({ id: "alice", metadata: { department: "engineering" } });
    await kg.addNode({ id: "bob", metadata: { department: "sales" } });

    const { nodes } = await kg.search("engineering");
    expect(nodes).toHaveLength(1);
    expect(nodes[0].id).toBe("alice");
  });

  it("returns connected edges for a node", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob");
    await kg.addRelationship("bob", "worked_with", "charlie");
    await kg.addRelationship("charlie", "reports_to", "alice");

    const all = await kg.getConnected("alice");
    expect(all).toHaveLength(2); // alice->bob and charlie->alice

    const outgoing = await kg.getConnected("alice", "out");
    expect(outgoing).toHaveLength(1);
    expect(outgoing[0].to).toBe("bob");

    const incoming = await kg.getConnected("alice", "in");
    expect(incoming).toHaveLength(1);
    expect(incoming[0].from).toBe("charlie");
  });

  it("deletes relationships", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob");
    await kg.addRelationship("alice", "worked_with", "charlie");

    await kg.deleteRelationship("alice", "worked_with", "bob");

    const results = await kg.query("worked_with");
    expect(results).toHaveLength(1);
    expect(results[0].to).toBe("charlie");
  });

  it("deletes nodes and their edges", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob");
    await kg.addRelationship("bob", "worked_with", "charlie");

    await kg.deleteNode("bob");

    const results = await kg.query("worked_with");
    expect(results).toHaveLength(0);

    const searchResults = await kg.search("bob");
    expect(searchResults.nodes).toHaveLength(0);
  });

  it("respects limit in queries and search", async () => {
    const kg = new InMemoryKnowledgeGraph();
    for (let i = 0; i < 5; i++) {
      await kg.addRelationship("alice", "worked_with", `person${i}`);
    }

    const results = await kg.query("worked_with", { limit: 2 });
    expect(results).toHaveLength(2);
  });

  it("updates relationships when adding duplicate", async () => {
    const kg = new InMemoryKnowledgeGraph();
    await kg.addRelationship("alice", "worked_with", "bob", { year: 2023 });
    await kg.addRelationship("alice", "worked_with", "bob", { year: 2024 });

    const results = await kg.query("worked_with");
    expect(results).toHaveLength(1);
    expect(results[0].metadata?.year).toBe(2024);
  });
});
