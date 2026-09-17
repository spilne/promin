// ---------------------------------------------------------------------------
// KnowledgeGraph — in-memory graph store for entity/relationship reasoning.
//
// Agents can query, add, search, and delete nodes/edges to build up
// context about domain entities without needing persistent storage.
// Useful for tracking relationships, dependencies, and facts that
// benefit from graph traversal.
//
// Usage:
//   const kg = new InMemoryKnowledgeGraph();
//   await kg.addRelationship("alice", "worked_with", "bob");
//   const results = await kg.query("worked_with", { from: "alice" });
// ---------------------------------------------------------------------------

export interface KnowledgeGraphNode {
  id: string;
  type?: string;
  metadata?: Record<string, unknown>;
}

export interface KnowledgeGraphEdge {
  from: string;
  to: string;
  type: string;
  metadata?: Record<string, unknown>;
}

export interface QueryOptions {
  from?: string;
  to?: string;
  limit?: number;
}

export interface SearchResult {
  nodes: KnowledgeGraphNode[];
  edges: KnowledgeGraphEdge[];
}

export interface KnowledgeGraph {
  /** Add or update a node in the graph. */
  addNode(node: KnowledgeGraphNode): Promise<void>;

  /** Add an edge (relationship) between two nodes. */
  addRelationship(
    from: string,
    type: string,
    to: string,
    metadata?: Record<string, unknown>,
  ): Promise<void>;

  /** Query edges by type and optional from/to filters. */
  query(type: string, options?: QueryOptions): Promise<KnowledgeGraphEdge[]>;

  /** Search nodes by ID or text metadata. */
  search(query: string, limit?: number): Promise<SearchResult>;

  /** Delete a node (and all connected edges). */
  deleteNode(id: string): Promise<void>;

  /** Delete a specific edge. */
  deleteRelationship(from: string, type: string, to: string): Promise<void>;

  /** Get a node by ID. */
  getNode(id: string): Promise<KnowledgeGraphNode | undefined>;

  /** Get all edges connected to a node. */
  getConnected(id: string, direction?: "in" | "out" | "both"): Promise<KnowledgeGraphEdge[]>;
}

// ---- InMemoryKnowledgeGraph ----

export class InMemoryKnowledgeGraph implements KnowledgeGraph {
  private nodes = new Map<string, KnowledgeGraphNode>();
  private edges: KnowledgeGraphEdge[] = [];

  async addNode(node: KnowledgeGraphNode): Promise<void> {
    this.nodes.set(node.id, node);
  }

  async addRelationship(
    from: string,
    type: string,
    to: string,
    metadata?: Record<string, unknown>,
  ): Promise<void> {
    // Ensure nodes exist
    if (!this.nodes.has(from)) this.nodes.set(from, { id: from });
    if (!this.nodes.has(to)) this.nodes.set(to, { id: to });

    // Check if edge already exists, update or add
    const existingIndex = this.edges.findIndex(
      (e) => e.from === from && e.type === type && e.to === to,
    );
    if (existingIndex >= 0) {
      this.edges[existingIndex] = { from, to, type, metadata };
    } else {
      this.edges.push({ from, to, type, metadata });
    }
  }

  async query(type: string, options?: QueryOptions): Promise<KnowledgeGraphEdge[]> {
    return this.edges
      .filter(
        (e) =>
          e.type === type &&
          (!options?.from || e.from === options.from) &&
          (!options?.to || e.to === options.to),
      )
      .slice(0, options?.limit);
  }

  async search(query: string, limit = 10): Promise<SearchResult> {
    const lowerQuery = query.toLowerCase();

    // Search node IDs
    const matchedNodes = Array.from(this.nodes.values()).filter(
      (n) =>
        n.id.toLowerCase().includes(lowerQuery) ||
        JSON.stringify(n.metadata || {})
          .toLowerCase()
          .includes(lowerQuery),
    );

    // Find edges connected to matched nodes
    const matchedIds = new Set(matchedNodes.map((n) => n.id));
    const connectedEdges = this.edges.filter((e) => matchedIds.has(e.from) || matchedIds.has(e.to));

    return {
      nodes: matchedNodes.slice(0, limit),
      edges: connectedEdges.slice(0, limit),
    };
  }

  async deleteNode(id: string): Promise<void> {
    this.nodes.delete(id);
    this.edges = this.edges.filter((e) => e.from !== id && e.to !== id);
  }

  async deleteRelationship(from: string, type: string, to: string): Promise<void> {
    this.edges = this.edges.filter((e) => !(e.from === from && e.type === type && e.to === to));
  }

  async getNode(id: string): Promise<KnowledgeGraphNode | undefined> {
    return this.nodes.get(id);
  }

  async getConnected(
    id: string,
    direction: "in" | "out" | "both" = "both",
  ): Promise<KnowledgeGraphEdge[]> {
    return this.edges.filter((e) => {
      if (direction === "out") return e.from === id;
      if (direction === "in") return e.to === id;
      return e.from === id || e.to === id;
    });
  }
}
