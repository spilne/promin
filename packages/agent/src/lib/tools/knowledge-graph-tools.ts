import { z } from "zod";
import type { AgentTool } from "../tool.ts";
import { multiTool, command } from "../multi-tool.ts";
import type { KnowledgeGraph } from "../knowledge-graph.ts";

export interface KnowledgeGraphToolsConfig {
  graph: KnowledgeGraph;
}

/**
 * Creates a unified knowledge graph tool that lets agents query, add, and manage
 * relationships between entities.
 *
 * Usage:
 *   const kg = new InMemoryKnowledgeGraph();
 *   const tool = createKnowledgeGraphTool({ graph: kg });
 *
 *   agentLoop({
 *     tools: { knowledgeGraph: tool },
 *   });
 *
 * Agents can then:
 *   - Add facts: "Alice worked with Bob on Project X"
 *   - Query: "Who did Alice work with?"
 *   - Search: "Find people who worked on security"
 */
export function createKnowledgeGraphTool(config: KnowledgeGraphToolsConfig): AgentTool {
  const { graph } = config;

  return multiTool({
    name: "knowledgeGraph",
    description:
      "Manage a knowledge graph of entities and relationships. Use to track facts, relationships, and dependencies without losing information between steps.",
    commands: {
      add: command({
        description: "Add a relationship between two entities (creates nodes if needed)",
        parameters: z.object({
          from: z.string().describe("Source entity (e.g. 'alice')"),
          type: z
            .string()
            .describe("Relationship type (e.g. 'worked_with', 'reported_by', 'owns')"),
          to: z.string().describe("Target entity (e.g. 'bob')"),
          metadata: z
            .record(z.unknown())
            .optional()
            .describe("Optional metadata (e.g. {project: 'X', year: 2024})"),
        }),
        execute: async ({ from, type, to, metadata }) => {
          await graph.addRelationship(from, type, to, metadata);
          return `Added relationship: ${from} --[${type}]--> ${to}`;
        },
      }),

      query: command({
        description: "Query relationships by type and optional filters",
        parameters: z.object({
          type: z.string().describe("Relationship type to query (e.g. 'worked_with')"),
          from: z.string().optional().describe("Filter by source entity"),
          to: z.string().optional().describe("Filter by target entity"),
          limit: z.number().int().min(1).max(50).optional().default(10).describe("Max results"),
        }),
        execute: async ({ type, from, to, limit }) => {
          const results = await graph.query(type, { from, to, limit });
          if (results.length === 0) return `No ${type} relationships found.`;
          return results
            .map(
              (r) =>
                `${r.from} --[${r.type}]--> ${r.to}${r.metadata ? ` (${JSON.stringify(r.metadata)})` : ""}`,
            )
            .join("\n");
        },
      }),

      search: command({
        description: "Search for entities and their connected relationships",
        parameters: z.object({
          query: z.string().describe("Entity name or metadata text to search for"),
          limit: z.number().int().min(1).max(50).optional().default(10).describe("Max results"),
        }),
        execute: async ({ query, limit }) => {
          const { nodes, edges } = await graph.search(query, limit);
          if (nodes.length === 0 && edges.length === 0) return `No results for "${query}"`;

          const parts: string[] = [];
          if (nodes.length > 0) {
            parts.push(
              `Entities:\n${nodes.map((n) => `  - ${n.id}${n.type ? ` (${n.type})` : ""}`).join("\n")}`,
            );
          }
          if (edges.length > 0) {
            parts.push(
              `Relationships:\n${edges.map((e) => `  ${e.from} --[${e.type}]--> ${e.to}`).join("\n")}`,
            );
          }
          return parts.join("\n\n");
        },
      }),

      connected: command({
        description: "Find all relationships for a specific entity",
        parameters: z.object({
          id: z.string().describe("Entity ID to query"),
          direction: z
            .enum(["in", "out", "both"])
            .optional()
            .default("both")
            .describe("in/out/both"),
        }),
        execute: async ({ id, direction }) => {
          const edges = await graph.getConnected(id, direction);
          if (edges.length === 0) return `No ${direction} relationships found for "${id}"`;

          return edges
            .map((e) => {
              if ((direction === "in" || direction === "both") && e.to === id) {
                return `${e.from} --[${e.type}]--> ${e.to}`;
              }
              if ((direction === "out" || direction === "both") && e.from === id) {
                return `${e.from} --[${e.type}]--> ${e.to}`;
              }
              return undefined;
            })
            .filter(Boolean)
            .join("\n");
        },
      }),

      delete: command({
        description: "Delete a relationship or entity",
        parameters: z.object({
          type: z.enum(["relationship", "node"]).describe("Delete a relationship or entire node"),
          from: z.string().optional().describe("Source (for relationship)"),
          relationshipType: z.string().optional().describe("Relationship type (for relationship)"),
          to: z.string().optional().describe("Target (for relationship)"),
          id: z.string().optional().describe("Entity ID (for node)"),
        }),
        execute: async ({ type, from, relationshipType, to, id }) => {
          if (type === "relationship") {
            if (!from || !relationshipType || !to) {
              return "Error: relationship deletion requires 'from', 'relationshipType', and 'to'";
            }
            await graph.deleteRelationship(from, relationshipType, to);
            return `Deleted: ${from} --[${relationshipType}]--> ${to}`;
          } else {
            if (!id) return "Error: node deletion requires 'id'";
            await graph.deleteNode(id);
            return `Deleted node: ${id}`;
          }
        },
      }),
    },
  });
}
