// An MCP endpoint defined in one file. Shared tools are named (they come
// from the host's tool sources: `listWorkflows` and the ./tools folder);
// a tool defined right here is private to this endpoint.
//
// Connect: /ns/<namespace>/mcp/ops (tenant) or /mcp/ops (operator).
import { mcpEndpoint, tool } from "@promin/agent";
import { z } from "zod";

export default mcpEndpoint({
  title: "Zorya ops",
  instructions:
    "Inspect this Zorya server: list its workflows, read the time, add or multiply numbers.",
  tools: [
    "listWorkflows",
    "currentTime",
    { tool: "calculate", name: "calc" },
    tool({
      name: "ping",
      description: "Answer pong, with the time on the server.",
      parameters: z.object({}),
      idempotent: true,
      execute: async () => ({ pong: true, at: new Date().toISOString() }),
    }),
  ],
  metadata: { description: "Ops tools for this demo server", tags: ["demo"] },
});
