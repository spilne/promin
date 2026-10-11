// A stdio MCP server spawned by the transport tests.
//   whoami  — the process id and the credential it received
//   env     — the value of the environment variable named by `name`
//   spawn   — starts a long-running helper process and returns its pid
//   crash   — exits the process mid-call
// It writes one line to stderr at start.

import { spawn } from "node:child_process";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";

const text = (value: string) => ({ content: [{ type: "text" as const, text: value }] });

const server = new Server({ name: "fixture", version: "1.0.0" }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: ["whoami", "env", "spawn", "crash"].map((name) => ({
    name,
    inputSchema: { type: "object" as const },
  })),
}));
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  switch (request.params.name) {
    case "crash":
      process.exit(1);
      break;
    case "env": {
      const name = String(request.params.arguments?.["name"] ?? "");
      return text(process.env[name] ?? "<unset>");
    }
    case "spawn": {
      const helper = spawn("sleep", ["60"], { stdio: "ignore" });
      return text(String(helper.pid));
    }
  }
  return text(`${process.pid}:${process.env["MCP_TOKEN"] ?? ""}`);
});
process.stderr.write("fixture server ready\n");
await server.connect(new StdioServerTransport());
