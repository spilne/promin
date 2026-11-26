// A scoped tool: it runs in the caller's (namespace, resource). Over MCP the
// namespace comes from the URL (/ns/<namespace>/mcp/support) and the
// resource from the X-Zorya-Resource header.
import { createScopedTool } from "@promin/agent";
import { z } from "zod";

export default createScopedTool({
  name: "whoami",
  description: "Name the tenant (namespace) and resource this call acts for.",
  parameters: z.object({}),
  execute: async (_input, ctx) => ({ namespace: ctx.namespaceId, resource: ctx.resourceId }),
});
