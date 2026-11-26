// A folder endpoint: the id is the folder name ("support"), and every tool
// under ./tools is part of it and private to it.
import { mcpEndpoint } from "@promin/agent";

export default mcpEndpoint({
  title: "Support desk",
  instructions: "Look up who you are acting for before answering account questions.",
  tools: ["currentTime"],
  metadata: { description: "Tenant-scoped support tools", tags: ["demo"] },
});
