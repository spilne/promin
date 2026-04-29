// ---------------------------------------------------------------------------
// remote-bot — federation demo.
//
// Points at `claude-bot` on the same Zorya instance (localhost:2024) so you
// can see how a remote backend recipe round-trips without standing up a
// second server.  In production you'd point at a different deployment.
// ---------------------------------------------------------------------------
import type { RegisterAgentInput } from "@promin/agent";

export const REMOTE_BOT: RegisterAgentInput = {
  id: "remote-bot",
  backend: {
    type: "remote",
    endpoint: "http://localhost:2024",
    remoteAgentId: "claude-bot",
  },
  metadata: {
    description:
      "Federation proxy — forwards every call to claude-bot on the local Zorya instance. " +
      "Demonstrates RemoteAgentBackend without requiring a separate server.",
    capabilities: ["chat"],
    tags: ["demo", "remote", "federation"],
  },
};
