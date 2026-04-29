import type { RegisteredAgent } from "./types.ts";
import { RemoteAgent } from "./remote-agent.ts";

/**
 * Materialize a `RegisteredAgent` with a `remote` backend into a live
 * `RemoteAgent` proxy that forwards all calls over HTTP.
 *
 * Throws if the recipe's backend type is not `"remote"`.
 */
export function resolveRemoteAgent(agent: RegisteredAgent): RemoteAgent {
  if (agent.backend.type !== "remote") {
    throw new Error(
      `resolveRemoteAgent: expected backend.type "remote", got "${agent.backend.type}" (agent: ${agent.id})`,
    );
  }
  const b = agent.backend;
  return new RemoteAgent({
    endpoint: b.endpoint.replace(/\/$/, ""),
    remoteAgentId: b.remoteAgentId,
    auth: b.auth,
    timeoutMs: b.timeoutMs,
  });
}
