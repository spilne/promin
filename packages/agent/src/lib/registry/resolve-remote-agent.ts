import type { RegisteredAgent } from "./types.ts";
import { RemoteAgent } from "./remote-agent.ts";

/**
 * Materialize a `RegisteredAgent` with a `remote` backend into a live
 * `RemoteAgent` proxy that forwards all calls over HTTP.
 *
 * `fetch` replaces the transport. Recipes written by self-registering
 * deployments name endpoints this host does not control; resolve those
 * with `fetch: createOutboundFetch({ policy })` so every call is checked
 * against the policy and connects to the address it vetted (a name that
 * re-resolves to a private address later is refused).
 *
 * Throws if the recipe's backend type is not `"remote"`.
 */
export function resolveRemoteAgent(params: {
  agent: RegisteredAgent;
  fetch?: (url: string, init: RequestInit) => Promise<Response>;
}): RemoteAgent {
  const { agent } = params;
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
    ...(params.fetch !== undefined && { fetch: params.fetch }),
  });
}
