// ---------------------------------------------------------------------------
// Outbound endpoint check for self-registering remote deployments.
//
// A deployment registers an `endpoint` this server will later call with
// agent prompts. Without a policy that is a server-side request forgery
// primitive: `http://169.254.169.254/` (cloud metadata), `http://localhost`
// admin ports, or any host on the private network. Registration checks the
// endpoint with `@promin/agent`'s `OutboundPolicy` (public http(s) hosts
// only by default, optionally narrowed by an allow-list).
//
// A registration-time check alone cannot stop DNS rebinding: the name can
// resolve to a private address later. The host that resolves the
// registered `RemoteAgentBackend` recipes closes that gap by passing
// `createOutboundFetch({ policy })` to `resolveRemoteAgent`, which checks
// every call and connects to the address it vetted.
// ---------------------------------------------------------------------------

import { checkOutboundUrl, type OutboundPolicy, type OutboundRejection } from "@promin/agent";

export type EndpointCheck =
  | { readonly ok: true; readonly url: string }
  | { readonly ok: false; readonly error: string; readonly message: string };

const ERROR_CODES: Record<OutboundRejection, string> = {
  invalid_url: "invalid_endpoint",
  host_not_allowed: "endpoint_not_allowed",
  host_unresolvable: "endpoint_unresolvable",
};

/** Validate `raw` against `policy`; returns the normalized URL on success. */
export async function checkRemoteEndpoint(params: {
  readonly raw: string;
  readonly policy?: OutboundPolicy;
}): Promise<EndpointCheck> {
  const check = await checkOutboundUrl({
    raw: params.raw,
    ...(params.policy !== undefined && { policy: params.policy }),
  });
  if (!check.ok) {
    return { ok: false, error: ERROR_CODES[check.error], message: `endpoint: ${check.message}` };
  }
  // Store without a trailing slash so `${endpoint}/api/...` stays canonical.
  return { ok: true, url: check.url.toString().replace(/\/+$/, "") };
}
