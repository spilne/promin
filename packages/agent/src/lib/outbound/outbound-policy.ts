// ---------------------------------------------------------------------------
// Outbound URL policy — which hosts server-side code may call on behalf of
// a model, an operator request or a self-registering peer.
//
// Without a policy every such call is a server-side request forgery
// primitive: `http://169.254.169.254/` (cloud metadata), `http://localhost`
// admin ports, or any host on the private network. The default policy
// accepts only public http(s) hosts — literal IPs are classified directly,
// hostnames through one DNS lookup — and an optional allow-list narrows it
// further.
//
// `checkOutboundUrl` returns the addresses it vetted. A caller that goes on
// to connect must connect to one of THOSE addresses (`outboundFetch` does),
// not resolve the name again: a second lookup can answer differently (DNS
// rebinding) and land on a private address the check never saw.
// ---------------------------------------------------------------------------

import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import { isPublicAddress } from "./address-ranges.ts";

export interface OutboundPolicy {
  /**
   * When set, only these hosts are accepted. Entries are exact hostnames
   * (case-insensitive) or `*.example.com` suffix patterns.
   */
  readonly allowedHosts?: ReadonlyArray<string>;
  /**
   * Accept loopback, private, link-local and other non-public addresses.
   * Default: `false`. Turn on only for trusted single-network setups.
   */
  readonly allowPrivateNetworks?: boolean;
  /**
   * Resolve a hostname to its addresses. Default: a DNS lookup of every
   * address. Tests inject a fake.
   */
  readonly resolveHost?: (hostname: string) => Promise<ReadonlyArray<string>>;
}

/** Why `checkOutboundUrl` refused a URL. */
export type OutboundRejection = "invalid_url" | "host_not_allowed" | "host_unresolvable";

export type OutboundUrlCheck =
  | {
      readonly ok: true;
      readonly url: URL;
      /** The URL's host without IPv6 brackets, lower-cased. */
      readonly hostname: string;
      /**
       * The vetted addresses `hostname` resolved to (the literal itself for
       * an IP host). Connect to one of these, never to a fresh lookup.
       */
      readonly addresses: ReadonlyArray<string>;
    }
  | { readonly ok: false; readonly error: OutboundRejection; readonly message: string };

/** Hostnames that name the local machine or cloud-internal services. */
const INTERNAL_HOSTNAMES = [/^localhost$/, /\.localhost$/, /\.internal$/, /\.local$/];

/** Validate `raw` against `policy` and resolve its host once. */
export async function checkOutboundUrl(params: {
  readonly raw: string;
  readonly policy?: OutboundPolicy;
}): Promise<OutboundUrlCheck> {
  const policy = params.policy ?? {};
  let url: URL;
  try {
    url = new URL(params.raw);
  } catch {
    return reject("invalid_url", "URL must be absolute.");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return reject("invalid_url", "URL must use http or https.");
  }
  if (url.username !== "" || url.password !== "") {
    return reject("invalid_url", "URL must not embed credentials.");
  }
  // WHATWG URL parsing already normalizes numeric hosts (`http://2130706433`
  // → `127.0.0.1`); IPv6 literals keep their brackets.
  const hostname = url.hostname.toLowerCase().replace(/^\[(.*)\]$/, "$1");
  if (hostname.length === 0) return reject("invalid_url", "URL must name a host.");

  if (policy.allowedHosts && !policy.allowedHosts.some((p) => hostMatches(hostname, p))) {
    return reject("host_not_allowed", `Host "${hostname}" is not in the allowed host list.`);
  }

  const literal = isIP(hostname) !== 0;
  if (
    !literal &&
    !policy.allowPrivateNetworks &&
    INTERNAL_HOSTNAMES.some((re) => re.test(hostname))
  ) {
    return reject("host_not_allowed", `Host "${hostname}" is an internal host.`);
  }

  let addresses: ReadonlyArray<string>;
  if (literal) {
    addresses = [hostname];
  } else {
    try {
      addresses = await (policy.resolveHost ?? defaultResolveHost)(hostname);
    } catch {
      return reject("host_unresolvable", `Host "${hostname}" did not resolve.`);
    }
    if (addresses.length === 0 || addresses.some((a) => isIP(a) === 0)) {
      return reject("host_unresolvable", `Host "${hostname}" did not resolve.`);
    }
  }
  if (!policy.allowPrivateNetworks && addresses.some((a) => !isPublicAddress(a))) {
    return reject(
      "host_not_allowed",
      literal
        ? `Host "${hostname}" is a non-public address.`
        : `Host "${hostname}" resolves to a non-public address.`,
    );
  }
  return { ok: true, url, hostname, addresses };
}

function reject(error: OutboundRejection, message: string): OutboundUrlCheck {
  return { ok: false, error, message };
}

function hostMatches(host: string, pattern: string): boolean {
  const p = pattern.toLowerCase();
  if (p.startsWith("*.")) return host.endsWith(p.slice(1)) && host.length > p.length - 1;
  return host === p;
}

async function defaultResolveHost(hostname: string): Promise<ReadonlyArray<string>> {
  const rows = await lookup(hostname, { all: true, verbatim: true });
  return rows.map((r) => r.address);
}
