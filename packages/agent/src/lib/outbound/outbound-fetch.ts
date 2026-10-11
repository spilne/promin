// ---------------------------------------------------------------------------
// `outboundFetch` — a fetch that only reaches hosts an `OutboundPolicy`
// accepts, and connects to the exact address the policy vetted.
//
// Every hop (redirects included) is checked with `checkOutboundUrl`, which
// resolves the host once. The request then goes to that resolved IP with
// the original `Host` header and, for https, the original name as TLS SNI
// and certificate identity — so a DNS answer that changes between check
// and connect (DNS rebinding) never reaches a private address, and TLS
// still authenticates the named host.
//
// Redirects are followed for GET / HEAD only; other methods get the 3xx
// response back. Credentials (`authorization`, `cookie`) are dropped when a
// redirect leaves the original origin.
// ---------------------------------------------------------------------------

import { isIP } from "node:net";
import {
  checkOutboundUrl,
  type OutboundPolicy,
  type OutboundRejection,
} from "./outbound-policy.ts";

/** The transport `outboundFetch` drives. Default: the global `fetch`. */
export type OutboundFetchTransport = (url: string, init: BunFetchRequestInit) => Promise<Response>;

/** Why `outboundFetch` refused to send (or keep following) a request. */
export type OutboundErrorCode = OutboundRejection | "too_many_redirects";

export class OutboundRequestError extends Error {
  readonly code: OutboundErrorCode;
  constructor(params: { code: OutboundErrorCode; message: string }) {
    super(params.message);
    this.name = "OutboundRequestError";
    this.code = params.code;
  }
}

export interface OutboundFetchOptions {
  /** Which hosts may be reached. Default: public http(s) hosts only. */
  readonly policy?: OutboundPolicy;
  /** Redirects followed for GET / HEAD, each checked against `policy`. Default 3. */
  readonly maxRedirects?: number;
  /** Underlying transport. Default: global `fetch`. Tests pass a fake. */
  readonly fetch?: OutboundFetchTransport;
}

export interface OutboundResponse {
  readonly response: Response;
  /** The logical URL that answered (after redirects), with its hostname — not the pinned IP. */
  readonly url: string;
}

const DEFAULT_MAX_REDIRECTS = 3;
const CREDENTIAL_HEADERS = ["authorization", "cookie", "proxy-authorization"];

/** Send one request under `policy`, pinned to the vetted address. */
export async function outboundFetch(
  params: OutboundFetchOptions & { readonly url: string; readonly init?: RequestInit },
): Promise<OutboundResponse> {
  const maxRedirects = params.maxRedirects ?? DEFAULT_MAX_REDIRECTS;
  const transport = params.fetch ?? ((url, init) => fetch(url, init));
  const init = params.init ?? {};
  const method = (init.method ?? "GET").toUpperCase();
  const followRedirects = method === "GET" || method === "HEAD";
  let headers = new Headers(init.headers);
  let target = params.url;

  for (let hop = 0; ; hop += 1) {
    const check = await checkOutboundUrl({
      raw: target,
      ...(params.policy !== undefined && { policy: params.policy }),
    });
    if (!check.ok) throw new OutboundRequestError({ code: check.error, message: check.message });

    const response = await connectPinned({ check, init: { ...init, headers }, transport });
    const location = response.headers.get("location");
    if (!followRedirects || response.status < 300 || response.status >= 400 || !location) {
      return { response, url: check.url.toString() };
    }
    await response.body?.cancel().catch(() => {});
    if (hop >= maxRedirects) {
      throw new OutboundRequestError({
        code: "too_many_redirects",
        message: `${params.url} redirected more than ${maxRedirects} times.`,
      });
    }
    const next = new URL(location, check.url);
    if (next.origin !== check.url.origin) {
      headers = new Headers(headers);
      for (const name of CREDENTIAL_HEADERS) headers.delete(name);
    }
    target = next.toString();
  }
}

/**
 * Build a fetch-compatible function that sends every request through
 * `outboundFetch`. For clients that take a `fetch` override.
 */
export function createOutboundFetch(
  options: OutboundFetchOptions = {},
): (url: string, init?: RequestInit) => Promise<Response> {
  return async (url, init) =>
    (await outboundFetch({ ...options, url, ...(init && { init }) })).response;
}

/** Connect to the first vetted address that answers, keeping the logical host. */
async function connectPinned(params: {
  check: Extract<Awaited<ReturnType<typeof checkOutboundUrl>>, { ok: true }>;
  init: RequestInit & { headers: Headers };
  transport: OutboundFetchTransport;
}): Promise<Response> {
  const { check, init, transport } = params;
  const literalHost = isIP(check.hostname) !== 0;
  const headers = new Headers(init.headers);
  headers.set("host", check.url.host);
  // A body that can be read twice lets a refused connection fall over to
  // the next address; a stream can only be sent once.
  const replayable = init.body === undefined || init.body === null || typeof init.body === "string";
  const candidates = replayable ? check.addresses : check.addresses.slice(0, 1);
  let lastError: unknown;
  for (const address of candidates) {
    const pinned = new URL(check.url);
    pinned.hostname = isIP(address) === 6 ? `[${address}]` : address;
    const request: BunFetchRequestInit = {
      ...init,
      headers,
      redirect: "manual",
      ...(check.url.protocol === "https:" &&
        !literalHost && { tls: { serverName: check.hostname } }),
    };
    try {
      return await transport(pinned.toString(), request);
    } catch (err) {
      if (init.signal?.aborted) throw err;
      lastError = err;
    }
  }
  throw lastError;
}
