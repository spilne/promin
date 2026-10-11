// ---------------------------------------------------------------------------
// Request body limits for `ZoryaServer.listen`.
//
// Bun's own `maxRequestBodySize` answers an oversized body with an empty
// 413 before the fetch handler runs, so the server sets Bun's ceiling above
// its own limits and enforces them here, where it can answer with the JSON
// error envelope. Bun's ceiling still bounds what the transport buffers.
// ---------------------------------------------------------------------------

import { payloadTooLarge } from "./router.ts";

/** Default body cap for API and RPC requests (bytes). */
export const DEFAULT_MAX_REQUEST_BODY_BYTES = 1_048_576;

/** Default body cap for `/webhooks/*` (bytes); GitHub sends up to 25 MB. */
export const DEFAULT_MAX_WEBHOOK_BODY_BYTES = 25 * 1_048_576;

/** Default seconds a connection may sit idle. SSE heartbeats every 15s. */
export const DEFAULT_IDLE_TIMEOUT_SECONDS = 60;

/** The body cap that applies to a request path. */
export function bodyLimitFor(params: {
  path: string;
  maxRequestBodySize: number;
  maxWebhookBodySize: number;
}): number {
  return params.path.startsWith("/webhooks/")
    ? params.maxWebhookBodySize
    : params.maxRequestBodySize;
}

/**
 * Enforce `maxBytes` on a request body. Returns the request to dispatch, or
 * a JSON 413. A declared `content-length` is checked up front; a chunked
 * body is read up to the limit and re-wrapped so handlers can read it.
 */
export async function limitRequestBody(params: {
  req: Request;
  maxBytes: number;
}): Promise<Request | Response> {
  const { req, maxBytes } = params;
  if (req.body === null) return req;

  const declared = req.headers.get("content-length");
  if (declared !== null) {
    const n = Number(declared);
    if (Number.isFinite(n) && n >= 0) {
      return n > maxBytes ? payloadTooLarge(maxBytes) : req;
    }
  }

  const reader = req.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => {});
      return payloadTooLarge(maxBytes);
    }
    chunks.push(value);
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    body.set(c, offset);
    offset += c.byteLength;
  }
  const headers = new Headers(req.headers);
  headers.delete("transfer-encoding");
  headers.set("content-length", String(total));
  return new Request(req.url, { method: req.method, headers, body, signal: req.signal });
}
