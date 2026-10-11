// ---------------------------------------------------------------------------
// The Bun listener: upgrades `/ws/worker` to the worker WebSocket, applies
// the per-path request-body limits, counts requests in flight, and hands
// everything else to the server's `handle`.
// ---------------------------------------------------------------------------

import { errorResponse } from "../router.ts";
import {
  DEFAULT_IDLE_TIMEOUT_SECONDS,
  DEFAULT_MAX_REQUEST_BODY_BYTES,
  DEFAULT_MAX_WEBHOOK_BODY_BYTES,
  bodyLimitFor,
  limitRequestBody,
} from "../request-limits.ts";
import type { ListenOptions, Logger } from "../server-config.ts";
import type { WorkerWebSocketServer } from "../services/worker-ws-server.ts";
import type { InFlightRequests } from "./mount-lifecycle.ts";

/** The parts of a Bun server the ZoryaServer keeps. */
export interface ListeningServer {
  readonly port: number;
  readonly hostname: string;
  stop(closeActiveConnections?: boolean): Promise<void> | void;
}

export function serveListener(params: {
  readonly options: ListenOptions;
  readonly workerWs: WorkerWebSocketServer;
  readonly inFlight: InFlightRequests;
  readonly handle: (req: Request) => Promise<Response>;
  readonly logger: Logger;
}): ListeningServer {
  const { options, workerWs, inFlight, handle, logger } = params;
  const port = options.port ?? 4000;
  const hostname = options.hostname ?? "0.0.0.0";
  const BunGlobal = (globalThis as { Bun?: { serve: typeof Bun.serve } }).Bun;
  if (!BunGlobal) {
    throw new Error("ZoryaServer.listen requires Bun runtime");
  }
  const maxRequestBodySize = options.maxRequestBodySize ?? DEFAULT_MAX_REQUEST_BODY_BYTES;
  const maxWebhookBodySize = options.maxWebhookBodySize ?? DEFAULT_MAX_WEBHOOK_BODY_BYTES;
  const srv = BunGlobal.serve({
    port,
    hostname,
    development: options.development ?? false,
    idleTimeout: options.idleTimeout ?? DEFAULT_IDLE_TIMEOUT_SECONDS,
    // Transport ceiling above the per-path limits so those can answer
    // with a JSON 413 (Bun's own 413 has no body).
    maxRequestBodySize: 2 * Math.max(maxRequestBodySize, maxWebhookBodySize),
    fetch: async (req, server) => {
      inFlight.enter();
      try {
        const upgradeResult = workerWs.upgradeIfWorkerWs({ req, server });
        if (upgradeResult !== undefined) return upgradeResult;
        const url = new URL(req.url);
        if (url.pathname === "/ws/worker") return undefined;
        const limited = await limitRequestBody({
          req,
          maxBytes: bodyLimitFor({ path: url.pathname, maxRequestBodySize, maxWebhookBodySize }),
        });
        if (limited instanceof Response) return limited;
        return await handle(limited);
      } catch (err) {
        return errorResponse({ err, req, logger });
      } finally {
        inFlight.leave();
      }
    },
    error: (err) => errorResponse({ err, logger }),
    websocket: workerWs.websocketHandlers(),
  });
  return {
    port: typeof srv.port === "number" ? srv.port : port,
    hostname: typeof srv.hostname === "string" ? srv.hostname : hostname,
    stop: (closeActiveConnections) => srv.stop(closeActiveConnections),
  };
}
