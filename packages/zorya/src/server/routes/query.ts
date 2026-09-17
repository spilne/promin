// ---------------------------------------------------------------------------
// POST /api/runs/:id/query — invoke a workflow's in-memory query handler.
//
// Body: { name: string, args?: unknown, timeoutMs?: number }
// Response (200): { result }
// Response (404): "no_handler" — workflow not running on any worker, or
//                  no handler with that name registered
// Response (504): "timeout" — no worker replied within the timeout
// Response (500): handler-throw "{ ok: false, error }" surfaced as 500
//
// The route broadcasts a `query` command to every connected worker and
// returns the first `{ hosted: true, result }` reply. Workers not
// hosting the workflow reply `{ hosted: false }` quickly so the server
// can exit early once it has heard from every worker. If none host the
// workflow → 404.
// ---------------------------------------------------------------------------

import type { WorkerWebSocketServer } from "../services/worker-ws-server.ts";
import { json, jsonError, readJson } from "../router.ts";

interface QueryRequestBody {
  name?: string;
  args?: unknown;
  timeoutMs?: number;
}

export function queryRun(workerWs: WorkerWebSocketServer) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const workflowId = params.id;
    if (!workflowId) return jsonError(400, "missing_id");
    const body = (await readJson<QueryRequestBody>(req)) ?? {};
    if (!body.name || typeof body.name !== "string") {
      return jsonError(400, "missing_name", "Body must include `name: string`.");
    }
    const timeoutMs = body.timeoutMs ?? 5_000;

    const workers = workerWs.connectedWorkers();
    if (workers.length === 0) {
      return jsonError(
        404,
        "no_workers",
        "No workers connected — query handlers run inside live workflow bodies.",
      );
    }

    // Race every connected worker. The first `{ hosted: true }` reply
    // wins; `hosted: false` replies are ignored. Errors thrown by the
    // matching worker's handler propagate as 500.
    let resolved = false;
    let result: unknown;
    let error: Error | undefined;
    let notHostedCount = 0;

    await Promise.race([
      new Promise<void>((resolve) => {
        for (const workerId of workers) {
          workerWs
            .request<{ hosted: boolean; result?: unknown }>({
              workerId,
              cmd: "query",
              args: { workflowId, name: body.name, args: body.args },
              timeoutMs,
            })
            .then((reply) => {
              if (resolved) return;
              if (reply.hosted) {
                resolved = true;
                result = reply.result;
                resolve();
                return;
              }
              notHostedCount++;
              if (notHostedCount >= workers.length) resolve();
            })
            .catch((err: Error) => {
              if (resolved) return;
              // Handler-throw surfaces as a worker reply with ok: false,
              // which `request()` rethrows as Error. Keep the first one
              // we see so the response is meaningful when nothing else
              // matches.
              error = err;
              notHostedCount++;
              if (notHostedCount >= workers.length) resolve();
            });
        }
      }),
      new Promise<void>((resolve) =>
        setTimeout(() => {
          if (!resolved) resolve();
        }, timeoutMs),
      ),
    ]);

    if (resolved) {
      return json(200, { result });
    }
    if (notHostedCount >= workers.length) {
      if (error) {
        return jsonError(500, "query_handler_error", error.message);
      }
      return jsonError(
        404,
        "no_handler",
        `No worker hosting "${workflowId}" or no handler "${body.name}" registered.`,
      );
    }
    return jsonError(504, "query_timeout", `Query timed out after ${timeoutMs}ms.`);
  };
}
