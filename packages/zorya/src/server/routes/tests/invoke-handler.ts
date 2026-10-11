// ---------------------------------------------------------------------------
// Call a route handler the way the router does: a thrown `HttpError`
// (request validation, 404 guards) becomes its JSON response. For unit
// tests that exercise one handler without a ZoryaServer.
// ---------------------------------------------------------------------------

import { errorResponse, type Handler } from "../../router.ts";

const silent = { error: () => {} };

export async function invoke(
  handler: Handler,
  req: Request,
  params: Record<string, string> = {},
): Promise<Response> {
  try {
    return await handler(req, params);
  } catch (err) {
    return errorResponse({ err, req, logger: silent });
  }
}
