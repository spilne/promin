import type { Principal } from "./security.ts";

const principals = new WeakMap<Request, Principal>();

/** The authenticated principal attached to this exact request object. */
export function requestPrincipal(req: Request): Principal | undefined {
  return principals.get(req);
}

/** Attach a principal for downstream route handlers without accepting it from the wire. */
export function setRequestPrincipal(params: { req: Request; principal: Principal }): void {
  principals.set(params.req, params.principal);
}
