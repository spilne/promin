import { principalRef, type Principal, type SecurityContext } from "./security.ts";

const contexts = new WeakMap<Request, SecurityContext>();

/** The derived actor/effective security context attached to this exact request object. */
export function requestSecurityContext(req: Request): SecurityContext | undefined {
  return contexts.get(req);
}

/** Attach derived identity for downstream route handlers; never accept this value from the wire. */
export function setRequestSecurityContext(params: {
  readonly req: Request;
  readonly context: SecurityContext;
}): void {
  contexts.set(params.req, params.context);
}

/** Compatibility accessor for handlers that only need the credential holder. */
export function requestPrincipal(req: Request): Principal | undefined {
  return requestSecurityContext(req)?.actor;
}

/** Attach a self context for callers that do not perform act-for resolution. */
export function setRequestPrincipal(params: { req: Request; principal: Principal }): void {
  setRequestSecurityContext({
    req: params.req,
    context: { actor: params.principal, effective: principalRef(params.principal) },
  });
}
