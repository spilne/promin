import { principalRef, type Principal, type SecurityContext } from "./security.ts";
import type { RunSecurityContext } from "@promin/workflow";

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

/** Stable references safe to persist on a durable run started by this request. */
export function requestRunSecurityContext(req: Request): RunSecurityContext | undefined {
  const context = requestSecurityContext(req);
  if (!context) return undefined;
  const actor = principalRef(context.actor);
  const effectiveKind = context.effective.kind;
  const effectiveId = context.effective.id;
  const same = actor.kind === effectiveKind && actor.id === effectiveId;
  return Object.freeze({
    actor: Object.freeze(actor),
    ...(!same && {
      effective: Object.freeze({
        kind: effectiveKind,
        id: effectiveId,
      }),
    }),
  });
}

/** Attach a self context for callers that do not perform act-for resolution. */
export function setRequestPrincipal(params: { req: Request; principal: Principal }): void {
  setRequestSecurityContext({
    req: params.req,
    context: { actor: params.principal, effective: principalRef(params.principal) },
  });
}
