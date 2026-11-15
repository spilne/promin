import type { Auth } from "../auth.ts";
import type { Logger } from "../server-config.ts";
import {
  OPERATOR_PRINCIPAL,
  WORKER_PRINCIPAL,
  principalRef,
  type Principal,
  type PrincipalRef,
  type SecurityContext,
  type ZoryaSecurity,
} from "./security.ts";

export interface ApiKeySecurityConfig {
  readonly operatorAuth: Auth;
  readonly workerAuth?: Auth;
  readonly logger?: Pick<Logger, "log">;
}

/** Adapt the built-in bearer API-key checkers to the common security contract. */
export function createApiKeySecurity(config: ApiKeySecurityConfig): ZoryaSecurity {
  const principal = (ref: PrincipalRef): Principal | undefined => {
    if (ref.kind === "operator" && ref.id === OPERATOR_PRINCIPAL.id) return OPERATOR_PRINCIPAL;
    if (ref.kind === "worker" && ref.id === WORKER_PRINCIPAL.id) return WORKER_PRINCIPAL;
    return undefined;
  };

  return {
    authenticate: async ({ request, plane }) => {
      if (plane === "operator") {
        return config.operatorAuth.check(request) ? OPERATOR_PRINCIPAL : undefined;
      }
      return config.workerAuth?.check(request) ? WORKER_PRINCIPAL : undefined;
    },
    resolvePrincipal: async (ref) => principal(ref),
    resolveIdentity: async ({ actor, asserted }) => {
      const currentActor = principal(actor);
      if (!currentActor) return undefined;
      if (!asserted || samePrincipal(currentActor, asserted)) {
        return selfSecurityContext(currentActor);
      }
      if (!currentActor.canActFor || !validEffectiveRef(asserted, principal)) return undefined;
      // A key-only deployment has no user directory, so its operator may act
      // for an otherwise unprovisioned identity by explicit reference.
      if (currentActor.kind === "operator") return { actor: currentActor, effective: asserted };
      const effective = principal(asserted);
      return effective ? { actor: currentActor, effective: principalRef(effective) } : undefined;
    },
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async ({ context, permission, resource }) => {
      const currentActor = principal(context.actor);
      if (!currentActor) return { allowed: false, reason: "identity_unknown" };
      if (permission.resource !== resource.kind) {
        return { allowed: false, reason: "permission_denied" };
      }
      if (!samePrincipal(currentActor, context.effective)) {
        if (!currentActor.canActFor) return { allowed: false, reason: "act_for_required" };
        if (!validEffectiveRef(context.effective, principal)) {
          return { allowed: false, reason: "identity_unknown" };
        }
      }
      if (currentActor.kind === "operator") return { allowed: true, reason: "operator_allow" };
      return { allowed: false, reason: "wrong_plane" };
    },
    recordAudit: async (event) => {
      config.logger?.log(JSON.stringify({ component: "zorya.security", ...event }));
    },
  };
}

export function selfSecurityContext(actor: Principal): SecurityContext {
  return { actor, effective: principalRef(actor) };
}

function samePrincipal(left: PrincipalRef, right: PrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function validEffectiveRef(
  ref: PrincipalRef,
  resolve: (ref: PrincipalRef) => Principal | undefined,
): boolean {
  return ref.kind === "operator" || ref.kind === "worker" ? resolve(ref) !== undefined : true;
}
