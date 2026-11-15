import type { Router } from "../router.ts";

/** Namespace whose access roles apply across every namespace in one deployment. */
export const ORG_WIDE_NAMESPACE = "org";

/** The authenticated identity attached to one request. */
export interface Principal {
  readonly kind: "operator" | "user" | "service-account";
  readonly id: string;
  readonly email?: string;
  /** Access roles keyed by namespace id; `org` applies in every namespace. */
  readonly accessRoles: Readonly<Record<string, ReadonlyArray<string>>>;
  /** Whether this principal may start work on behalf of a user. */
  readonly canActFor: boolean;
}

/** Authentication and coarse access decisions supplied to a Zorya server. */
export interface ZoryaSecurity {
  /** Resolve the request's caller, or return undefined for invalid credentials. */
  authenticate(req: Request): Promise<Principal | undefined>;
  /** Resolve a user principal for a service account acting on that user's behalf. */
  principalById(id: string): Promise<Principal | undefined>;
  /** Whether the principal may act in the namespace. */
  canActIn(params: { principal: Principal; namespaceId: string }): Promise<boolean>;
  /** Whether the principal may use a knowledge base in the namespace. */
  canUseKnowledge(params: {
    principal: Principal;
    namespaceId: string;
    baseId: string;
  }): Promise<boolean>;
  /** Mount implementation-specific routes such as login and administration. */
  mount?(router: Router): void;
  /** Start implementation-specific background work. */
  start?(): Promise<void>;
  /** Stop implementation-specific background work. */
  stop?(): Promise<void>;
}

/** The all-powerful principal produced by the built-in API-key security adapter. */
export const OPERATOR_PRINCIPAL: Principal = Object.freeze({
  kind: "operator",
  id: "operator",
  accessRoles: Object.freeze({ [ORG_WIDE_NAMESPACE]: Object.freeze(["admin"]) }),
  canActFor: true,
});
