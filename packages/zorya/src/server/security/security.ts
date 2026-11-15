import type { Router } from "../router.ts";

/** Namespace whose access roles apply across every namespace in one deployment. */
export const ORG_WIDE_NAMESPACE = "org";

/** Authentication planes have distinct credentials and authority. */
export type SecurityPlane = "operator" | "worker";

export type PrincipalKind = "operator" | "worker" | "user" | "service-account";

/** Stable, serializable reference to an identity. */
export interface PrincipalRef {
  readonly kind: PrincipalKind;
  readonly id: string;
}

/** The authenticated identity attached to one request. */
export interface Principal extends PrincipalRef {
  readonly email?: string;
  /** Access roles keyed by namespace id; `org` applies in every namespace. */
  readonly accessRoles: Readonly<Record<string, ReadonlyArray<string>>>;
  /** Whether this principal may start work on behalf of another identity. */
  readonly canActFor: boolean;
}

/** The credential holder remains visible when work is performed for somebody else. */
export interface SecurityContext {
  readonly actor: Principal;
  readonly effective: PrincipalRef;
}

/** Approved resource/action vocabulary. Kept as immutable data for conformance checks. */
export const SECURITY_ACTIONS = deepFreezeActions({
  namespace: ["read", "create", "update", "archive"],
  user: ["read", "disable", "bind"],
  "service-account": ["read", "create", "rotate", "delete"],
  agent: ["read", "create", "update", "promote", "rollback", "disable", "invoke"],
  "agent-role": ["read", "create", "update", "disable"],
  skill: ["read", "create", "update", "disable"],
  fragment: ["read", "create", "update", "disable"],
  toolset: ["read", "create", "promote", "rollback"],
  thread: ["read", "send", "rename", "archive", "view-trace"],
  memory: ["read", "write", "erase"],
  approval: ["decide", "decide-elevated"],
  knowledge: ["read", "create", "update", "delete", "grant", "use", "inspect-text"],
  connector: ["read", "register", "approve-tools", "disable", "remove"],
  secret: ["list", "write", "delete"],
  workflow: ["read", "create", "update"],
  dag: ["read", "create", "update"],
  run: ["read", "trigger", "cancel", "signal"],
  schedule: ["read", "create", "update", "delete"],
  eval: ["read", "create", "run"],
  webhook: ["read", "configure"],
  worker: ["read", "drain"],
  audit: ["search", "export"],
  usage: ["read"],
} as const);

export type SecurityResourceKind = keyof typeof SECURITY_ACTIONS;
export type SecurityAction<K extends SecurityResourceKind> = (typeof SECURITY_ACTIONS)[K][number];
export type SecurityPermissionFor<K extends SecurityResourceKind> = {
  readonly resource: K;
  readonly action: SecurityAction<K>;
};
export type SecurityPermission = {
  readonly [K in SecurityResourceKind]: SecurityPermissionFor<K>;
}[SecurityResourceKind];

/** Resource under an authorization check. Namespace is mandatory, including for org resources. */
export interface SecurityResourceRef<K extends SecurityResourceKind = SecurityResourceKind> {
  readonly kind: K;
  readonly namespaceId: string;
  readonly id?: string;
}

/** Stable, audit-safe authorization reason codes. */
export type AuthorizationReason =
  | "operator_allow"
  | "access_role_allow"
  | "knowledge_grant_allow"
  | "self_identity_allow"
  | "permission_denied"
  | "namespace_denied"
  | "principal_disabled"
  | "identity_unknown"
  | "act_for_required"
  | "wrong_plane";

export type AuthorizationDecision =
  | { readonly allowed: true; readonly reason: AuthorizationReason }
  | { readonly allowed: false; readonly reason: AuthorizationReason };

/** Backends may receive either namespace-only scope or a narrower identity allow-list. */
export type IdentityFilter =
  | { readonly kind: "namespace"; readonly namespaceId: string }
  | {
      readonly kind: "identities";
      readonly namespaceId: string;
      readonly identities: ReadonlyArray<PrincipalRef>;
    };

export interface AuthenticationRequest {
  readonly request: Request;
  readonly plane: SecurityPlane;
}

export interface AuthorizationRequestFor<K extends SecurityResourceKind> {
  readonly context: SecurityContext;
  readonly permission: SecurityPermissionFor<K>;
  readonly resource: SecurityResourceRef<K>;
}

/** Correlated permission/resource request; no union inference can pair different kinds. */
export type AuthorizationRequest = {
  readonly [K in SecurityResourceKind]: AuthorizationRequestFor<K>;
}[SecurityResourceKind];

export interface ResolveIdentityRequest {
  readonly actor: Principal;
  readonly asserted?: PrincipalRef;
}

export interface IdentityFilterRequest {
  readonly context: SecurityContext;
  readonly namespaceId: string;
}

/** Dependency-free audit record accepted by core and optional durable sinks. */
export interface SecurityAuditEvent {
  readonly event: string;
  readonly occurredAt: string;
  readonly context: SecurityContext;
  readonly namespaceId: string;
  readonly resource?: SecurityResourceRef;
  readonly permission?: SecurityPermission;
  readonly decision?: AuthorizationDecision;
  readonly metadata?: Readonly<Record<string, string | number | boolean | null>>;
}

export interface RequestAuthenticator {
  authenticate(request: AuthenticationRequest): Promise<Principal | undefined>;
}

export interface Authorizer {
  authorize(request: AuthorizationRequest): Promise<AuthorizationDecision>;
}

export interface IdentityResolver {
  resolvePrincipal(ref: PrincipalRef): Promise<Principal | undefined>;
  resolveIdentity(request: ResolveIdentityRequest): Promise<SecurityContext | undefined>;
  identityFilter(request: IdentityFilterRequest): Promise<IdentityFilter>;
}

export interface SecurityAuditSink {
  recordAudit(event: SecurityAuditEvent): Promise<void>;
}

export interface DashboardLoginIdentity {
  readonly subject: string;
  readonly email: string;
  readonly emailVerified: boolean;
}

/** Login provider contract lives in core; providers do not pull the optional module into root. */
export interface DashboardLoginProvider {
  readonly id: string;
  authorizationUrl(params: {
    readonly state: string;
    readonly codeChallenge: string;
    readonly redirectUri: string;
  }): Promise<string>;
  exchange(params: {
    readonly code: string;
    readonly codeVerifier: string;
    readonly redirectUri: string;
  }): Promise<DashboardLoginIdentity>;
}

/** The single security door used by a Zorya server. */
export interface ZoryaSecurity
  extends RequestAuthenticator, Authorizer, IdentityResolver, SecurityAuditSink {
  /** Mount implementation-specific routes such as login and administration. */
  mount?(router: Router): void;
  /** Start implementation-specific background work. */
  start?(): Promise<void>;
  /** Stop implementation-specific background work. */
  stop?(): Promise<void>;
}

export function principalRef(principal: Principal): PrincipalRef {
  return { kind: principal.kind, id: principal.id };
}

/** The all-powerful principal produced by the built-in operator API-key adapter. */
export const OPERATOR_PRINCIPAL: Principal = Object.freeze({
  kind: "operator",
  id: "operator",
  accessRoles: Object.freeze({ [ORG_WIDE_NAMESPACE]: Object.freeze(["admin"]) }),
  canActFor: true,
});

/** Worker credentials never inherit operator permissions. */
export const WORKER_PRINCIPAL: Principal = Object.freeze({
  kind: "worker",
  id: "worker",
  accessRoles: Object.freeze({}),
  canActFor: false,
});

function deepFreezeActions<const T extends Readonly<Record<string, ReadonlyArray<string>>>>(
  actions: T,
): T {
  for (const values of Object.values(actions)) Object.freeze(values);
  return Object.freeze(actions) as T;
}
