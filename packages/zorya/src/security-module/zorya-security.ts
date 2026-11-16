import { createHash, timingSafeEqual } from "node:crypto";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { bearerToken } from "../server/auth.ts";
import { jsonError, notFound, type Router } from "../server/router.ts";
import {
  ORG_WIDE_NAMESPACE,
  SECURITY_ACTIONS,
  principalRef,
  snapshotAuthorizationDecision,
  snapshotPrincipal,
  snapshotPrincipalRef,
  snapshotSecurityContext,
  type EffectiveAuthorizationResult,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityAuditSink,
  type SecurityPermission,
  type SecurityResourceKind,
  type SecurityResourceRef,
  type ZoryaSecurity,
} from "../server/security/index.ts";
import type { LoginIdentity, LoginProvider } from "./login-provider.ts";
import type { SecuritySession, SecurityStoreV2, SecurityUser } from "./security-store.ts";
import { authenticateServiceAccountCredential } from "./service-account-credentials.ts";
import {
  clearLoginStateCookie,
  clearSessionCookies,
  createLoginState,
  csrfCookie,
  CSRF_HEADER,
  csrfToken,
  csrfTokenMatches,
  hashCsrfToken,
  hashSessionToken,
  newCsrfToken,
  newSessionToken,
  readLoginState,
  sessionCookie,
  sessionToken,
} from "./sessions.ts";
import { mountAccessRoutes } from "./access-routes.ts";

const DEFAULT_SESSION_TTL_MS = 8 * 60 * 60 * 1_000;
/** Cookie-authenticated SSE clients reconnect and fully reauthorize at least this often. */
export const DEFAULT_SESSION_SSE_MAX_AGE_MS = 30_000;

export interface ServiceAccountConfig {
  readonly id: string;
  readonly token: string;
  readonly canActFor?: boolean;
  readonly accessRoles?: Readonly<Record<string, ReadonlyArray<string>>>;
}

export interface ZoryaSecurityConfig {
  readonly store: SecurityStoreV2;
  readonly loginProviders: ReadonlyArray<LoginProvider>;
  readonly publicBaseUrl: string;
  readonly allowedDomains: ReadonlyArray<string>;
  readonly defaultAccessRole?: string;
  readonly adminEmails?: ReadonlyArray<string>;
  readonly serviceAccounts?: ReadonlyArray<ServiceAccountConfig>;
  /** Host-local pepper for durable service-account credential digests. Defaults to `secret`. */
  readonly serviceAccountCredentialPepper?: string;
  /** Required enterprise audit destination. Security events are never silently discarded. */
  readonly auditSink: SecurityAuditSink;
  readonly secret: string;
  readonly sessionTtlMs?: number;
  /** Maximum lifetime of one cookie-authenticated SSE connection. Default: 30 seconds. */
  readonly sessionSseMaxAgeMs?: number;
  /** Explicit development-only opt-in for non-Secure cookies on an HTTP public URL. */
  readonly allowInsecureHttp?: boolean;
  readonly clock?: WallClock;
}

interface StoredServiceAccount {
  readonly config: ServiceAccountConfig;
  readonly digest: Buffer;
}

/** Build the optional browser-login and service-account security implementation. */
export function createZoryaSecurity(config: ZoryaSecurityConfig): ZoryaSecurity {
  if (config.allowedDomains.length === 0) {
    throw new Error("createZoryaSecurity: allowedDomains must not be empty");
  }
  if (config.loginProviders.length === 0) {
    throw new Error("createZoryaSecurity: loginProviders must not be empty");
  }
  if (config.secret.length < 32) {
    throw new Error("createZoryaSecurity: secret must be at least 32 characters");
  }
  const serviceAccountCredentialPepper = config.serviceAccountCredentialPepper ?? config.secret;
  if (serviceAccountCredentialPepper.length < 32) {
    throw new Error(
      "createZoryaSecurity: serviceAccountCredentialPepper must be at least 32 characters",
    );
  }
  const auditSink = config.auditSink;
  const auditRecord = auditSink?.recordAudit;
  if (!auditSink || typeof auditRecord !== "function") {
    throw new Error("createZoryaSecurity: auditSink is required");
  }
  const recordAudit = auditRecord.bind(auditSink);

  const clock = config.clock ?? SystemWallClock;
  const publicBaseUrl = new URL(config.publicBaseUrl);
  if (publicBaseUrl.protocol !== "https:" && publicBaseUrl.protocol !== "http:") {
    throw new Error("createZoryaSecurity: publicBaseUrl must use http or https");
  }
  const secure = publicBaseUrl.protocol === "https:";
  if (!secure && config.allowInsecureHttp !== true) {
    throw new Error(
      "createZoryaSecurity: an HTTP publicBaseUrl requires allowInsecureHttp: true for development",
    );
  }
  const sessionTtlMs = config.sessionTtlMs ?? DEFAULT_SESSION_TTL_MS;
  if (!Number.isSafeInteger(sessionTtlMs) || sessionTtlMs <= 0) {
    throw new Error("createZoryaSecurity: sessionTtlMs must be a positive integer");
  }
  const sessionSseMaxAgeMs = config.sessionSseMaxAgeMs ?? DEFAULT_SESSION_SSE_MAX_AGE_MS;
  if (!Number.isSafeInteger(sessionSseMaxAgeMs) || sessionSseMaxAgeMs <= 0) {
    throw new Error("createZoryaSecurity: sessionSseMaxAgeMs must be a positive integer");
  }
  const trustedOrigin = publicBaseUrl.origin;
  const domains = new Set(config.allowedDomains.map(normalize));
  if ([...domains].some((domain) => domain.length === 0 || domain.includes("@"))) {
    throw new Error("createZoryaSecurity: allowedDomains must be domain names");
  }
  const admins = new Set((config.adminEmails ?? []).map(normalize));
  const providers = new Map<string, LoginProvider>();
  for (const provider of config.loginProviders) {
    if (!provider.id || providers.has(provider.id)) {
      throw new Error(`createZoryaSecurity: duplicate or empty login provider id "${provider.id}"`);
    }
    providers.set(provider.id, provider);
  }
  const serviceAccounts: StoredServiceAccount[] = [];
  const cookieAuthenticated = new WeakSet<Request>();
  const serviceAccountIds = new Set<string>();
  for (const account of config.serviceAccounts ?? []) {
    if (!account.id || !account.token || serviceAccountIds.has(account.id)) {
      throw new Error(`createZoryaSecurity: invalid or duplicate service account "${account.id}"`);
    }
    serviceAccountIds.add(account.id);
    serviceAccounts.push({ config: account, digest: digest(account.token) });
  }

  const principalForUser = async (user: SecurityUser): Promise<Principal | undefined> => {
    if (user.status !== "active") return undefined;
    const access = await principalAccessFor(config.store, { kind: "user", id: user.id });
    return {
      kind: "user",
      id: user.id,
      email: user.email,
      accessRoles: access.accessRoles,
      ...(access.teams.length > 0 && { teams: access.teams }),
      canActFor: false,
    };
  };

  const principalForServiceAccount = async (id: string): Promise<Principal | undefined> => {
    const durable = await config.store.getServiceAccount(id);
    if (durable) {
      if (durable.status !== "active") return undefined;
      const access = await principalAccessFor(config.store, {
        kind: "service-account",
        id: durable.id,
      });
      return {
        kind: "service-account",
        id: durable.id,
        accessRoles: access.accessRoles,
        ...(access.teams.length > 0 && { teams: access.teams }),
        canActFor: durable.canActFor,
      };
    }
    const matched = serviceAccounts.find((account) => account.config.id === id)?.config;
    if (!matched) return undefined;
    return {
      kind: "service-account",
      id: matched.id,
      accessRoles: matched.accessRoles ?? {},
      canActFor: matched.canActFor ?? false,
    };
  };

  const resolvePrincipal = async (ref: PrincipalRef): Promise<Principal | undefined> => {
    if (ref.kind === "user") {
      const user = await config.store.getUser(ref.id);
      return user ? principalForUser(user) : undefined;
    }
    if (ref.kind === "service-account") return principalForServiceAccount(ref.id);
    return undefined;
  };

  const resolveCurrent = async (
    ref: PrincipalRef,
  ): Promise<
    | { readonly principal: Principal }
    | { readonly reason: "principal_disabled" | "identity_unknown" }
  > => {
    const principal = await resolvePrincipal(ref);
    if (principal) return { principal };
    if (ref.kind === "user") {
      const user = await config.store.getUser(ref.id);
      if (user?.status === "disabled") return { reason: "principal_disabled" };
    }
    if (ref.kind === "service-account") {
      const account = await config.store.getServiceAccount(ref.id);
      if (account?.status === "disabled") return { reason: "principal_disabled" };
    }
    return { reason: "identity_unknown" };
  };

  const authorizeEffective: NonNullable<ZoryaSecurity["authorizeEffective"]> = async ({
    context,
    permission,
    resource,
  }): Promise<EffectiveAuthorizationResult> => {
    const actorResult = await resolveCurrent(context.actor);
    if ("reason" in actorResult)
      return { decision: { allowed: false, reason: actorResult.reason } };
    // Policy and the returned ACL caller consume this same detached snapshot.
    const currentActor = snapshotPrincipal(actorResult.principal);
    if (permission.resource !== resource.kind) {
      return {
        decision: { allowed: false, reason: "permission_denied" },
        effective: currentActor,
      };
    }
    let principal = currentActor;
    if (!samePrincipal(currentActor, context.effective)) {
      if (!currentActor.canActFor) {
        return {
          decision: { allowed: false, reason: "act_for_required" },
          effective: currentActor,
        };
      }
      const effectiveResult = await resolveCurrent(context.effective);
      if ("reason" in effectiveResult) {
        return { decision: { allowed: false, reason: effectiveResult.reason } };
      }
      principal = snapshotPrincipal(effectiveResult.principal);
    }
    const principalAccess = await accessAt(config.store, principal, resource.namespaceId);
    const actorAccess = samePrincipal(currentActor, principal)
      ? principalAccess
      : await accessAt(config.store, currentActor, resource.namespaceId);
    const { teams: _priorTeams, ...principalWithoutTeams } = principal;
    principal = snapshotPrincipal({
      ...principalWithoutTeams,
      accessRoles: { [resource.namespaceId]: principalAccess.accessRoles },
      ...(principalAccess.groups.length > 0 && { teams: principalAccess.groups }),
    });
    const roles = new Set(principalAccess.accessRoles);
    const actorRoles = new Set(actorAccess.accessRoles);
    if (roles.has("admin")) {
      return {
        decision: { allowed: true, reason: "access_role_allow" },
        effective: principal,
      };
    }
    if (roles.has("auditor") && isAuditorPermission(permission)) {
      return {
        decision: { allowed: true, reason: "access_role_allow" },
        effective: principal,
      };
    }
    if (roles.has("agent-manager") && isAgentManagerPermission(permission)) {
      return {
        decision: { allowed: true, reason: "access_role_allow" },
        effective: principal,
      };
    }
    if (
      roles.has("approver") &&
      permission.resource === "approval" &&
      permission.action === "decide-elevated"
    ) {
      return {
        decision: { allowed: true, reason: "access_role_allow" },
        effective: principal,
      };
    }
    if (
      isMemberPermission({
        principal,
        roles,
        actor: currentActor,
        actorRoles,
        permission,
        resource,
      })
    ) {
      return {
        decision: { allowed: true, reason: "self_identity_allow" },
        effective: principal,
      };
    }
    if (permission.resource === "agent" && permission.action === "invoke") {
      return roles.has("member")
        ? {
            decision: { allowed: true, reason: "access_role_allow" },
            effective: principal,
          }
        : {
            decision: { allowed: false, reason: "namespace_denied" },
            effective: principal,
          };
    }
    if (
      permission.resource === "knowledge" &&
      permission.action !== "use" &&
      permission.action !== "inspect-text" &&
      roles.has("knowledge-manager")
    ) {
      return {
        decision: { allowed: true, reason: "access_role_allow" },
        effective: principal,
      };
    }
    if (
      permission.resource === "knowledge" &&
      (permission.action === "use" || permission.action === "inspect-text")
    ) {
      if (!resource.id) {
        return {
          decision: { allowed: false, reason: "permission_denied" },
          effective: principal,
        };
      }
      const grants = await config.store.listKnowledgeAudienceGrants({
        namespaceId: resource.namespaceId,
        baseId: resource.id,
      });
      const groups = new Set(principalAccess.groups);
      return grants.some((grant) =>
        grant.audience.kind === "access-role"
          ? roles.has(grant.audience.id)
          : groups.has(grant.audience.id),
      )
        ? {
            decision: { allowed: true, reason: "knowledge_grant_allow" },
            effective: principal,
          }
        : {
            decision: { allowed: false, reason: "permission_denied" },
            effective: principal,
          };
    }
    return {
      decision: { allowed: false, reason: "permission_denied" },
      effective: principal,
    };
  };

  return {
    authenticate: async ({ request: req, plane }) => {
      if (plane !== "operator") return undefined;
      const cookieToken = sessionToken(req.headers.get("cookie"), secure);
      // A presented browser session is authoritative. Invalid or stale
      // sessions fail closed instead of falling through to bearer auth.
      if (cookieToken.kind !== "absent") {
        if (
          cookieToken.kind !== "value" ||
          cookieToken.value.length === 0 ||
          cookieToken.value.length > 256
        ) {
          return undefined;
        }
        const session = await config.store.getSessionRecord(hashSessionToken(cookieToken.value));
        if (!session || session.authMethod !== "oidc" || session.principal.kind !== "user") {
          return undefined;
        }
        if (!validCookieRequest({ request: req, session, secure, trustedOrigin })) return undefined;
        const principal = await resolvePrincipal(session.principal);
        if (!principal) return undefined;
        cookieAuthenticated.add(req);
        return principal;
      }

      const token = bearerToken(req);
      if (!token) return undefined;
      const durable = await authenticateServiceAccountCredential({
        store: config.store,
        token,
        pepper: serviceAccountCredentialPepper,
        clock,
      });
      if (durable) return principalForServiceAccount(durable.id);
      const presented = digest(token);
      let matched: ServiceAccountConfig | undefined;
      for (const account of serviceAccounts) {
        const equal = timingSafeEqual(presented, account.digest);
        if (equal) matched = account.config;
      }
      if (!matched) return undefined;
      return principalForServiceAccount(matched.id);
    },
    secureResponse: ({ request, response }) =>
      cookieAuthenticated.has(request)
        ? limitEventStreamLifetime({ response, clock, maxAgeMs: sessionSseMaxAgeMs })
        : response,
    resolvePrincipal,
    resolveIdentity: async ({ actor, asserted }) => {
      const actorResult = await resolveCurrent(actor);
      if ("reason" in actorResult) return undefined;
      const currentActor = actorResult.principal;
      if (!asserted || samePrincipal(currentActor, asserted)) {
        return { actor: currentActor, effective: principalRef(currentActor) };
      }
      if (!currentActor.canActFor) return undefined;
      const effective = await resolvePrincipal(asserted);
      return effective ? { actor: currentActor, effective: principalRef(effective) } : undefined;
    },
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async (request) => (await authorizeEffective(request)).decision,
    authorizeEffective,
    recordAudit: async (event) => recordAudit(snapshotAuditEvent(event)),
    mount: (router) => {
      mountLoginRoutes({
        router,
        config,
        providers,
        domains,
        admins,
        clock,
        secure,
        sessionTtlMs,
        trustedOrigin,
      });
      mountAccessRoutes({
        router,
        store: config.store,
        credentialPepper: serviceAccountCredentialPepper,
        clock,
      });
    },
  };
}

function snapshotAuditEvent(source: SecurityAuditEvent): SecurityAuditEvent {
  if (!source || typeof source !== "object") throw new TypeError("Invalid security audit event");
  const record = source as unknown as Record<string, unknown>;
  const event = record.event;
  const occurredAt = record.occurredAt;
  const namespaceId = record.namespaceId;
  const contextSource = record.context;
  const resourceSource = record.resource;
  const permissionSource = record.permission;
  const decisionSource = record.decision;
  const metadataSource = record.metadata;
  if (
    typeof event !== "string" ||
    event.length === 0 ||
    typeof occurredAt !== "string" ||
    occurredAt.length === 0 ||
    typeof namespaceId !== "string" ||
    namespaceId.length === 0
  ) {
    throw new TypeError("Invalid security audit event");
  }
  const context =
    contextSource === undefined
      ? undefined
      : snapshotSecurityContext(contextSource as NonNullable<SecurityAuditEvent["context"]>);
  const resource = resourceSource === undefined ? undefined : snapshotAuditResource(resourceSource);
  const permission =
    permissionSource === undefined ? undefined : snapshotAuditPermission(permissionSource);
  if (permission !== undefined && resource !== undefined && permission.resource !== resource.kind) {
    throw new TypeError("Security audit permission and resource do not match");
  }
  const decision =
    decisionSource === undefined
      ? undefined
      : snapshotAuthorizationDecision(
          decisionSource as NonNullable<SecurityAuditEvent["decision"]>,
        );
  const metadata = snapshotAuditMetadata(metadataSource);
  return Object.freeze({
    event,
    occurredAt,
    ...(context !== undefined && { context }),
    namespaceId,
    ...(resource !== undefined && { resource }),
    ...(permission !== undefined && { permission }),
    ...(decision !== undefined && { decision }),
    ...(metadata !== undefined && { metadata }),
  });
}

function snapshotAuditPermission(source: unknown): SecurityPermission {
  if (!source || typeof source !== "object") {
    throw new TypeError("Invalid security audit permission");
  }
  const record = source as Record<string, unknown>;
  const resource = record.resource;
  const action = record.action;
  if (
    typeof resource !== "string" ||
    !Object.hasOwn(SECURITY_ACTIONS, resource) ||
    typeof action !== "string" ||
    !(SECURITY_ACTIONS[resource as SecurityResourceKind] as readonly string[]).includes(action)
  ) {
    throw new TypeError("Invalid security audit permission");
  }
  return Object.freeze({ resource, action }) as SecurityPermission;
}

function snapshotAuditResource(source: unknown): SecurityResourceRef {
  if (!source || typeof source !== "object") {
    throw new TypeError("Invalid security audit resource");
  }
  const record = source as Record<string, unknown>;
  const kind = record.kind;
  const namespaceId = record.namespaceId;
  const id = record.id;
  const principalSource = record.principal;
  if (
    typeof kind !== "string" ||
    !Object.hasOwn(SECURITY_ACTIONS, kind) ||
    typeof namespaceId !== "string" ||
    namespaceId.length === 0 ||
    (id !== undefined && (typeof id !== "string" || id.length === 0))
  ) {
    throw new TypeError("Invalid security audit resource");
  }
  const principal =
    principalSource === undefined
      ? undefined
      : snapshotPrincipalRef(principalSource as PrincipalRef);
  return Object.freeze({
    kind: kind as SecurityResourceKind,
    namespaceId,
    ...(id !== undefined && { id }),
    ...(principal !== undefined && { principal }),
  }) as SecurityResourceRef;
}

function snapshotAuditMetadata(
  source: unknown,
): Readonly<Record<string, string | number | boolean | null>> | undefined {
  if (source === undefined) return undefined;
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    throw new TypeError("Invalid security audit metadata");
  }
  const result = Object.create(null) as Record<string, string | number | boolean | null>;
  for (const key of Object.keys(source)) {
    const value = (source as Record<string, unknown>)[key];
    if (
      value !== null &&
      typeof value !== "string" &&
      typeof value !== "number" &&
      typeof value !== "boolean"
    ) {
      throw new TypeError("Invalid security audit metadata");
    }
    result[key] = value;
  }
  return Object.freeze(result);
}

/** Configuration authority only; runtime actions require their own role. */
const AGENT_MANAGER_ACTIONS: Readonly<Record<string, ReadonlyArray<string>>> = Object.freeze({
  agent: Object.freeze(["read", "create", "update", "promote", "rollback", "disable"]),
  "agent-role": Object.freeze(["read", "create", "update", "disable"]),
  skill: Object.freeze(["read", "create", "update", "disable"]),
  fragment: Object.freeze(["read", "create", "update", "disable"]),
  toolset: Object.freeze(["read", "create", "promote", "rollback"]),
  eval: Object.freeze(["read", "create"]),
  schedule: Object.freeze(["read", "create", "update", "delete"]),
  workflow: Object.freeze(["read", "create", "update"]),
  dag: Object.freeze(["read", "create", "update"]),
});

const AUDITOR_CONFIG_RESOURCES = new Set([
  "namespace",
  "user",
  "service-account",
  "agent",
  "agent-role",
  "skill",
  "fragment",
  "toolset",
  "knowledge",
  "connector",
  "workflow",
  "dag",
  "schedule",
  "eval",
  "webhook",
  "worker",
]);

function isAgentManagerPermission(permission: {
  readonly resource: string;
  readonly action: string;
}): boolean {
  if (!Object.hasOwn(AGENT_MANAGER_ACTIONS, permission.resource)) return false;
  return AGENT_MANAGER_ACTIONS[permission.resource]?.includes(permission.action) === true;
}

function isAuditorPermission(permission: {
  readonly resource: string;
  readonly action: string;
}): boolean {
  if (permission.resource === "audit")
    return permission.action === "search" || permission.action === "export";
  if (permission.resource === "thread") return permission.action === "view-trace";
  return permission.action === "read" && AUDITOR_CONFIG_RESOURCES.has(permission.resource);
}

function isMemberPermission(params: {
  readonly principal: Principal;
  readonly roles: ReadonlySet<string>;
  readonly actor: Principal;
  readonly actorRoles: ReadonlySet<string>;
  readonly permission: { readonly resource: string; readonly action: string };
  readonly resource: { readonly id?: string; readonly principal?: PrincipalRef };
}): boolean {
  const { principal, roles, actor, actorRoles, permission, resource } = params;
  if (permission.resource === "approval" && permission.action === "decide") {
    return (
      actorRoles.has("member") &&
      resource.principal !== undefined &&
      samePrincipal(resource.principal, actor)
    );
  }
  if (!roles.has("member")) return false;
  if (permission.resource === "agent" && permission.action === "read") return true;
  if (permission.resource === "thread") {
    return (
      permission.action === "read" ||
      permission.action === "send" ||
      permission.action === "rename" ||
      permission.action === "archive"
    );
  }
  if (permission.resource === "run" && permission.action === "trigger") return true;
  if (permission.resource === "memory") {
    return resource.id === principal.id;
  }
  return false;
}

function samePrincipal(left: PrincipalRef, right: PrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

async function principalAccessFor(
  store: SecurityStoreV2,
  subject: { readonly kind: "user" | "service-account"; readonly id: string },
): Promise<{
  readonly accessRoles: Readonly<Record<string, ReadonlyArray<string>>>;
  readonly teams: ReadonlyArray<string>;
}> {
  const groups = await store.listGroupsForSubject(subject);
  const bindings = [
    ...(await store.listSubjectBindings(subject)),
    ...(
      await Promise.all(
        groups.map((group) => store.listSubjectBindings({ kind: "group", id: group.id })),
      )
    ).flat(),
  ];
  const roles = Object.create(null) as Record<string, string[]>;
  for (const binding of bindings) {
    const namespaceRoles = (roles[binding.namespaceId] ??= []);
    if (!namespaceRoles.includes(binding.accessRole)) namespaceRoles.push(binding.accessRole);
  }
  return { accessRoles: roles, teams: groups.map((group) => group.id) };
}

async function accessAt(
  store: SecurityStoreV2,
  principal: Principal,
  namespaceId: string,
): Promise<{
  readonly accessRoles: ReadonlyArray<string>;
  readonly groups: ReadonlyArray<string>;
}> {
  if (principal.kind === "user") {
    return store.resolveAccess({ subject: { kind: "user", id: principal.id }, namespaceId });
  }
  if (principal.kind === "service-account" && (await store.getServiceAccount(principal.id))) {
    return store.resolveAccess({
      subject: { kind: "service-account", id: principal.id },
      namespaceId,
    });
  }
  return { accessRoles: [...rolesFor(principal, namespaceId)], groups: principal.teams ?? [] };
}

function rolesFor(principal: Principal, namespaceId: string): Set<string> {
  return new Set([
    ...ownRoles(principal.accessRoles, ORG_WIDE_NAMESPACE),
    ...ownRoles(principal.accessRoles, namespaceId),
  ]);
}

function ownRoles(
  accessRoles: Principal["accessRoles"],
  namespaceId: string,
): ReadonlyArray<string> {
  return Object.hasOwn(accessRoles, namespaceId) ? (accessRoles[namespaceId] ?? []) : [];
}

function mountLoginRoutes(params: {
  readonly router: Router;
  readonly config: ZoryaSecurityConfig;
  readonly providers: ReadonlyMap<string, LoginProvider>;
  readonly domains: ReadonlySet<string>;
  readonly admins: ReadonlySet<string>;
  readonly clock: WallClock;
  readonly secure: boolean;
  readonly sessionTtlMs: number;
  readonly trustedOrigin: string;
}): void {
  params.router.route({
    method: "GET",
    path: "/auth/login/:provider",
    access: { kind: "public", purpose: "login" },
    handler: async (_req, route) => {
      const providerId = route.provider;
      const provider = providerId ? params.providers.get(providerId) : undefined;
      if (!provider) return notFound("provider_not_found");
      const login = createLoginState({
        secret: params.config.secret,
        clock: params.clock,
        secure: params.secure,
      });
      const redirectUri = callbackUrl(params.config.publicBaseUrl, provider.id);
      const location = await provider.authorizationUrl({
        state: login.state,
        codeChallenge: login.codeChallenge,
        redirectUri,
      });
      return redirect(location, [login.cookie]);
    },
  });
  params.router.route({
    method: "GET",
    path: "/auth/callback/:provider",
    access: { kind: "public", purpose: "login" },
    handler: async (req, route) => {
      const providerId = route.provider;
      const provider = providerId ? params.providers.get(providerId) : undefined;
      if (!provider) return notFound("provider_not_found");
      const url = new URL(req.url);
      const state = readLoginState({
        cookieHeader: req.headers.get("cookie"),
        secret: params.config.secret,
        clock: params.clock,
      });
      if (!state || url.searchParams.get("state") !== state.state) {
        return jsonError(400, "invalid_state");
      }
      const code = url.searchParams.get("code");
      if (!code) return jsonError(400, "invalid_callback");

      let identity: LoginIdentity;
      try {
        identity = await provider.exchange({
          code,
          codeVerifier: state.codeVerifier,
          redirectUri: callbackUrl(params.config.publicBaseUrl, provider.id),
        });
      } catch {
        return jsonError(401, "invalid_identity_token");
      }
      if (!identity.emailVerified) return jsonError(403, "email_not_verified");
      const email = normalize(identity.email);
      const domain = emailDomain(email);
      if (!domain || !params.domains.has(domain)) return jsonError(403, "domain_not_allowed");

      const accessRole = params.admins.has(email)
        ? "admin"
        : (params.config.defaultAccessRole ?? "member");
      const result = await params.config.store.findOrCreateProviderUser({
        identity: {
          providerId: provider.id,
          issuer: identity.issuer ?? `provider:${provider.id}`,
          subject: identity.subject,
        },
        email,
        bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole }],
      });
      if (result.user.status !== "active") return jsonError(403, "user_disabled");

      const token = newSessionToken();
      const csrf = newCsrfToken();
      const createdAt = params.clock.currentTimeMs();
      await params.config.store.putSession({
        tokenHash: hashSessionToken(token),
        principal: { kind: "user", id: result.user.id },
        createdAt,
        expiresAt: createdAt + params.sessionTtlMs,
        csrfTokenHash: hashCsrfToken(csrf),
        authMethod: "oidc",
      });
      return redirect(new URL("/", params.config.publicBaseUrl).toString(), [
        clearLoginStateCookie(params.secure),
        sessionCookie({ token, ttlMs: params.sessionTtlMs, secure: params.secure }),
        csrfCookie({ token: csrf, ttlMs: params.sessionTtlMs, secure: params.secure }),
      ]);
    },
  });
  params.router.route({
    method: "POST",
    path: "/auth/logout",
    access: { kind: "public", purpose: "login" },
    handler: async (req) => {
      const token = sessionToken(req.headers.get("cookie"), params.secure);
      if (token.kind !== "value" || token.value.length === 0 || token.value.length > 256) {
        return jsonError(401, "unauthorized");
      }
      const tokenHash = hashSessionToken(token.value);
      const session = await params.config.store.getSessionRecord(tokenHash);
      if (
        !session ||
        session.authMethod !== "oidc" ||
        !validCookieRequest({
          request: req,
          session,
          secure: params.secure,
          trustedOrigin: params.trustedOrigin,
        })
      ) {
        return jsonError(403, "csrf_failed");
      }
      await params.config.store.revokeSession({
        tokenHash,
        revokedAt: params.clock.currentTimeMs(),
      });
      const headers = new Headers();
      for (const value of clearSessionCookies(params.secure)) headers.append("set-cookie", value);
      return new Response(null, {
        status: 204,
        headers,
      });
    },
  });
}

function redirect(location: string, cookies: ReadonlyArray<string>): Response {
  const headers = new Headers({ location });
  for (const value of cookies) headers.append("set-cookie", value);
  return new Response(null, { status: 302, headers });
}

function callbackUrl(base: string, providerId: string): string {
  return new URL(`/auth/callback/${encodeURIComponent(providerId)}`, base).toString();
}

function emailDomain(email: string): string | undefined {
  const at = email.lastIndexOf("@");
  if (at <= 0 || at === email.length - 1) return undefined;
  return email.slice(at + 1);
}

function normalize(value: string): string {
  return value.toLowerCase();
}

function validCookieRequest(params: {
  readonly request: Request;
  readonly session: SecuritySession;
  readonly secure: boolean;
  readonly trustedOrigin: string;
}): boolean {
  if (isSafeMethod(params.request.method)) return true;
  if (params.request.headers.get("origin") !== params.trustedOrigin) return false;
  const fromCookie = csrfToken(params.request.headers.get("cookie"), params.secure);
  if (fromCookie.kind !== "value" || fromCookie.value.length === 0) return false;
  const fromHeader = params.request.headers.get(CSRF_HEADER);
  if (!fromHeader) return false;
  return (
    csrfTokenMatches(fromCookie.value, params.session.csrfTokenHash) &&
    csrfTokenMatches(fromHeader, params.session.csrfTokenHash)
  );
}

function isSafeMethod(method: string): boolean {
  return method === "GET" || method === "HEAD" || method === "OPTIONS" || method === "TRACE";
}

function limitEventStreamLifetime(params: {
  readonly response: Response;
  readonly clock: WallClock;
  readonly maxAgeMs: number;
}): Response {
  if (!(params.response.headers.get("content-type") ?? "").startsWith("text/event-stream")) {
    return params.response;
  }
  const source = params.response.body;
  if (!source) return params.response;
  const reader = source.getReader();
  let closed = false;
  let timer: ReturnType<WallClock["setTimeout"]> | undefined;
  const body = new ReadableStream<Uint8Array>({
    start: (controller) => {
      timer = params.clock.setTimeout(() => {
        if (closed) return;
        closed = true;
        void reader.cancel("session_revalidation_required").catch(() => {});
        try {
          controller.close();
        } catch {
          // The source or client may have ended the stream concurrently.
        }
      }, params.maxAgeMs);
      timer.unref?.();
    },
    pull: async (controller) => {
      if (closed) return;
      try {
        const { value, done } = await reader.read();
        if (closed) return;
        if (done) {
          closed = true;
          timer?.clear();
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        if (closed) return;
        closed = true;
        timer?.clear();
        controller.error(error);
      }
    },
    cancel: async (reason) => {
      if (!closed) {
        closed = true;
        timer?.clear();
      }
      await reader.cancel(reason).catch(() => {});
    },
  });
  return new Response(body, {
    status: params.response.status,
    statusText: params.response.statusText,
    headers: params.response.headers,
  });
}

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}
