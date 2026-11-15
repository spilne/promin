import { createHash, timingSafeEqual } from "node:crypto";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { bearerToken } from "../server/auth.ts";
import { jsonError, notFound, type Router } from "../server/router.ts";
import {
  ORG_WIDE_NAMESPACE,
  principalRef,
  type Principal,
  type PrincipalRef,
  type ZoryaSecurity,
} from "../server/security/index.ts";
import type { LoginIdentity, LoginProvider } from "./login-provider.ts";
import type { SecurityStore, SecurityUser } from "./security-store.ts";
import {
  clearLoginStateCookie,
  clearSessionCookie,
  createLoginState,
  hashSessionToken,
  newSessionToken,
  readLoginState,
  sessionCookie,
  sessionToken,
} from "./sessions.ts";
import { mountAccessRoutes } from "./access-routes.ts";

const DEFAULT_SESSION_TTL_MS = 8 * 60 * 60 * 1_000;

export interface ServiceAccountConfig {
  readonly id: string;
  readonly token: string;
  readonly canActFor?: boolean;
  readonly accessRoles?: Readonly<Record<string, ReadonlyArray<string>>>;
}

export interface ZoryaSecurityConfig {
  readonly store: SecurityStore;
  readonly loginProviders: ReadonlyArray<LoginProvider>;
  readonly publicBaseUrl: string;
  readonly allowedDomains: ReadonlyArray<string>;
  readonly defaultAccessRole?: string;
  readonly adminEmails?: ReadonlyArray<string>;
  readonly serviceAccounts?: ReadonlyArray<ServiceAccountConfig>;
  readonly secret: string;
  readonly sessionTtlMs?: number;
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

  const clock = config.clock ?? SystemWallClock;
  const publicBaseUrl = new URL(config.publicBaseUrl);
  const secure = publicBaseUrl.protocol === "https:";
  const sessionTtlMs = config.sessionTtlMs ?? DEFAULT_SESSION_TTL_MS;
  if (!Number.isFinite(sessionTtlMs) || sessionTtlMs <= 0) {
    throw new Error("createZoryaSecurity: sessionTtlMs must be positive");
  }
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
    const accessRoles = Object.create(null) as Record<string, string[]>;
    for (const binding of await config.store.listBindings(user.id)) {
      (accessRoles[binding.namespaceId] ??= []).push(binding.accessRole);
    }
    return {
      kind: "user",
      id: user.id,
      email: user.email,
      accessRoles,
      canActFor: false,
    };
  };

  const principalForServiceAccount = (id: string): Principal | undefined => {
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
    return { reason: "identity_unknown" };
  };

  return {
    authenticate: async ({ request: req, plane }) => {
      if (plane !== "operator") return undefined;
      const cookieToken = sessionToken(req.headers.get("cookie"));
      // A presented browser session is authoritative. Invalid or stale
      // sessions fail closed instead of falling through to bearer auth.
      if (cookieToken !== undefined) {
        const session = await config.store.getSession(hashSessionToken(cookieToken));
        if (!session) return undefined;
        const user = await config.store.getUser(session.userId);
        return user ? principalForUser(user) : undefined;
      }

      const token = bearerToken(req);
      if (!token) return undefined;
      const presented = digest(token);
      let matched: ServiceAccountConfig | undefined;
      for (const account of serviceAccounts) {
        const equal = timingSafeEqual(presented, account.digest);
        if (equal) matched = account.config;
      }
      if (!matched) return undefined;
      return principalForServiceAccount(matched.id);
    },
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
    authorize: async ({ context, permission, resource }) => {
      const actorResult = await resolveCurrent(context.actor);
      if ("reason" in actorResult) return { allowed: false, reason: actorResult.reason };
      const currentActor = actorResult.principal;
      if (permission.resource !== resource.kind) {
        return { allowed: false, reason: "permission_denied" };
      }
      let principal = currentActor;
      if (!samePrincipal(currentActor, context.effective)) {
        if (!currentActor.canActFor) {
          return { allowed: false, reason: "act_for_required" };
        }
        const effectiveResult = await resolveCurrent(context.effective);
        if ("reason" in effectiveResult) {
          return { allowed: false, reason: effectiveResult.reason };
        }
        principal = effectiveResult.principal;
      }
      const roles = rolesFor(principal, resource.namespaceId);
      if (roles.has("admin")) return { allowed: true, reason: "access_role_allow" };
      if (roles.has("auditor") && isAuditorPermission(permission)) {
        return { allowed: true, reason: "access_role_allow" };
      }
      if (roles.has("agent-manager") && isAgentManagerPermission(permission)) {
        return { allowed: true, reason: "access_role_allow" };
      }
      if (
        roles.has("approver") &&
        permission.resource === "approval" &&
        permission.action === "decide-elevated"
      ) {
        return { allowed: true, reason: "access_role_allow" };
      }
      if (isMemberPermission({ principal, roles, permission, resource })) {
        return { allowed: true, reason: "self_identity_allow" };
      }
      if (permission.resource === "agent" && permission.action === "invoke") {
        return roles.has("member")
          ? { allowed: true, reason: "access_role_allow" }
          : { allowed: false, reason: "namespace_denied" };
      }
      if (
        permission.resource === "knowledge" &&
        permission.action !== "use" &&
        permission.action !== "inspect-text" &&
        roles.has("knowledge-manager")
      ) {
        return { allowed: true, reason: "access_role_allow" };
      }
      if (
        permission.resource === "knowledge" &&
        (permission.action === "use" || permission.action === "inspect-text")
      ) {
        if (!resource.id) return { allowed: false, reason: "permission_denied" };
        const grants = await config.store.listKnowledgeGrants({
          namespaceId: resource.namespaceId,
          baseId: resource.id,
        });
        return grants.some((grant) => roles.has(grant.accessRole))
          ? { allowed: true, reason: "knowledge_grant_allow" }
          : { allowed: false, reason: "permission_denied" };
      }
      return { allowed: false, reason: "permission_denied" };
    },
    recordAudit: async () => {},
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
      });
      mountAccessRoutes({ router, store: config.store });
    },
  };
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
  readonly permission: { readonly resource: string; readonly action: string };
  readonly resource: { readonly id?: string };
}): boolean {
  const { principal, roles, permission, resource } = params;
  if (permission.resource === "agent" && permission.action === "read") {
    return hasAnyAccessRole(principal);
  }
  if (!roles.has("member")) return false;
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
  return (
    permission.resource === "approval" &&
    permission.action === "decide" &&
    resource.id === principal.id
  );
}

function hasAnyAccessRole(principal: Principal): boolean {
  return Object.keys(principal.accessRoles).some(
    (namespaceId) => ownRoles(principal.accessRoles, namespaceId).length > 0,
  );
}

function samePrincipal(left: PrincipalRef, right: PrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
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
}): void {
  params.router
    .get("/auth/login/:provider", async (_req, route) => {
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
    })
    .get("/auth/callback/:provider", async (req, route) => {
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
      const result = await params.config.store.findOrCreateUser({
        email,
        bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole }],
      });
      if (result.user.status !== "active") return jsonError(403, "user_disabled");

      const token = newSessionToken();
      await params.config.store.createSession({
        tokenHash: hashSessionToken(token),
        userId: result.user.id,
        ttlMs: params.sessionTtlMs,
      });
      return redirect(new URL("/", params.config.publicBaseUrl).toString(), [
        clearLoginStateCookie(params.secure),
        sessionCookie({ token, ttlMs: params.sessionTtlMs, secure: params.secure }),
      ]);
    })
    .post("/auth/logout", async (req) => {
      const token = sessionToken(req.headers.get("cookie"));
      if (token) await params.config.store.deleteSession(hashSessionToken(token));
      return new Response(null, {
        status: 204,
        headers: { "set-cookie": clearSessionCookie(params.secure) },
      });
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

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}
