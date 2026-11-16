import type { RetrieverCaller, RetrieverSearchAccess } from "@promin/agent";
import type { RunSecurityContext, WallClock } from "@promin/workflow";
import { normalizeRunSecurityContext } from "@promin/workflow/storage-kit";
import type { Logger } from "../server-config.ts";
import {
  ORG_WIDE_NAMESPACE,
  type AuthorizationDecision,
  type EffectiveAuthorizationResult,
  type IdentityFilter,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityContext,
  type ZoryaSecurity,
} from "./security.ts";
import {
  snapshotAuthorizationDecision,
  snapshotEffectiveAuthorizationResult,
  snapshotPrincipalRef,
  snapshotSecurityContext,
  snapshotSecurityPermission,
  snapshotSecurityResource,
} from "./route-access-guard.ts";

export interface KnowledgeExecutionSecurityConfig {
  readonly security: ZoryaSecurity;
  readonly clock: Pick<WallClock, "now">;
  readonly logger: Pick<Logger, "error">;
  /** Built-in key mode adopts missing legacy turns as this principal. */
  readonly legacyPrincipal?: PrincipalRef;
}

/** Fresh knowledge-base authorization performed at every retriever search. */
export class KnowledgeExecutionSecurity {
  private readonly security: ZoryaSecurity;
  private readonly clock: Pick<WallClock, "now">;
  private readonly logger: Pick<Logger, "error">;
  private readonly legacyPrincipal?: PrincipalRef;

  constructor(config: KnowledgeExecutionSecurityConfig) {
    this.security = config.security;
    this.clock = config.clock;
    this.logger = config.logger;
    if (config.legacyPrincipal) {
      this.legacyPrincipal = Object.freeze({
        kind: config.legacyPrincipal.kind,
        id: config.legacyPrincipal.id,
      });
    }
  }

  async access(params: {
    readonly securityContext?: RunSecurityContext;
    readonly namespaceId: string;
    readonly baseId: string;
  }): Promise<RetrieverSearchAccess | undefined> {
    // Built-in API-key mode is namespace-only and deliberately ignores the
    // legacy request body's represented resource. Enterprise mode instead
    // requires the durable/request identity captured for this turn.
    const requested = this.legacyPrincipal
      ? { actor: this.legacyPrincipal }
      : params.securityContext;
    if (!requested) {
      await this.auditMissing(params);
      return undefined;
    }
    const runContext = normalizeRunSecurityContext(requested);
    const actor = await this.security.resolvePrincipal(toPrincipalRef(runContext.actor));
    if (!actor) {
      await this.auditUnresolved({ params, runContext });
      return undefined;
    }
    const asserted = runContext.effective ? toPrincipalRef(runContext.effective) : undefined;
    const resolved = await this.security.resolveIdentity({
      actor,
      ...(asserted && { asserted }),
    });
    if (!resolved) {
      await this.auditResolved({
        params,
        context: snapshotSecurityContext({
          actor,
          effective: asserted ?? toPrincipalRef(actor),
        }),
        reason: "permission_denied",
      });
      return undefined;
    }
    const context = snapshotSecurityContext(resolved);
    const identityFilter = await this.currentIdentityFilter({
      context,
      params,
    });
    if (!identityFilter || !filterIncludes(identityFilter, context.effective)) {
      await this.auditResolved({
        params,
        context,
        reason: "permission_denied",
      });
      return undefined;
    }
    const permission = snapshotSecurityPermission({
      resource: "knowledge",
      action: "use",
    });
    const resource = snapshotSecurityResource({
      kind: "knowledge",
      namespaceId: params.namespaceId,
      id: params.baseId,
    });
    const authorizeEffective = this.security.authorizeEffective;
    const authorization = authorizeEffective
      ? snapshotEffectiveAuthorizationResult(
          await authorizeEffective({
            context,
            permission,
            resource,
          } as import("./security.ts").AuthorizationRequest),
        )
      : undefined;
    const bound =
      !authorization?.decision.allowed ||
      (authorization.effective !== undefined &&
        samePrincipal(authorization.effective, context.effective));
    const decision =
      authorization && bound
        ? authorization.decision
        : snapshotAuthorizationDecision({
            allowed: false,
            reason: "permission_denied",
          });
    await this.auditDecision({ params, context, decision });
    if (
      !decision.allowed ||
      !authorization?.decision.allowed ||
      authorization.effective === undefined
    ) {
      return undefined;
    }
    return this.finalAccess({
      params,
      runContext,
      auditedAuthorization: authorization,
      auditedIdentityFilter: identityFilter,
    });
  }

  /**
   * Audit is an awaited external sink and can race revocation. Repeat the
   * whole identity/filter/authorization chain after it, then return without
   * another await on the allowed path.
   */
  private async finalAccess(input: {
    readonly params: {
      readonly securityContext?: RunSecurityContext;
      readonly namespaceId: string;
      readonly baseId: string;
    };
    readonly runContext: RunSecurityContext;
    readonly auditedAuthorization: EffectiveAuthorizationResult;
    readonly auditedIdentityFilter: IdentityFilter;
  }): Promise<RetrieverSearchAccess | undefined> {
    const { params, runContext } = input;
    const actor = await this.security.resolvePrincipal(toPrincipalRef(runContext.actor));
    if (!actor) {
      await this.auditUnresolved({ params, runContext });
      return undefined;
    }
    const asserted = runContext.effective ? toPrincipalRef(runContext.effective) : undefined;
    const resolved = await this.security.resolveIdentity({
      actor,
      ...(asserted && { asserted }),
    });
    if (!resolved) {
      await this.auditResolved({
        params,
        context: snapshotSecurityContext({
          actor,
          effective: asserted ?? toPrincipalRef(actor),
        }),
        reason: "permission_denied",
      });
      return undefined;
    }
    const context = snapshotSecurityContext(resolved);
    const identityFilter = await this.currentIdentityFilter({
      context,
      params,
    });
    if (!identityFilter || !filterIncludes(identityFilter, context.effective)) {
      await this.auditResolved({
        params,
        context,
        reason: "permission_denied",
      });
      return undefined;
    }
    const authorizeEffective = this.security.authorizeEffective;
    const authorization = authorizeEffective
      ? snapshotEffectiveAuthorizationResult(
          await authorizeEffective({
            context,
            permission: snapshotSecurityPermission({
              resource: "knowledge",
              action: "use",
            }),
            resource: snapshotSecurityResource({
              kind: "knowledge",
              namespaceId: params.namespaceId,
              id: params.baseId,
            }),
          } as import("./security.ts").AuthorizationRequest),
        )
      : undefined;
    if (
      !authorization ||
      !authorization.decision.allowed ||
      !authorization.effective ||
      !samePrincipal(authorization.effective, context.effective)
    ) {
      const decision =
        authorization?.decision.allowed === false
          ? authorization.decision
          : snapshotAuthorizationDecision({
              allowed: false,
              reason: "permission_denied",
            });
      await this.auditDecision({ params, context, decision });
      return undefined;
    }
    if (
      !sameAuthorizationSnapshot(authorization, input.auditedAuthorization) ||
      !sameIdentityFilter(identityFilter, input.auditedIdentityFilter)
    ) {
      await this.auditResolved({
        params,
        context,
        reason: "permission_denied",
      });
      return undefined;
    }
    return Object.freeze({
      caller: callerFor(authorization.effective, params.namespaceId),
    });
  }

  private async currentIdentityFilter(input: {
    readonly context: SecurityContext;
    readonly params: { readonly namespaceId: string; readonly baseId: string };
  }): Promise<IdentityFilter | undefined> {
    try {
      const filter = snapshotIdentityFilter(
        await this.security.identityFilter({
          context: input.context,
          namespaceId: input.params.namespaceId,
        }),
      );
      return filter.namespaceId === input.params.namespaceId ? filter : undefined;
    } catch {
      return undefined;
    }
  }

  private event(params: {
    readonly params: { readonly namespaceId: string; readonly baseId: string };
    readonly context: SecurityContext;
    readonly decision: AuthorizationDecision;
  }): SecurityAuditEvent {
    const permission = snapshotSecurityPermission({
      resource: "knowledge",
      action: "use",
    });
    const resource = snapshotSecurityResource({
      kind: "knowledge",
      namespaceId: params.params.namespaceId,
      id: params.params.baseId,
    });
    return Object.freeze({
      event: "knowledge.retrieval.authorization",
      occurredAt: this.clock.now().toISOString(),
      context: params.context,
      namespaceId: params.params.namespaceId,
      resource,
      permission,
      decision: params.decision,
      metadata: Object.freeze({ baseId: params.params.baseId }),
    });
  }

  private async auditResolved(params: {
    readonly params: { readonly namespaceId: string; readonly baseId: string };
    readonly context: SecurityContext;
    readonly reason: "identity_unknown" | "permission_denied";
  }): Promise<void> {
    await this.auditDecision({
      params: params.params,
      context: params.context,
      decision: Object.freeze({ allowed: false, reason: params.reason }),
    });
  }

  private async auditDecision(params: {
    readonly params: { readonly namespaceId: string; readonly baseId: string };
    readonly context: SecurityContext;
    readonly decision: AuthorizationDecision;
  }): Promise<void> {
    const event = this.event(params);
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      if (params.decision.allowed) throw error;
      this.logDeniedAudit(error);
    }
  }

  private async auditUnresolved(params: {
    readonly params: { readonly namespaceId: string; readonly baseId: string };
    readonly runContext: RunSecurityContext;
  }): Promise<void> {
    const permission = snapshotSecurityPermission({
      resource: "knowledge",
      action: "use",
    });
    const resource = snapshotSecurityResource({
      kind: "knowledge",
      namespaceId: params.params.namespaceId,
      id: params.params.baseId,
    });
    const actor = toPrincipalRef(params.runContext.actor);
    const effective = toPrincipalRef(params.runContext.effective ?? params.runContext.actor);
    const event: SecurityAuditEvent = Object.freeze({
      event: "knowledge.retrieval.authorization",
      occurredAt: this.clock.now().toISOString(),
      namespaceId: params.params.namespaceId,
      resource,
      permission,
      decision: Object.freeze({ allowed: false, reason: "identity_unknown" }),
      metadata: Object.freeze({
        baseId: params.params.baseId,
        denialReason: "identity_unknown",
        actorKind: actor.kind,
        actorId: actor.id,
        effectiveKind: effective.kind,
        effectiveId: effective.id,
      }),
    });
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      this.logDeniedAudit(error);
    }
  }

  private async auditMissing(params: {
    readonly namespaceId: string;
    readonly baseId: string;
  }): Promise<void> {
    const permission = snapshotSecurityPermission({
      resource: "knowledge",
      action: "use",
    });
    const resource = snapshotSecurityResource({
      kind: "knowledge",
      namespaceId: params.namespaceId,
      id: params.baseId,
    });
    const event: SecurityAuditEvent = Object.freeze({
      event: "knowledge.retrieval.authorization",
      occurredAt: this.clock.now().toISOString(),
      namespaceId: params.namespaceId,
      resource,
      permission,
      decision: Object.freeze({ allowed: false, reason: "identity_unknown" }),
      metadata: Object.freeze({
        baseId: params.baseId,
        denialReason: "missing_identity",
      }),
    });
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      this.logDeniedAudit(error);
    }
  }

  private logDeniedAudit(error: unknown): void {
    try {
      this.logger.error("[zorya] knowledge denial audit failed", error);
    } catch {
      // A denial remains denied when observability is unavailable.
    }
  }
}

function callerFor(principal: Principal, namespaceId: string): RetrieverCaller {
  const roles = [...ownRoles(principal, namespaceId), ...ownRoles(principal, ORG_WIDE_NAMESPACE)];
  return Object.freeze({
    subject: Object.freeze({ kind: principal.kind, id: principal.id }),
    userId: principal.id,
    ...(principal.email !== undefined && { email: principal.email }),
    accessRoles: Object.freeze([...new Set(roles)]),
    teams: Object.freeze([...(principal.teams ?? [])]),
  });
}

function ownRoles(principal: Principal, namespaceId: string): ReadonlyArray<string> {
  return Object.hasOwn(principal.accessRoles, namespaceId)
    ? (principal.accessRoles[namespaceId] ?? [])
    : [];
}

function toPrincipalRef(ref: { readonly kind: string; readonly id: string }): PrincipalRef {
  return Object.freeze({ kind: ref.kind as PrincipalRef["kind"], id: ref.id });
}

function samePrincipal(
  left: { readonly kind: string; readonly id: string },
  right: { readonly kind: string; readonly id: string },
): boolean {
  return left.kind === right.kind && left.id === right.id;
}

const MAX_IDENTITY_FILTER_IDENTITIES = 10_000;

function snapshotIdentityFilter(source: unknown): IdentityFilter {
  if ((typeof source !== "object" && typeof source !== "function") || source === null) {
    throw new TypeError("Invalid identity filter");
  }
  const kind = Reflect.get(source, "kind") as unknown;
  const namespaceId = Reflect.get(source, "namespaceId") as unknown;
  if (typeof namespaceId !== "string" || namespaceId.length === 0) {
    throw new TypeError("Invalid identity filter namespace");
  }
  if (kind === "namespace") return Object.freeze({ kind, namespaceId });
  if (kind !== "identities") throw new TypeError("Invalid identity filter kind");
  const identitiesValue = Reflect.get(source, "identities") as unknown;
  if (!Array.isArray(identitiesValue)) throw new TypeError("Invalid identity filter identities");
  const length = Reflect.get(identitiesValue, "length") as unknown;
  if (
    typeof length !== "number" ||
    !Number.isSafeInteger(length) ||
    length < 0 ||
    length > MAX_IDENTITY_FILTER_IDENTITIES
  ) {
    throw new TypeError("Invalid identity filter identities length");
  }
  const identities: PrincipalRef[] = [];
  for (let index = 0; index < length; index++) {
    identities.push(
      snapshotPrincipalRef(Reflect.get(identitiesValue, String(index)) as PrincipalRef),
    );
  }
  return Object.freeze({
    kind,
    namespaceId,
    identities: Object.freeze(identities),
  });
}

function sameAuthorizationSnapshot(
  current: EffectiveAuthorizationResult,
  audited: EffectiveAuthorizationResult,
): boolean {
  return (
    current.decision.allowed === audited.decision.allowed &&
    current.decision.reason === audited.decision.reason &&
    current.effective !== undefined &&
    audited.effective !== undefined &&
    samePrincipalSnapshot(current.effective, audited.effective)
  );
}

function samePrincipalSnapshot(left: Principal, right: Principal): boolean {
  if (
    !samePrincipal(left, right) ||
    left.email !== right.email ||
    left.canActFor !== right.canActFor
  ) {
    return false;
  }
  const leftNamespaces = Object.keys(left.accessRoles);
  const rightNamespaces = Object.keys(right.accessRoles);
  if (leftNamespaces.length !== rightNamespaces.length) return false;
  const sameRoles = leftNamespaces.every((namespaceId) => {
    if (!Object.hasOwn(right.accessRoles, namespaceId)) return false;
    const leftRoles = left.accessRoles[namespaceId] ?? [];
    const rightRoles = right.accessRoles[namespaceId] ?? [];
    return (
      leftRoles.length === rightRoles.length &&
      leftRoles.every((role, index) => role === rightRoles[index])
    );
  });
  if (!sameRoles) return false;
  const leftTeams = left.teams ?? [];
  const rightTeams = right.teams ?? [];
  return (
    leftTeams.length === rightTeams.length &&
    leftTeams.every((team, index) => team === rightTeams[index])
  );
}

function sameIdentityFilter(left: IdentityFilter, right: IdentityFilter): boolean {
  if (left.kind !== right.kind || left.namespaceId !== right.namespaceId) return false;
  if (left.kind === "namespace" || right.kind === "namespace") return true;
  return (
    left.identities.length === right.identities.length &&
    left.identities.every((identity, index) => samePrincipal(identity, right.identities[index]!))
  );
}

function filterIncludes(filter: IdentityFilter, effective: PrincipalRef): boolean {
  return (
    filter.kind === "namespace" ||
    filter.identities.some((identity) => samePrincipal(identity, effective))
  );
}
