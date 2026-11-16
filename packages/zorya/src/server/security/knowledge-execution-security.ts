import type { RetrieverCaller, RetrieverSearchAccess } from "@promin/agent";
import type { RunSecurityContext, WallClock } from "@promin/workflow";
import { normalizeRunSecurityContext } from "@promin/workflow/storage-kit";
import type { Logger } from "../server-config.ts";
import {
  ORG_WIDE_NAMESPACE,
  type AuthorizationDecision,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityContext,
  type ZoryaSecurity,
} from "./security.ts";
import {
  snapshotAuthorizationDecision,
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
    const resolved = await this.security.resolveIdentity({ actor, ...(asserted && { asserted }) });
    if (!resolved) {
      await this.auditResolved({
        params,
        context: snapshotSecurityContext({ actor, effective: asserted ?? toPrincipalRef(actor) }),
        reason: "permission_denied",
      });
      return undefined;
    }
    const context = snapshotSecurityContext(resolved);
    const effective = samePrincipal(context.actor, context.effective)
      ? context.actor
      : await this.security.resolvePrincipal(context.effective);
    if (!effective) {
      await this.auditResolved({ params, context, reason: "identity_unknown" });
      return undefined;
    }
    const identityFilter = await this.security.identityFilter({
      context,
      namespaceId: params.namespaceId,
    });
    if (
      identityFilter.namespaceId !== params.namespaceId ||
      (identityFilter.kind === "identities" &&
        !identityFilter.identities.some((identity) => samePrincipal(identity, context.effective)))
    ) {
      await this.auditResolved({ params, context, reason: "permission_denied" });
      return undefined;
    }
    const permission = snapshotSecurityPermission({ resource: "knowledge", action: "use" });
    const resource = snapshotSecurityResource({
      kind: "knowledge",
      namespaceId: params.namespaceId,
      id: params.baseId,
    });
    const decision = snapshotAuthorizationDecision(
      await this.security.authorize({
        context,
        permission,
        resource,
      } as import("./security.ts").AuthorizationRequest),
    );
    await this.auditDecision({ params, context, decision });
    if (!decision.allowed) return undefined;
    return this.finalAccess({ params, runContext });
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
  }): Promise<RetrieverSearchAccess | undefined> {
    const { params, runContext } = input;
    const actor = await this.security.resolvePrincipal(toPrincipalRef(runContext.actor));
    if (!actor) {
      await this.auditUnresolved({ params, runContext });
      return undefined;
    }
    const asserted = runContext.effective ? toPrincipalRef(runContext.effective) : undefined;
    const resolved = await this.security.resolveIdentity({ actor, ...(asserted && { asserted }) });
    if (!resolved) {
      await this.auditResolved({
        params,
        context: snapshotSecurityContext({ actor, effective: asserted ?? toPrincipalRef(actor) }),
        reason: "permission_denied",
      });
      return undefined;
    }
    const context = snapshotSecurityContext(resolved);
    const effective = samePrincipal(context.actor, context.effective)
      ? context.actor
      : await this.security.resolvePrincipal(context.effective);
    if (!effective) {
      await this.auditResolved({ params, context, reason: "identity_unknown" });
      return undefined;
    }
    const identityFilter = await this.security.identityFilter({
      context,
      namespaceId: params.namespaceId,
    });
    if (
      identityFilter.namespaceId !== params.namespaceId ||
      (identityFilter.kind === "identities" &&
        !identityFilter.identities.some((identity) => samePrincipal(identity, context.effective)))
    ) {
      await this.auditResolved({ params, context, reason: "permission_denied" });
      return undefined;
    }
    const decision = snapshotAuthorizationDecision(
      await this.security.authorize({
        context,
        permission: snapshotSecurityPermission({ resource: "knowledge", action: "use" }),
        resource: snapshotSecurityResource({
          kind: "knowledge",
          namespaceId: params.namespaceId,
          id: params.baseId,
        }),
      } as import("./security.ts").AuthorizationRequest),
    );
    if (!decision.allowed) {
      await this.auditDecision({ params, context, decision });
      return undefined;
    }
    return Object.freeze({ caller: callerFor(effective, params.namespaceId) });
  }

  private event(params: {
    readonly params: { readonly namespaceId: string; readonly baseId: string };
    readonly context: SecurityContext;
    readonly decision: AuthorizationDecision;
  }): SecurityAuditEvent {
    const permission = snapshotSecurityPermission({ resource: "knowledge", action: "use" });
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
    const permission = snapshotSecurityPermission({ resource: "knowledge", action: "use" });
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
    const permission = snapshotSecurityPermission({ resource: "knowledge", action: "use" });
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
      metadata: Object.freeze({ baseId: params.baseId, denialReason: "missing_identity" }),
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
    teams: Object.freeze([]),
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
