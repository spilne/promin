import type { RunSecurityContext, WallClock } from "@promin/workflow";
import type { DurableScheduleConfig } from "@promin/workflow/scheduler";
import { normalizeRunSecurityContext } from "@promin/workflow/storage-kit";
import type { Logger } from "../server-config.ts";
import {
  ORG_WIDE_NAMESPACE,
  type AuthorizationRequest,
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

export class ScheduleSecurityDeniedError extends Error {
  override readonly name = "ScheduleSecurityDeniedError";
  constructor(readonly scheduleId: string) {
    super(`Schedule "${scheduleId}" is not authorized to execute`);
  }
}

export class ScheduleAssignmentError extends Error {
  override readonly name = "ScheduleAssignmentError";
  constructor(readonly code: "assignment_required" | "identity_unknown" | "assignment_denied") {
    super(code);
  }
}

export interface AuthorizedScheduleIdentity {
  readonly context: SecurityContext;
  readonly principal: Principal;
  readonly runContext: RunSecurityContext;
}

export interface ScheduleExecutionSecurityConfig {
  readonly security: ZoryaSecurity;
  readonly clock: Pick<WallClock, "now">;
  readonly logger: Pick<Logger, "error">;
  /** Built-in key mode adopts missing legacy assignments as this principal. */
  readonly legacyPrincipal?: PrincipalRef;
}

/** Fresh identity and policy checks for unattended schedule execution. */
export class ScheduleExecutionSecurity {
  private readonly security: ZoryaSecurity;
  private readonly clock: Pick<WallClock, "now">;
  private readonly logger: Pick<Logger, "error">;
  private readonly legacyPrincipal?: PrincipalRef;

  constructor(config: ScheduleExecutionSecurityConfig) {
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

  /** Resolve a create/update assignment. Module mode requires a service account. */
  async assignedContext(params: {
    readonly scheduleId: string;
    readonly namespaceId: string;
    readonly serviceAccountId?: string;
    readonly runAsUserId?: string;
  }): Promise<RunSecurityContext> {
    let requested: RunSecurityContext | undefined;
    if (params.serviceAccountId !== undefined) {
      requested = {
        actor: { kind: "service-account", id: params.serviceAccountId },
        ...(params.runAsUserId !== undefined && {
          effective: { kind: "user", id: params.runAsUserId },
        }),
      };
    } else if (params.runAsUserId !== undefined) {
      throw new ScheduleAssignmentError("identity_unknown");
    } else if (this.legacyPrincipal !== undefined) {
      requested = { actor: this.legacyPrincipal };
    }
    if (!requested) throw new ScheduleAssignmentError("assignment_required");
    const identity = await this.resolveAndAuthorize({
      scheduleId: params.scheduleId,
      namespaceId: params.namespaceId,
      runContext: requested,
      event: "schedule.assignment.authorization",
    });
    if (!identity) throw new ScheduleAssignmentError("assignment_denied");
    return identity.runContext;
  }

  /** Recheck a stored assignment immediately before dispatch. */
  async authorizeDispatch(schedule: DurableScheduleConfig): Promise<{
    readonly schedule: DurableScheduleConfig;
    readonly identity: AuthorizedScheduleIdentity;
  }> {
    const requested =
      schedule.securityContext ??
      (this.legacyPrincipal !== undefined ? { actor: this.legacyPrincipal } : undefined);
    if (!requested) throw new ScheduleSecurityDeniedError(schedule.id);
    const identity = await this.resolveAndAuthorize({
      scheduleId: schedule.id,
      namespaceId: schedule.namespace ?? ORG_WIDE_NAMESPACE,
      runContext: requested,
      event: "schedule.execution.authorization",
    });
    if (!identity) throw new ScheduleSecurityDeniedError(schedule.id);
    return {
      schedule: Object.freeze({
        ...schedule,
        securityContext: identity.runContext,
      }),
      identity,
    };
  }

  private async resolveAndAuthorize(params: {
    readonly scheduleId: string;
    readonly namespaceId: string;
    readonly runContext: RunSecurityContext;
    readonly event: string;
  }): Promise<AuthorizedScheduleIdentity | undefined> {
    const runContext = normalizeRunSecurityContext(params.runContext);
    const actorRef = toPrincipalRef(runContext.actor);
    const asserted = runContext.effective ? toPrincipalRef(runContext.effective) : undefined;
    const actor = await this.security.resolvePrincipal(actorRef);
    if (!actor) {
      await this.auditDenied({
        params,
        runContext,
        reason: "identity_unknown",
      });
      return undefined;
    }
    const resolved = await this.security.resolveIdentity({
      actor,
      ...(asserted && { asserted }),
    });
    if (!resolved) {
      await this.auditDenied({
        params,
        runContext,
        reason: "act_for_required",
      });
      return undefined;
    }
    const context = snapshotSecurityContext(resolved);
    const principal = samePrincipal(context.actor, context.effective)
      ? context.actor
      : await this.security.resolvePrincipal(context.effective);
    if (!principal) {
      await this.auditDenied({
        params,
        runContext,
        reason: "identity_unknown",
      });
      return undefined;
    }
    const permission = snapshotSecurityPermission({
      resource: "run",
      action: "trigger",
    });
    const resource = snapshotSecurityResource({
      kind: "run",
      namespaceId: params.namespaceId,
      id: `schedule:${params.scheduleId}`,
    });
    const decision = snapshotAuthorizationDecision(
      await this.security.authorize({
        context,
        permission,
        resource,
      } as AuthorizationRequest),
    );
    const event: SecurityAuditEvent = Object.freeze({
      event: params.event,
      occurredAt: this.clock.now().toISOString(),
      context,
      namespaceId: resource.namespaceId,
      resource,
      permission,
      decision,
      metadata: Object.freeze({ scheduleId: params.scheduleId }),
    });
    await this.security.recordAudit(event);
    if (!decision.allowed) return undefined;
    return Object.freeze({
      context,
      principal,
      runContext: toRunContext(context),
    });
  }

  private async auditDenied(params: {
    readonly params: {
      readonly scheduleId: string;
      readonly namespaceId: string;
      readonly event: string;
    };
    readonly runContext: RunSecurityContext;
    readonly reason: "identity_unknown" | "act_for_required";
  }): Promise<void> {
    const actor = unresolvedPrincipal(params.runContext.actor);
    const context = snapshotSecurityContext({
      actor,
      effective: toPrincipalRef(params.runContext.effective ?? params.runContext.actor),
    });
    const permission = snapshotSecurityPermission({
      resource: "run",
      action: "trigger",
    });
    const resource = snapshotSecurityResource({
      kind: "run",
      namespaceId: params.params.namespaceId,
      id: `schedule:${params.params.scheduleId}`,
    });
    const event: SecurityAuditEvent = Object.freeze({
      event: params.params.event,
      occurredAt: this.clock.now().toISOString(),
      context,
      namespaceId: resource.namespaceId,
      resource,
      permission,
      decision: Object.freeze({ allowed: false, reason: params.reason }),
      metadata: Object.freeze({ scheduleId: params.params.scheduleId }),
    });
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      try {
        this.logger.error("[zorya] schedule security denial audit failed", error);
      } catch {
        // Denial stays denied even if observability is unavailable.
      }
    }
  }
}

function toPrincipalRef(ref: {
  readonly kind: PrincipalRef["kind"];
  readonly id: string;
}): PrincipalRef {
  return Object.freeze({ kind: ref.kind, id: ref.id });
}

function toRunContext(context: SecurityContext): RunSecurityContext {
  return normalizeRunSecurityContext({
    actor: { kind: context.actor.kind, id: context.actor.id },
    ...(!samePrincipal(context.actor, context.effective) && {
      effective: { kind: context.effective.kind, id: context.effective.id },
    }),
  });
}

function samePrincipal(left: PrincipalRef, right: PrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function unresolvedPrincipal(ref: {
  readonly kind: PrincipalRef["kind"];
  readonly id: string;
}): Principal {
  return Object.freeze({
    kind: ref.kind,
    id: ref.id,
    accessRoles: Object.freeze({}),
    canActFor: false,
  });
}
