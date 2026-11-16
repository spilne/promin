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
    readonly caller?: SecurityContext;
    readonly serviceAccountId?: string;
    readonly runAsUserId?: string;
    readonly resourceId?: string;
  }): Promise<RunSecurityContext> {
    let requested: RunSecurityContext | undefined;
    if (params.serviceAccountId !== undefined) {
      if (!params.caller) throw new ScheduleAssignmentError("assignment_denied");
      const context = snapshotSecurityContext(params.caller);
      const permission = snapshotSecurityPermission({
        resource: "service-account",
        action: "use",
      });
      const resource = snapshotSecurityResource({
        kind: "service-account",
        namespaceId: params.namespaceId,
        id: params.serviceAccountId,
        principal: { kind: "service-account", id: params.serviceAccountId },
      });
      const decision = snapshotAuthorizationDecision(
        await this.security.authorize({ context, permission, resource } as AuthorizationRequest),
      );
      try {
        await this.security.recordAudit(
          Object.freeze({
            event: "schedule.service-account.authorization",
            occurredAt: this.clock.now().toISOString(),
            context,
            namespaceId: params.namespaceId,
            resource,
            permission,
            decision,
            metadata: Object.freeze({ scheduleId: params.scheduleId }),
          }),
        );
      } catch (error) {
        if (decision.allowed) throw error;
        try {
          this.logger.error("[zorya] schedule service-account denial audit failed", error);
        } catch {
          // An explicit denial stays denied when observability is unavailable.
        }
      }
      if (!decision.allowed) throw new ScheduleAssignmentError("assignment_denied");
      const representedUser = params.runAsUserId ?? params.resourceId;
      requested = {
        actor: { kind: "service-account", id: params.serviceAccountId },
        ...(representedUser !== undefined && {
          effective: { kind: "user", id: representedUser },
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
    const snapshot = snapshotSchedule(schedule);
    const requested =
      snapshot.securityContext ??
      (this.legacyPrincipal !== undefined ? { actor: this.legacyPrincipal } : undefined);
    if (!requested) throw new ScheduleSecurityDeniedError(snapshot.id);
    const resourceBound = bindAgentResource(snapshot, requested);
    if (!resourceBound) {
      await this.auditDenied({
        params: {
          scheduleId: snapshot.id,
          namespaceId: snapshot.namespace ?? ORG_WIDE_NAMESPACE,
          event: "schedule.execution.authorization",
        },
        runContext: requested,
        reason: "act_for_required",
      });
      throw new ScheduleSecurityDeniedError(snapshot.id);
    }
    const identity = await this.resolveAndAuthorize({
      scheduleId: snapshot.id,
      namespaceId: snapshot.namespace ?? ORG_WIDE_NAMESPACE,
      runContext: resourceBound,
      event: "schedule.execution.authorization",
    });
    if (!identity) throw new ScheduleSecurityDeniedError(snapshot.id);
    return {
      schedule: Object.freeze({
        ...snapshot,
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
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      if (decision.allowed) throw error;
      try {
        this.logger.error("[zorya] schedule security denial audit failed", error);
      } catch {
        // An explicit denial stays denied even when observability is unavailable.
      }
      return undefined;
    }
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

/**
 * Bind agent-owned memory/thread scope to the effective user. A service
 * account without an explicit effective identity may assert the resource
 * user, but `resolveIdentity` still has to approve its current act-for grant.
 */
function bindAgentResource(
  schedule: DurableScheduleConfig,
  context: RunSecurityContext,
): RunSecurityContext | undefined {
  const resourceId =
    schedule.metadata?.agentTrigger === true && typeof schedule.metadata.resourceId === "string"
      ? schedule.metadata.resourceId
      : undefined;
  if (resourceId === undefined) return normalizeRunSecurityContext(context);
  const normalized = normalizeRunSecurityContext(context);
  if (
    normalized.effective !== undefined &&
    (normalized.effective.kind !== "user" || normalized.effective.id !== resourceId)
  ) {
    return undefined;
  }
  if (
    normalized.effective === undefined &&
    normalized.actor.kind === "user" &&
    normalized.actor.id !== resourceId
  ) {
    return undefined;
  }
  return normalizeRunSecurityContext({
    actor: normalized.actor,
    effective: { kind: "user", id: resourceId },
  });
}

/** Read every schedule field once, detach caller-owned data, and freeze it before any await. */
function snapshotSchedule(source: DurableScheduleConfig): DurableScheduleConfig {
  const id = source.id;
  const name = source.name;
  const namespace = source.namespace;
  const cron = source.cron;
  const rrule = source.rrule;
  const intervalMs = source.intervalMs;
  const timezone = source.timezone;
  const enabled = source.enabled;
  const startAt = source.startAt;
  const endAt = source.endAt;
  const jitterMs = source.jitterMs;
  const metadata = source.metadata;
  const securityContext = source.securityContext;
  const maxCatchUp = source.maxCatchUp;
  return Object.freeze({
    id,
    ...(name !== undefined && { name }),
    ...(namespace !== undefined && { namespace }),
    ...(cron !== undefined && { cron }),
    ...(rrule !== undefined && { rrule }),
    ...(intervalMs !== undefined && { intervalMs }),
    ...(timezone !== undefined && { timezone }),
    ...(enabled !== undefined && { enabled }),
    ...(startAt !== undefined && { startAt: snapshotDate(startAt, "startAt") }),
    ...(endAt !== undefined && { endAt: snapshotDate(endAt, "endAt") }),
    ...(jitterMs !== undefined && { jitterMs }),
    ...(metadata !== undefined && { metadata: snapshotMetadata(metadata) }),
    ...(securityContext !== undefined && {
      securityContext: normalizeRunSecurityContext(securityContext),
    }),
    ...(maxCatchUp !== undefined && { maxCatchUp }),
  });
}

function snapshotDate(value: Date, field: string): Date {
  const time = Date.prototype.getTime.call(value);
  if (!Number.isFinite(time)) throw new TypeError(`schedule ${field} must be a valid Date`);
  return Object.freeze(new Date(time));
}

function snapshotMetadata(source: Record<string, unknown>): Record<string, unknown> {
  const seen = new WeakSet<object>();
  const detached = snapshotMetadataValue(source, seen, "metadata");
  if (detached === null || typeof detached !== "object" || Array.isArray(detached)) {
    throw new TypeError("schedule metadata must be an object");
  }
  return detached as Record<string, unknown>;
}

function snapshotMetadataValue(value: unknown, seen: WeakSet<object>, path: string): unknown {
  if (
    value === undefined ||
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value !== "object") {
    throw new TypeError(`schedule ${path} contains a non-data value`);
  }
  if (seen.has(value)) throw new TypeError(`schedule ${path} contains a cycle`);
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      const out = value.map((entry, index) =>
        snapshotMetadataValue(entry, seen, `${path}[${index}]`),
      );
      return Object.freeze(out);
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`schedule ${path} must contain plain data`);
    }
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value)) {
      out[key] = snapshotMetadataValue(
        (value as Record<string, unknown>)[key],
        seen,
        `${path}.${key}`,
      );
    }
    return Object.freeze(out);
  } finally {
    seen.delete(value);
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
