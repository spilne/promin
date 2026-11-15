import {
  type FenceGuard,
  type RunPrincipalRef,
  type RunSecurityContext,
  type WallClock,
  type WorkflowRunAdmissionGuard,
  type WorkflowState,
  type WorkflowStorage,
} from "@promin/workflow";
import { normalizeRunSecurityContext } from "@promin/workflow/storage-kit";
import type { Logger } from "../server-config.ts";
import {
  ORG_WIDE_NAMESPACE,
  type AuthorizationDecision,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type AuthorizationRequest,
  type ZoryaSecurity,
} from "./security.ts";
import {
  snapshotAuthorizationDecision,
  snapshotSecurityContext,
  snapshotSecurityPermission,
  snapshotSecurityResource,
} from "./route-access-guard.ts";

export const RUN_SECURITY_BLOCKED_ERROR_TAG = "RunSecurityBlockedError";
const RUN_SECURITY_BLOCKED_MESSAGE = "Run blocked by security policy";

/** Stable terminal error surfaced for a durable run whose identity is no longer authorized. */
export class RunSecurityBlockedError extends Error {
  readonly _tag = RUN_SECURITY_BLOCKED_ERROR_TAG;
  readonly workflowId: string;

  constructor(workflowId: string) {
    super(`${RUN_SECURITY_BLOCKED_MESSAGE} (${workflowId})`);
    this.name = RUN_SECURITY_BLOCKED_ERROR_TAG;
    this.workflowId = workflowId;
  }
}

export interface RunExecutionSecurityConfig {
  readonly storage: WorkflowStorage;
  readonly security: ZoryaSecurity;
  readonly clock: Pick<WallClock, "now">;
  readonly logger: Pick<Logger, "error">;
  /** Built-in key mode adopts contextless legacy rows as this principal. */
  readonly legacyPrincipal?: PrincipalRef;
}

/**
 * Re-resolves and authorizes a durable run immediately before execution.
 * Policy denial is persisted as a terminal failure so scanners and workers
 * never retry the same revoked identity forever.
 */
export class RunExecutionSecurity {
  private readonly storage: WorkflowStorage;
  private readonly security: ZoryaSecurity;
  private readonly clock: Pick<WallClock, "now">;
  private readonly logger: Pick<Logger, "error">;
  private readonly legacyPrincipal?: PrincipalRef;

  constructor(config: RunExecutionSecurityConfig) {
    this.storage = config.storage;
    this.security = config.security;
    this.clock = config.clock;
    this.logger = config.logger;
    if (config.legacyPrincipal !== undefined) {
      this.legacyPrincipal = Object.freeze({
        kind: config.legacyPrincipal.kind,
        id: config.legacyPrincipal.id,
      });
    }
  }

  /** Context persisted for a new trigger, including key-mode legacy adoption. */
  contextForTrigger(requested: RunSecurityContext | undefined): RunSecurityContext | undefined {
    if (requested !== undefined) return normalizeRunSecurityContext(requested);
    if (this.legacyPrincipal === undefined) return undefined;
    return normalizeRunSecurityContext({ actor: this.legacyPrincipal });
  }

  /** Workflow-engine hook: guarded, terminal and inherited by child runs. */
  readonly admissionGuard: WorkflowRunAdmissionGuard = async ({ state, guard }) => {
    if (await this.authorizeState({ state, source: "execute", guard })) return;
    throw new RunSecurityBlockedError(state.workflowId);
  };

  /** Authorize a stored run before queueing/claiming it for a remote worker. */
  async authorizeStoredRun(workflowId: string, source: RunExecutionSource): Promise<boolean> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) return false;
    return this.authorizeState({ state, source });
  }

  /** Authorize a child create before a remote worker may persist it. */
  async authorizeContext(params: {
    readonly workflowId: string;
    readonly namespace?: string;
    readonly securityContext?: RunSecurityContext;
    readonly source: RunExecutionSource;
  }): Promise<boolean> {
    const runContext = this.contextForStored(params.securityContext);
    if (!runContext) {
      this.report("missing durable security context", params.workflowId);
      return false;
    }
    return this.decide({
      workflowId: params.workflowId,
      namespaceId: params.namespace ?? ORG_WIDE_NAMESPACE,
      runContext,
      source: params.source,
    });
  }

  private async authorizeState(params: {
    readonly state: WorkflowState;
    readonly source: RunExecutionSource;
    readonly guard?: FenceGuard;
  }): Promise<boolean> {
    const runContext = this.contextForStored(params.state.securityContext);
    const allowed =
      runContext !== undefined &&
      (await this.decide({
        workflowId: params.state.workflowId,
        namespaceId: params.state.namespace ?? ORG_WIDE_NAMESPACE,
        runContext,
        source: params.source,
      }));
    if (allowed) return true;
    if (!runContext) {
      this.report("missing durable security context", params.state.workflowId);
    }
    await this.storage.failWorkflow({
      workflowId: params.state.workflowId,
      error: RUN_SECURITY_BLOCKED_MESSAGE,
      errorTag: RUN_SECURITY_BLOCKED_ERROR_TAG,
      ...(params.guard !== undefined && { guard: params.guard }),
    });
    return false;
  }

  private contextForStored(stored: RunSecurityContext | undefined): RunSecurityContext | undefined {
    return this.contextForTrigger(stored);
  }

  private async decide(params: {
    readonly workflowId: string;
    readonly namespaceId: string;
    readonly runContext: RunSecurityContext;
    readonly source: RunExecutionSource;
  }): Promise<boolean> {
    const actorRef = toPrincipalRef(params.runContext.actor);
    const effectiveRef = toPrincipalRef(params.runContext.effective ?? params.runContext.actor);
    let actor: Principal | undefined;
    try {
      actor = await this.security.resolvePrincipal(actorRef);
    } catch (error) {
      this.report("principal resolution failed", params.workflowId, error);
    }
    const context = snapshotSecurityContext({
      actor: actor ?? unresolvedPrincipal(actorRef),
      effective: effectiveRef,
    });
    const permission = snapshotSecurityPermission({ resource: "run", action: "trigger" });
    const resource = snapshotSecurityResource({
      kind: "run",
      namespaceId: params.namespaceId,
      id: params.workflowId,
    });

    let decision: AuthorizationDecision;
    try {
      decision = snapshotAuthorizationDecision(
        await this.security.authorize({ context, permission, resource } as AuthorizationRequest),
      );
    } catch (error) {
      this.report("authorization failed", params.workflowId, error);
      decision = Object.freeze({ allowed: false, reason: "permission_denied" });
    }

    const event: SecurityAuditEvent = Object.freeze({
      event: "run.execution.authorization",
      occurredAt: this.clock.now().toISOString(),
      context,
      namespaceId: resource.namespaceId,
      resource,
      permission,
      decision,
      metadata: Object.freeze({ source: params.source }),
    });
    try {
      await this.security.recordAudit(event);
    } catch (error) {
      this.report("audit failed", params.workflowId, error);
      // Allowed execution without its audit record is a fail-open.
      return false;
    }
    return decision.allowed;
  }

  private report(message: string, workflowId: string, error?: unknown): void {
    try {
      this.logger.error(
        `[zorya] run security ${message} (run ${workflowId})`,
        ...(error === undefined ? [] : [error]),
      );
    } catch {
      // Observability never changes the fail-closed decision.
    }
  }
}

export type RunExecutionSource =
  | "execute"
  | "queue"
  | "wake"
  | "recovery"
  | "worker-claim"
  | "worker-lock"
  | "child-create";

function toPrincipalRef(ref: RunPrincipalRef): PrincipalRef {
  return Object.freeze({ kind: ref.kind, id: ref.id });
}

function unresolvedPrincipal(ref: PrincipalRef): Principal {
  return Object.freeze({
    kind: ref.kind,
    id: ref.id,
    accessRoles: Object.freeze({}),
    canActFor: false,
  });
}
