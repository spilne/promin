import { describe, expect, it } from "bun:test";
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  SystemWallClock,
  workflow,
  type RunSecurityContext,
  type WorkflowState,
} from "@promin/workflow";
import { succeed } from "@spilne/perfect-core";
import {
  RUN_SECURITY_BLOCKED_ERROR_TAG,
  RunExecutionSecurity,
  RunSecurityBlockedError,
} from "../run-execution-security.ts";
import {
  OPERATOR_PRINCIPAL,
  principalRef,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type ZoryaSecurity,
} from "../security.ts";

const quietLogger = { error() {} };

function principal(ref: PrincipalRef, canActFor = false): Principal {
  return { ...ref, accessRoles: { team: ["member"] }, canActFor };
}

function security(
  params: {
    principals?: Map<string, Principal>;
    allowed?: () => boolean;
    audit?: (event: SecurityAuditEvent) => Promise<void>;
  } = {},
): ZoryaSecurity {
  const principals = params.principals ?? new Map([["operator:operator", OPERATOR_PRINCIPAL]]);
  const resolve = async (ref: PrincipalRef) => principals.get(`${ref.kind}:${ref.id}`);
  return {
    authenticate: async () => undefined,
    resolvePrincipal: resolve,
    resolveIdentity: async ({ actor, asserted }) => ({
      actor,
      effective: asserted ?? principalRef(actor),
    }),
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async ({ context }) => {
      const current = await resolve(context.actor);
      if (!current) return { allowed: false, reason: "identity_unknown" };
      if (context.effective.id !== current.id && !current.canActFor) {
        return { allowed: false, reason: "act_for_required" };
      }
      return params.allowed?.() === false
        ? { allowed: false, reason: "permission_denied" }
        : { allowed: true, reason: "access_role_allow" };
    },
    recordAudit: params.audit ?? (async () => {}),
  };
}

async function storedRun(params: {
  storage: InMemoryWorkflowStorage;
  id: string;
  securityContext?: RunSecurityContext;
}): Promise<WorkflowState> {
  await params.storage.createWorkflow({
    workflowId: params.id,
    workflowName: "secured",
    input: null,
    namespace: "team",
    ...(params.securityContext !== undefined && { securityContext: params.securityContext }),
  });
  return (await params.storage.loadWorkflow(params.id))!;
}

describe("RunExecutionSecurity", () => {
  it("adopts a legacy operator only when explicitly configured", async () => {
    const storage = new InMemoryWorkflowStorage();
    const audits: SecurityAuditEvent[] = [];
    const guard = new RunExecutionSecurity({
      storage,
      security: security({ audit: async (event) => void audits.push(event) }),
      clock: SystemWallClock,
      logger: quietLogger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });
    const state = await storedRun({ storage, id: "legacy" });

    await guard.admissionGuard({ state });

    expect((await storage.loadWorkflow("legacy"))?.status).toBe("pending");
    expect(audits).toHaveLength(1);
    expect(audits[0]).toMatchObject({
      event: "run.execution.authorization",
      context: { actor: { kind: "operator", id: "operator" } },
      decision: { allowed: true, reason: "access_role_allow" },
    });
  });

  it("terminalizes a contextless enterprise run instead of retrying it", async () => {
    const storage = new InMemoryWorkflowStorage();
    const guard = new RunExecutionSecurity({
      storage,
      security: security(),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    const state = await storedRun({ storage, id: "missing" });

    await expect(guard.admissionGuard({ state })).rejects.toBeInstanceOf(RunSecurityBlockedError);
    expect(await storage.loadWorkflow("missing")).toMatchObject({
      status: "failed",
      errorTag: RUN_SECURITY_BLOCKED_ERROR_TAG,
    });
  });

  it("freshly rechecks actor and current act-for authority", async () => {
    const storage = new InMemoryWorkflowStorage();
    const actor = principal({ kind: "service-account", id: "agent" }, true);
    const user = principal({ kind: "user", id: "alice" });
    const principals = new Map([
      ["service-account:agent", actor],
      ["user:alice", user],
    ]);
    const guard = new RunExecutionSecurity({
      storage,
      security: security({ principals }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    const state = await storedRun({
      storage,
      id: "revoked",
      securityContext: {
        actor: { kind: "service-account", id: "agent" },
        effective: { kind: "user", id: "alice" },
      },
    });
    await guard.admissionGuard({ state });

    principals.set("service-account:agent", { ...actor, canActFor: false });
    await expect(guard.admissionGuard({ state })).rejects.toBeInstanceOf(RunSecurityBlockedError);
    expect(await storage.loadWorkflow("revoked")).toMatchObject({
      status: "failed",
      errorTag: RUN_SECURITY_BLOCKED_ERROR_TAG,
    });
  });

  it("propagates a temporary principal-directory failure without terminalizing the run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const guard = new RunExecutionSecurity({
      storage,
      security: {
        ...security(),
        resolvePrincipal: async () => {
          throw new Error("directory down");
        },
      },
      clock: SystemWallClock,
      logger: quietLogger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });
    const state = await storedRun({ storage, id: "directory-down" });

    await expect(guard.admissionGuard({ state })).rejects.toThrow("directory down");
    expect((await storage.loadWorkflow("directory-down"))?.status).toBe("pending");
  });

  it("propagates a temporary authorizer failure without terminalizing the run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const guard = new RunExecutionSecurity({
      storage,
      security: {
        ...security(),
        authorize: async () => {
          throw new Error("policy down");
        },
      },
      clock: SystemWallClock,
      logger: quietLogger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });
    const state = await storedRun({ storage, id: "policy-down" });

    await expect(guard.admissionGuard({ state })).rejects.toThrow("policy down");
    expect((await storage.loadWorkflow("policy-down"))?.status).toBe("pending");
  });

  it("retries safely after a temporary audit failure instead of recording a denial", async () => {
    const storage = new InMemoryWorkflowStorage();
    let auditDown = true;
    const guard = new RunExecutionSecurity({
      storage,
      security: security({
        audit: async () => {
          if (auditDown) throw new Error("audit down");
        },
      }),
      clock: SystemWallClock,
      logger: quietLogger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });
    const state = await storedRun({ storage, id: "audit-down" });

    await expect(guard.admissionGuard({ state })).rejects.toThrow("audit down");
    expect((await storage.loadWorkflow("audit-down"))?.status).toBe("pending");

    auditDown = false;
    await expect(guard.admissionGuard({ state })).resolves.toBeUndefined();
  });

  it("blocks an owed cancelled recovery before any compensation user code runs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const actor = principal({ kind: "service-account", id: "cancelled-owner" });
    const guard = new RunExecutionSecurity({
      storage,
      security: security({
        principals: new Map([["service-account:cancelled-owner", actor]]),
        allowed: () => false,
      }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    let compensations = 0;
    const secured = workflow<void>({
      name: "cancelled-recovery",
      compensate: { onCancel: "rollback" },
    })
      .step("effect", () => succeed("done"), { compensate: async () => void compensations++ })
      .build();
    await storage.createWorkflow({
      workflowId: "cancelled-recovery",
      workflowName: secured.name,
      input: undefined,
      securityContext: { actor: principalRef(actor) },
    });
    await storage.saveStepResult({
      workflowId: "cancelled-recovery",
      stepName: "effect",
      result: "done",
      durationMs: 1,
      startedAt: new Date(),
    });
    await storage.suspendWorkflow({
      workflowId: "cancelled-recovery",
      stepName: "wait",
      stepUpdate: { status: "waiting", stepType: "signal", signalName: "resume" },
    });
    await storage.cancelWorkflow({ workflowId: "cancelled-recovery" });
    const runner = createWorkflowRunner({ storage, runAdmissionGuard: guard.admissionGuard });
    runner.exemptInternalWorkflowFromRunAdmission?.(secured);

    await runner.runSafe({
      workflow: secured,
      workflowId: "cancelled-recovery",
      input: undefined,
    });

    expect(compensations).toBe(0);
    expect(await storage.loadWorkflow("cancelled-recovery")).toMatchObject({
      status: "failed",
      errorTag: RUN_SECURITY_BLOCKED_ERROR_TAG,
      compensationOrigin: "cancel-settled",
    });
  });
});
