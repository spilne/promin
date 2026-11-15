import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import type { DurableScheduleConfig } from "@promin/workflow/scheduler";
import {
  ScheduleExecutionSecurity,
  ScheduleSecurityDeniedError,
} from "../schedule-execution-security.ts";
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

function security(params: {
  principals: Map<string, Principal>;
  allowed?: () => boolean;
  audits?: SecurityAuditEvent[];
}): ZoryaSecurity {
  const resolve = async (ref: PrincipalRef) => params.principals.get(`${ref.kind}:${ref.id}`);
  return {
    authenticate: async () => undefined,
    resolvePrincipal: resolve,
    resolveIdentity: async ({ actor, asserted }) => {
      if (asserted && !actor.canActFor) return undefined;
      return { actor, effective: asserted ?? principalRef(actor) };
    },
    identityFilter: async ({ namespaceId }) => ({
      kind: "namespace",
      namespaceId,
    }),
    authorize: async () =>
      params.allowed?.() === false
        ? { allowed: false, reason: "permission_denied" }
        : { allowed: true, reason: "access_role_allow" },
    recordAudit: async (event) => void params.audits?.push(event),
  };
}

describe("ScheduleExecutionSecurity", () => {
  it("requires an explicit service account in module mode", async () => {
    const guard = new ScheduleExecutionSecurity({
      security: security({ principals: new Map() }),
      clock: SystemWallClock,
      logger: quietLogger,
    });

    await expect(
      guard.assignedContext({ scheduleId: "nightly", namespaceId: "team" }),
    ).rejects.toMatchObject({
      code: "assignment_required",
    });
    await expect(
      guard.authorizeDispatch({
        id: "legacy",
        intervalMs: 60_000,
        namespace: "team",
      }),
    ).rejects.toBeInstanceOf(ScheduleSecurityDeniedError);
  });

  it("adopts the canonical operator only when legacy key mode configures it", async () => {
    const audits: SecurityAuditEvent[] = [];
    const guard = new ScheduleExecutionSecurity({
      security: security({
        principals: new Map([["operator:operator", OPERATOR_PRINCIPAL]]),
        audits,
      }),
      clock: SystemWallClock,
      logger: quietLogger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });

    expect(await guard.assignedContext({ scheduleId: "new", namespaceId: "team" })).toEqual({
      actor: { kind: "operator", id: "operator" },
    });
    const authorized = await guard.authorizeDispatch({
      id: "legacy",
      intervalMs: 60_000,
      namespace: "team",
    });
    expect(authorized.schedule.securityContext).toEqual({
      actor: { kind: "operator", id: "operator" },
    });
    expect(Object.isFrozen(authorized.schedule.securityContext?.actor)).toBe(true);
    expect(audits.map((event) => event.event)).toEqual([
      "schedule.assignment.authorization",
      "schedule.execution.authorization",
    ]);
  });

  it("re-resolves the assigned actor and current act-for authority on every dispatch", async () => {
    const serviceAccount = principal({ kind: "service-account", id: "nightly-sa" }, true);
    const user = principal({ kind: "user", id: "alice" });
    const principals = new Map([
      ["service-account:nightly-sa", serviceAccount],
      ["user:alice", user],
    ]);
    const guard = new ScheduleExecutionSecurity({
      security: security({ principals }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    const context = await guard.assignedContext({
      scheduleId: "nightly",
      namespaceId: "team",
      serviceAccountId: "nightly-sa",
      runAsUserId: "alice",
    });
    const schedule = {
      id: "nightly",
      intervalMs: 60_000,
      namespace: "team",
      securityContext: context,
    };

    await expect(guard.authorizeDispatch(schedule)).resolves.toMatchObject({
      identity: {
        principal: { kind: "user", id: "alice" },
        runContext: {
          actor: { kind: "service-account", id: "nightly-sa" },
          effective: { kind: "user", id: "alice" },
        },
      },
    });

    principals.set("service-account:nightly-sa", {
      ...serviceAccount,
      canActFor: false,
    });
    await expect(guard.authorizeDispatch(schedule)).rejects.toBeInstanceOf(
      ScheduleSecurityDeniedError,
    );

    principals.delete("service-account:nightly-sa");
    await expect(guard.authorizeDispatch(schedule)).rejects.toBeInstanceOf(
      ScheduleSecurityDeniedError,
    );
  });

  it("binds agent resource scope to a freshly authorized effective user", async () => {
    const serviceAccount = principal({ kind: "service-account", id: "nightly-sa" }, false);
    const user = principal({ kind: "user", id: "alice" });
    const principals = new Map([
      ["service-account:nightly-sa", serviceAccount],
      ["user:alice", user],
    ]);
    const guard = new ScheduleExecutionSecurity({
      security: security({ principals }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    const schedule = {
      id: "nightly",
      intervalMs: 60_000,
      namespace: "team",
      metadata: {
        agentTrigger: true,
        agentId: "writer",
        task: "summarize",
        namespaceId: "team",
        resourceId: "alice",
        createdByAgent: "writer",
      },
      securityContext: { actor: { kind: "service-account" as const, id: "nightly-sa" } },
    };

    await expect(guard.authorizeDispatch(schedule)).rejects.toBeInstanceOf(
      ScheduleSecurityDeniedError,
    );

    principals.set("service-account:nightly-sa", { ...serviceAccount, canActFor: true });
    await expect(guard.authorizeDispatch(schedule)).resolves.toMatchObject({
      identity: {
        principal: { kind: "user", id: "alice" },
        runContext: {
          actor: { kind: "service-account", id: "nightly-sa" },
          effective: { kind: "user", id: "alice" },
        },
      },
    });
  });

  it("keeps explicit denial denied when denial audit fails, but fails closed on allow audit failure", async () => {
    const serviceAccount = principal({ kind: "service-account", id: "nightly-sa" });
    const auditError = new Error("audit unavailable");
    const makeGuard = (allowed: boolean) =>
      new ScheduleExecutionSecurity({
        security: {
          ...security({
            principals: new Map([["service-account:nightly-sa", serviceAccount]]),
            allowed: () => allowed,
          }),
          recordAudit: async () => {
            throw auditError;
          },
        },
        clock: SystemWallClock,
        logger: quietLogger,
      });
    const schedule = {
      id: "nightly",
      intervalMs: 60_000,
      namespace: "team",
      securityContext: { actor: { kind: "service-account" as const, id: "nightly-sa" } },
    };

    await expect(makeGuard(false).authorizeDispatch(schedule)).rejects.toBeInstanceOf(
      ScheduleSecurityDeniedError,
    );
    await expect(makeGuard(true).authorizeDispatch(schedule)).rejects.toBe(auditError);
  });

  it("snapshots hostile schedule fields exactly once and deeply freezes detached dispatch data", async () => {
    const principals = new Map([
      ["service-account:nightly-sa", principal({ kind: "service-account", id: "nightly-sa" })],
    ]);
    const guard = new ScheduleExecutionSecurity({
      security: security({ principals }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    let namespaceReads = 0;
    let contextReads = 0;
    let metadataReads = 0;
    let payloadReads = 0;
    const safeMetadata = {
      workflowName: "nightly-workflow",
      get payload() {
        payloadReads++;
        return payloadReads === 1 ? { nested: ["safe"] } : { nested: ["changed"] };
      },
    };
    const hostile = {
      id: "nightly",
      intervalMs: 60_000,
      get namespace() {
        namespaceReads++;
        return namespaceReads === 1 ? "team" : "other-team";
      },
      get securityContext() {
        contextReads++;
        return contextReads === 1
          ? { actor: { kind: "service-account" as const, id: "nightly-sa" } }
          : { actor: { kind: "operator" as const, id: "operator" } };
      },
      get metadata() {
        metadataReads++;
        return metadataReads === 1 ? safeMetadata : { workflowName: "changed" };
      },
    } as unknown as DurableScheduleConfig;

    const authorized = await guard.authorizeDispatch(hostile);
    expect({ namespaceReads, contextReads, metadataReads, payloadReads }).toEqual({
      namespaceReads: 1,
      contextReads: 1,
      metadataReads: 1,
      payloadReads: 1,
    });
    expect(authorized.schedule).toMatchObject({
      namespace: "team",
      metadata: { workflowName: "nightly-workflow", payload: { nested: ["safe"] } },
      securityContext: { actor: { kind: "service-account", id: "nightly-sa" } },
    });
    expect(Object.isFrozen(authorized.schedule)).toBe(true);
    expect(Object.isFrozen(authorized.schedule.metadata)).toBe(true);
    const payload = authorized.schedule.metadata?.payload as {
      nested: ReadonlyArray<string>;
    };
    expect(Object.isFrozen(payload)).toBe(true);
    expect(Object.isFrozen(payload.nested)).toBe(true);
  });
});
