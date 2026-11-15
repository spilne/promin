import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
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
});
