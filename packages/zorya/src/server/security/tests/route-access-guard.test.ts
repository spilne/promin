import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Auth } from "../../auth.ts";
import { Router } from "../../router.ts";
import {
  OPERATOR_PRINCIPAL,
  createApiKeySecurity,
  createSecurityRouteAccessGuard,
  setRequestPrincipal,
  setRequestSecurityContext,
  type AuthorizationDecision,
  type SecurityAuditEvent,
  type ZoryaSecurity,
} from "../index.ts";

const clock = FakeWallClock.create(Date.parse("2026-11-15T12:00:00.000Z"));

function security(params: {
  readonly decision: AuthorizationDecision;
  readonly audit?: (event: SecurityAuditEvent) => Promise<void>;
  readonly events: string[];
}): ZoryaSecurity {
  const base = createApiKeySecurity({ operatorAuth: new Auth({ apiKeys: ["key"] }) });
  return {
    ...base,
    authorize: async (request) => {
      params.events.push("authorize");
      expect(request.permission).toEqual({ resource: "agent", action: "read" });
      expect(request.resource).toEqual({
        kind: "agent",
        namespaceId: "team-a",
        id: "agent-1",
      });
      return params.decision;
    },
    recordAudit: async (event) => {
      params.events.push("audit");
      await params.audit?.(event);
    },
  };
}

function request(): Request {
  const req = new Request("http://zorya.test/agents/agent-1");
  setRequestPrincipal({ req, principal: OPERATOR_PRINCIPAL });
  return req;
}

function router(params: {
  readonly security: ZoryaSecurity;
  readonly events: string[];
  readonly errors?: unknown[];
  readonly loggerThrows?: boolean;
}): Router {
  return new Router({
    logger: { error() {} },
    accessGuard: createSecurityRouteAccessGuard({
      security: params.security,
      clock,
      logger: {
        error: (...args) => {
          params.errors?.push(args);
          if (params.loggerThrows) throw new Error("logger offline");
        },
      },
    }),
  }).route({
    method: "GET",
    path: "/agents/:id",
    access: {
      kind: "protected",
      permission: { resource: "agent", action: "read" },
      resolveTarget: ({ params: route }) => {
        params.events.push("resolve");
        return {
          resource: { kind: "agent", namespaceId: "team-a", id: route.id },
        };
      },
    },
    handler: () => {
      params.events.push("handler");
      return new Response(null, { status: 204 });
    },
  });
}

describe("security route access guard", () => {
  it("authorizes and audits exactly once before an allowed handler", async () => {
    const events: string[] = [];
    let recorded: SecurityAuditEvent | undefined;
    const response = await router({
      events,
      security: security({
        events,
        decision: { allowed: true, reason: "operator_allow" },
        audit: async (event) => {
          recorded = event;
        },
      }),
    }).handle(request());

    expect(response.status).toBe(204);
    expect(events).toEqual(["resolve", "authorize", "audit", "handler"]);
    expect(recorded).toMatchObject({
      event: "authorization",
      occurredAt: "2026-11-15T12:00:00.000Z",
      context: { actor: OPERATOR_PRINCIPAL, effective: { kind: "operator", id: "operator" } },
      namespaceId: "team-a",
      permission: { resource: "agent", action: "read" },
      resource: { kind: "agent", namespaceId: "team-a", id: "agent-1" },
      decision: { allowed: true, reason: "operator_allow" },
      metadata: { method: "GET", path: "/agents/agent-1" },
    });
  });

  it("returns a generic denial and never invokes the handler", async () => {
    const events: string[] = [];
    const response = await router({
      events,
      security: security({
        events,
        decision: { allowed: false, reason: "principal_disabled" },
      }),
    }).handle(request());

    expect(response.status).toBe(403);
    expect(await response.json()).toEqual({ error: "forbidden" });
    expect(events).toEqual(["resolve", "authorize", "audit"]);
  });

  it("fails closed on malformed decisions and reads stateful fields exactly once", async () => {
    const events: string[] = [];
    const reads = { allowed: 0, reason: 0 };
    const malformed = {
      get allowed() {
        reads.allowed++;
        return reads.allowed === 1 ? "yes" : true;
      },
      get reason() {
        reads.reason++;
        return reads.reason === 1 ? "operator_allow" : "permission_denied";
      },
    };
    const response = await router({
      events,
      security: security({
        events,
        decision: malformed as unknown as AuthorizationDecision,
      }),
    }).handle(request());

    expect(response.status).toBe(500);
    expect(events).toEqual(["resolve", "authorize"]);
    expect(reads).toEqual({ allowed: 1, reason: 1 });
  });

  it("uses one frozen decision snapshot for both branching and retained audit", async () => {
    const events: string[] = [];
    const reads = { allowed: 0, reason: 0 };
    let retained: SecurityAuditEvent | undefined;
    const stateful = {
      get allowed() {
        reads.allowed++;
        return reads.allowed === 1;
      },
      get reason() {
        reads.reason++;
        return reads.reason === 1 ? "operator_allow" : "not-approved";
      },
    };
    const response = await router({
      events,
      security: security({
        events,
        decision: stateful as unknown as AuthorizationDecision,
        audit: async (event) => {
          retained = event;
        },
      }),
    }).handle(request());

    expect(response.status).toBe(204);
    expect(events).toEqual(["resolve", "authorize", "audit", "handler"]);
    expect(reads).toEqual({ allowed: 1, reason: 1 });
    expect(retained?.decision).toEqual({ allowed: true, reason: "operator_allow" });
    expect(Object.isFrozen(retained?.decision)).toBeTrue();
  });

  it("fails an allowed request closed when audit persistence fails", async () => {
    const events: string[] = [];
    const errors: unknown[] = [];
    const response = await router({
      events,
      errors,
      security: security({
        events,
        decision: { allowed: true, reason: "operator_allow" },
        audit: async () => {
          throw new Error("audit offline");
        },
      }),
    }).handle(request());

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "security_audit_unavailable" });
    expect(events).toEqual(["resolve", "authorize", "audit"]);
    expect(errors).toHaveLength(1);
  });

  it("keeps a denied request denied when denial audit logging fails", async () => {
    const events: string[] = [];
    const errors: unknown[] = [];
    const response = await router({
      events,
      errors,
      loggerThrows: true,
      security: security({
        events,
        decision: { allowed: false, reason: "permission_denied" },
        audit: async () => {
          throw new Error("audit offline");
        },
      }),
    }).handle(request());

    expect(response.status).toBe(403);
    expect(events).toEqual(["resolve", "authorize", "audit"]);
    expect(errors).toHaveLength(1);
  });

  it("keeps an allowed request blocked when both audit and host logging fail", async () => {
    const events: string[] = [];
    const response = await router({
      events,
      loggerThrows: true,
      security: security({
        events,
        decision: { allowed: true, reason: "operator_allow" },
        audit: async () => {
          throw new Error("audit offline");
        },
      }),
    }).handle(request());

    expect(response.status).toBe(503);
    expect(events).toEqual(["resolve", "authorize", "audit"]);
  });

  it("rejects missing request context without consulting policy or running the handler", async () => {
    const events: string[] = [];
    const response = await router({
      events,
      security: security({ events, decision: { allowed: true, reason: "operator_allow" } }),
    }).handle(new Request("http://zorya.test/agents/agent-1"));

    expect(response.status).toBe(403);
    expect(events).toEqual(["resolve"]);
  });

  it("retains an immutable identity snapshot independent of caller mutations", async () => {
    const events: string[] = [];
    const roles = ["member"];
    const actor = {
      kind: "user" as const,
      id: "user-1",
      email: "before@example.com",
      accessRoles: { "team-a": roles },
      canActFor: false,
    };
    const req = new Request("http://zorya.test/agents/agent-1");
    setRequestSecurityContext({ req, context: { actor, effective: actor } });
    let retained: SecurityAuditEvent | undefined;
    const response = await router({
      events,
      security: security({
        events,
        decision: { allowed: true, reason: "access_role_allow" },
        audit: async (event) => {
          retained = event;
        },
      }),
    }).handle(req);

    roles.push("admin");
    (actor as { email: string }).email = "after@example.com";
    expect(response.status).toBe(204);
    expect(retained?.context?.actor.email).toBe("before@example.com");
    expect(retained?.context?.actor.accessRoles["team-a"]).toEqual(["member"]);
    expect(Object.isFrozen(retained)).toBeTrue();
    expect(Object.isFrozen(retained?.context)).toBeTrue();
    expect(Object.isFrozen(retained?.context?.actor)).toBeTrue();
    expect(Object.isFrozen(retained?.context?.actor.accessRoles)).toBeTrue();
    expect(Object.isFrozen(retained?.context?.actor.accessRoles["team-a"])).toBeTrue();
    expect(Object.isFrozen(retained?.permission)).toBeTrue();
    expect(Object.isFrozen(retained?.resource)).toBeTrue();
    expect(Object.isFrozen(retained?.metadata)).toBeTrue();
  });

  it("snapshots hostile context and role getters once before authorization", async () => {
    const events: string[] = [];
    const reads = {
      actor: 0,
      effective: 0,
      kind: 0,
      id: 0,
      email: 0,
      accessRoles: 0,
      canActFor: 0,
      role: 0,
      effectiveKind: 0,
      effectiveId: 0,
    };
    const roleValues: string[] = [];
    Object.defineProperty(roleValues, "0", {
      enumerable: true,
      get: () => {
        reads.role++;
        return reads.role === 1 ? "member" : 42;
      },
    });
    roleValues.length = 1;
    const actor = {
      get kind() {
        reads.kind++;
        return reads.kind === 1 ? "user" : "evil";
      },
      get id() {
        reads.id++;
        return reads.id === 1 ? "user-1" : "";
      },
      get email() {
        reads.email++;
        return reads.email === 1 ? "user@example.com" : 42;
      },
      get accessRoles() {
        reads.accessRoles++;
        return reads.accessRoles === 1 ? { "team-a": roleValues } : null;
      },
      get canActFor() {
        reads.canActFor++;
        return reads.canActFor === 1 ? false : "yes";
      },
    };
    const effective = {
      get kind() {
        reads.effectiveKind++;
        return reads.effectiveKind === 1 ? "user" : "evil";
      },
      get id() {
        reads.effectiveId++;
        return reads.effectiveId === 1 ? "user-1" : "";
      },
    };
    const context = {
      get actor() {
        reads.actor++;
        return actor;
      },
      get effective() {
        reads.effective++;
        return effective;
      },
    };
    const req = new Request("http://zorya.test/agents/agent-1");
    setRequestSecurityContext({ req, context: context as never });
    const response = await router({
      events,
      security: security({
        events,
        decision: { allowed: true, reason: "access_role_allow" },
      }),
    }).handle(req);

    expect(response.status).toBe(204);
    expect(reads).toEqual({
      actor: 1,
      effective: 1,
      kind: 1,
      id: 1,
      email: 1,
      accessRoles: 1,
      canActFor: 1,
      role: 1,
      effectiveKind: 1,
      effectiveId: 1,
    });
  });

  it("does not apply RBAC to explicitly classified non-protected routes", async () => {
    const events: string[] = [];
    const policy = security({ events, decision: { allowed: false, reason: "permission_denied" } });
    const access = [
      { kind: "public", purpose: "lifecycle" },
      { kind: "worker" },
      { kind: "signed-webhook" },
      { kind: "signal-token" },
    ] as const;
    const admitted: string[] = [];
    const routes = new Router({
      accessGuard: createSecurityRouteAccessGuard({
        security: policy,
        clock,
        logger: { error() {} },
      }),
    });
    access.forEach((metadata, index) => {
      routes.route({
        method: "GET",
        path: `/classified/${index}`,
        access: metadata,
        handler: () => {
          admitted.push(metadata.kind);
          return new Response(null, { status: 204 });
        },
      });
    });

    for (let index = 0; index < access.length; index++) {
      expect(
        (await routes.handle(new Request(`http://zorya.test/classified/${index}`))).status,
      ).toBe(204);
    }
    expect(admitted).toEqual(["public", "worker", "signed-webhook", "signal-token"]);
    expect(events).toEqual([]);
  });
});
