import { describe, expect, it } from "bun:test";
import type { Principal, PrincipalRef, SecurityAuditEvent, ZoryaSecurity } from "../security.ts";
import { OPERATOR_PRINCIPAL, principalRef } from "../security.ts";
import { KnowledgeExecutionSecurity } from "../knowledge-execution-security.ts";

const clock = { now: () => new Date("2026-01-02T03:04:05.000Z") };
const logger = { error() {} };

function user(id: string, roles: ReadonlyArray<string>): Principal {
  return {
    kind: "user",
    id,
    email: `${id}@example.test`,
    accessRoles: { team: roles },
    canActFor: false,
  };
}

function fakeSecurity(params: {
  principals: Map<string, Principal>;
  allowed?: () => boolean;
  included?: () => boolean;
  audits?: SecurityAuditEvent[];
  audit?: (event: SecurityAuditEvent) => Promise<void>;
}): ZoryaSecurity {
  const resolve = async (ref: PrincipalRef) => params.principals.get(`${ref.kind}:${ref.id}`);
  return {
    authenticate: async () => undefined,
    resolvePrincipal: resolve,
    resolveIdentity: async ({ actor, asserted }) => {
      const effective = asserted ?? principalRef(actor);
      if ((effective.kind !== actor.kind || effective.id !== actor.id) && !actor.canActFor) {
        return undefined;
      }
      return { actor, effective };
    },
    identityFilter: async ({ context, namespaceId }) => ({
      kind: "identities",
      namespaceId,
      identities: params.included?.() === false ? [] : [context.effective],
    }),
    authorize: async () =>
      params.allowed?.() === false
        ? { allowed: false, reason: "permission_denied" }
        : { allowed: true, reason: "access_role_allow" },
    recordAudit: params.audit ?? (async (event) => void params.audits?.push(event)),
  };
}

describe("KnowledgeExecutionSecurity", () => {
  it("freshly resolves caller roles and observes grant revocation before the next search", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    let allowed = true;
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({ principals, audits, allowed: () => allowed }),
      clock,
      logger,
    });

    const first = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });
    principals.set("user:alice", user("alice", ["finance"]));
    const second = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });
    allowed = false;
    const revoked = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(first?.caller).toMatchObject({ userId: "alice", accessRoles: ["member"] });
    expect(second?.caller).toMatchObject({ userId: "alice", accessRoles: ["finance"] });
    expect(revoked).toBeUndefined();
    expect(audits.map((event) => event.decision?.allowed)).toEqual([true, true, false]);
    expect(JSON.stringify(audits)).not.toContain("query");
    expect(JSON.stringify(audits)).not.toContain("document");
    expect(audits[2]).toMatchObject({
      event: "knowledge.retrieval.authorization",
      resource: { kind: "knowledge", namespaceId: "team", id: "handbook" },
      metadata: { baseId: "handbook" },
    });
  });

  it("reauthorizes after an awaited allow audit before returning caller access", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    let allowed = true;
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        allowed: () => allowed,
        audit: async (event) => {
          audits.push(event);
          if (event.decision?.allowed) allowed = false;
        },
      }),
      clock,
      logger,
    });

    const access = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(access).toBeUndefined();
    expect(audits.map((event) => event.decision?.allowed)).toEqual([true, false]);
  });

  it("builds the returned caller from the principal refreshed after audit", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        audit: async () => {
          principals.set("user:alice", user("alice", ["finance"]));
        },
      }),
      clock,
      logger,
    });

    const access = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(access?.caller).toMatchObject({
      subject: { kind: "user", id: "alice" },
      userId: "alice",
      accessRoles: ["finance"],
    });
  });

  it("keeps same-id users and service accounts distinct for retriever ACLs", async () => {
    const principals = new Map<string, Principal>([
      ["user:shared", user("shared", ["member"])],
      [
        "service-account:shared",
        {
          kind: "service-account",
          id: "shared",
          accessRoles: { team: ["automation"] },
          canActFor: false,
        },
      ],
    ]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({ principals }),
      clock,
      logger,
    });

    const userAccess = await guard.access({
      securityContext: { actor: { kind: "user", id: "shared" } },
      namespaceId: "team",
      baseId: "handbook",
    });
    const serviceAccess = await guard.access({
      securityContext: { actor: { kind: "service-account", id: "shared" } },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(userAccess?.caller?.subject).toEqual({ kind: "user", id: "shared" });
    expect(serviceAccess?.caller?.subject).toEqual({
      kind: "service-account",
      id: "shared",
    });
    expect(userAccess?.caller?.userId).toBe(serviceAccess?.caller?.userId);
  });

  it("fails closed for missing or filtered enterprise identities", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({ principals, audits, included: () => false }),
      clock,
      logger,
    });

    expect(await guard.access({ namespaceId: "team", baseId: "handbook" })).toBeUndefined();
    expect(
      await guard.access({
        securityContext: { actor: { kind: "user", id: "alice" } },
        namespaceId: "team",
        baseId: "handbook",
      }),
    ).toBeUndefined();
    expect(
      await guard.access({
        securityContext: { actor: { kind: "user", id: "unknown" } },
        namespaceId: "team",
        baseId: "handbook",
      }),
    ).toBeUndefined();
    expect(audits).toHaveLength(3);
    expect(audits.every((event) => event.decision?.allowed === false)).toBe(true);
    expect("context" in audits[2]!).toBe(false);
    expect(audits[2]?.metadata).toMatchObject({ actorKind: "user", actorId: "unknown" });
  });

  it("keeps key mode namespace-only even when a body names an unknown user", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["operator:operator", OPERATOR_PRINCIPAL]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({ principals, audits }),
      clock,
      logger,
      legacyPrincipal: principalRef(OPERATOR_PRINCIPAL),
    });

    const access = await guard.access({
      securityContext: {
        actor: { kind: "operator", id: "operator" },
        effective: { kind: "user", id: "unknown" },
      },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(access?.caller).toMatchObject({ userId: "operator", accessRoles: ["admin"] });
    expect(audits[0]?.context?.effective).toEqual({ kind: "operator", id: "operator" });
  });

  it("never turns an authorization denial into access when its audit sink is down", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        allowed: () => false,
        audit: async () => {
          throw new Error("audit down");
        },
      }),
      clock,
      logger,
    });

    await expect(
      guard.access({
        securityContext: { actor: { kind: "user", id: "alice" } },
        namespaceId: "team",
        baseId: "handbook",
      }),
    ).resolves.toBeUndefined();
  });
});
