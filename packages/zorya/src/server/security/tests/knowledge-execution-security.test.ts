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
  beforeAuthorize?: () => void;
  identityFilter?: (input: {
    readonly context: { readonly effective: PrincipalRef };
    readonly namespaceId: string;
  }) => unknown;
}): ZoryaSecurity {
  const resolve = async (ref: PrincipalRef) => params.principals.get(`${ref.kind}:${ref.id}`);
  const authorizeEffective: NonNullable<ZoryaSecurity["authorizeEffective"]> = async ({
    context,
  }) => {
    params.beforeAuthorize?.();
    const effective = await resolve(context.effective);
    if (!effective) return { decision: { allowed: false, reason: "identity_unknown" } };
    if (params.allowed?.() === false) {
      return { decision: { allowed: false, reason: "permission_denied" }, effective };
    }
    return { decision: { allowed: true, reason: "access_role_allow" }, effective };
  };
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
    identityFilter: async ({ context, namespaceId }) =>
      (params.identityFilter?.({ context, namespaceId }) ?? {
        kind: "identities",
        namespaceId,
        identities: params.included?.() === false ? [] : [context.effective],
      }) as never,
    authorize: async (request) => (await authorizeEffective(request)).decision,
    authorizeEffective,
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

  it("denies when the principal snapshot changes during the allow audit", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        audit: async (event) => {
          audits.push(event);
          if (event.decision?.allowed) {
            principals.set("user:alice", user("alice", ["finance"]));
          }
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
    expect(audits[1]?.decision).toEqual({ allowed: false, reason: "permission_denied" });
    expect(Object.isFrozen(audits[1])).toBeTrue();
    expect(Object.isFrozen(audits[1]?.decision)).toBeTrue();
  });

  it("denies when the normalized identity filter changes during the allow audit", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    let expanded = false;
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        identityFilter: ({ context, namespaceId }) => ({
          kind: "identities",
          namespaceId,
          identities: expanded
            ? [context.effective, { kind: "user", id: "bob" }]
            : [context.effective],
        }),
        audit: async (event) => {
          audits.push(event);
          if (event.decision?.allowed) expanded = true;
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

  it("denies when final authorization differs from the audited principal snapshot", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    let authorizationCalls = 0;
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        beforeAuthorize: () => {
          authorizationCalls++;
          if (authorizationCalls === 2) {
            principals.set("user:alice", user("alice", ["finance"]));
          }
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

    expect(authorizationCalls).toBe(2);
    expect(access).toBeUndefined();
  });

  it("rejects an allowed result bound to a different effective identity", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([
      ["user:alice", user("alice", ["member"])],
      ["user:mallory", user("mallory", ["admin"])],
    ]);
    const security = fakeSecurity({ principals, audits });
    security.authorizeEffective = async () => ({
      decision: { allowed: true, reason: "access_role_allow" },
      effective: user("mallory", ["admin"]),
    });
    const guard = new KnowledgeExecutionSecurity({ security, clock, logger });

    const access = await guard.access({
      securityContext: { actor: { kind: "user", id: "alice" } },
      namespaceId: "team",
      baseId: "handbook",
    });

    expect(access).toBeUndefined();
    expect(audits[0]?.decision).toEqual({ allowed: false, reason: "permission_denied" });
  });

  it("fails closed for malformed identity-filter discriminators", async () => {
    const audits: SecurityAuditEvent[] = [];
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        audits,
        identityFilter: ({ namespaceId }) => ({ kind: "totally-unapproved", namespaceId }),
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
    expect(audits).toHaveLength(1);
    expect(audits[0]?.decision).toEqual({ allowed: false, reason: "permission_denied" });
  });

  it("fails closed for wrong namespaces and malformed identity allow-lists", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const malformed = [
      { kind: "namespace", namespaceId: "other-team" },
      { kind: "identities", namespaceId: "team", identities: "alice" },
      {
        kind: "identities",
        namespaceId: "team",
        identities: [{ kind: "unapproved-kind", id: "alice" }],
      },
    ];

    for (const identityFilter of malformed) {
      const guard = new KnowledgeExecutionSecurity({
        security: fakeSecurity({ principals, identityFilter: () => identityFilter }),
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
    }
  });

  it("fails closed when a custom security adapter lacks bound authorization", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    const legacy = fakeSecurity({ principals });
    const guard = new KnowledgeExecutionSecurity({
      security: { ...legacy, authorizeEffective: undefined },
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

  it("snapshots hostile identity-filter getters exactly once on both passes", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    let kindReads = 0;
    let namespaceReads = 0;
    let identitiesReads = 0;
    let idReads = 0;
    let lengthReads = 0;
    const guard = new KnowledgeExecutionSecurity({
      security: fakeSecurity({
        principals,
        identityFilter: ({ namespaceId }) => {
          let localKindReads = 0;
          let localIdReads = 0;
          const identity = new Proxy(
            {},
            {
              get: (_target, property) => {
                if (property === "kind") return "user";
                if (property === "id") {
                  idReads++;
                  return ++localIdReads === 1 ? "alice" : "mallory";
                }
                return undefined;
              },
            },
          );
          return new Proxy(
            {},
            {
              get: (_target, property) => {
                if (property === "kind") {
                  kindReads++;
                  return ++localKindReads === 1 ? "identities" : "totally-unapproved";
                }
                if (property === "namespaceId") {
                  namespaceReads++;
                  return namespaceId;
                }
                if (property === "identities") {
                  identitiesReads++;
                  let localLengthReads = 0;
                  return new Proxy([identity], {
                    get: (target, arrayProperty, receiver) => {
                      if (arrayProperty === "length") {
                        lengthReads++;
                        return ++localLengthReads === 1 ? 1 : Number.MAX_SAFE_INTEGER;
                      }
                      return Reflect.get(target, arrayProperty, receiver);
                    },
                  });
                }
                return undefined;
              },
            },
          );
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

    expect(access?.caller?.userId).toBe("alice");
    expect({ kindReads, namespaceReads, identitiesReads, idReads, lengthReads }).toEqual({
      kindReads: 2,
      namespaceReads: 2,
      identitiesReads: 2,
      idReads: 2,
      lengthReads: 2,
    });
  });

  it("fails closed for infinite and oversized proxy identity-array lengths", async () => {
    const principals = new Map<string, Principal>([["user:alice", user("alice", ["member"])]]);
    for (const length of [Number.POSITIVE_INFINITY, 10_001]) {
      let lengthReads = 0;
      const identities = new Proxy([{ kind: "user", id: "alice" }], {
        get: (target, property, receiver) => {
          if (property === "length") {
            lengthReads++;
            return length;
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const guard = new KnowledgeExecutionSecurity({
        security: fakeSecurity({
          principals,
          identityFilter: ({ namespaceId }) => ({
            kind: "identities",
            namespaceId,
            identities,
          }),
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
      expect(lengthReads).toBe(1);
    }
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
