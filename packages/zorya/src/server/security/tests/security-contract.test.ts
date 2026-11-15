import { describe, expect, it } from "bun:test";
import { Auth } from "../../auth.ts";
import {
  OPERATOR_PRINCIPAL,
  SECURITY_ACTIONS,
  WORKER_PRINCIPAL,
  createApiKeySecurity,
  requestPrincipal,
  requestSecurityContext,
  setRequestSecurityContext,
  type SecurityPermission,
  type SecurityResourceRef,
} from "../index.ts";

const OPERATOR_KEY = "operator-key";
const WORKER_KEY = "worker-key";

function request(key: string): Request {
  return new Request("http://zorya.test", {
    headers: { authorization: `Bearer ${key}` },
  });
}

function security(log?: (message: string) => void) {
  return createApiKeySecurity({
    operatorAuth: new Auth({ apiKeys: [OPERATOR_KEY] }),
    workerAuth: new Auth({ apiKeys: [WORKER_KEY] }),
    ...(log && { logger: { log } }),
  });
}

describe("security contract", () => {
  it("publishes exactly the approved resource/action vocabulary", () => {
    expect(SECURITY_ACTIONS).toEqual({
      namespace: ["read", "create", "update", "archive"],
      user: ["read", "disable", "bind"],
      "service-account": ["read", "create", "rotate", "delete"],
      agent: ["read", "create", "update", "promote", "rollback", "disable", "invoke"],
      "agent-role": ["read", "create", "update", "disable"],
      skill: ["read", "create", "update", "disable"],
      fragment: ["read", "create", "update", "disable"],
      toolset: ["read", "create", "promote", "rollback"],
      thread: ["read", "send", "rename", "archive", "view-trace"],
      memory: ["read", "write", "erase"],
      approval: ["decide", "decide-elevated"],
      knowledge: ["read", "create", "update", "delete", "grant", "use", "inspect-text"],
      connector: ["read", "register", "approve-tools", "disable", "remove"],
      secret: ["list", "write", "delete"],
      workflow: ["read", "create", "update"],
      dag: ["read", "create", "update"],
      run: ["read", "trigger", "cancel", "signal"],
      schedule: ["read", "create", "update", "delete"],
      eval: ["read", "create", "run"],
      webhook: ["read", "configure"],
      worker: ["read", "drain"],
      audit: ["search", "export"],
      usage: ["read"],
    });
  });

  it("keeps operator and worker authentication on separate planes", async () => {
    const adapter = security();
    expect(
      await adapter.authenticate({ request: request(OPERATOR_KEY), plane: "operator" }),
    ).toEqual(OPERATOR_PRINCIPAL);
    expect(await adapter.authenticate({ request: request(OPERATOR_KEY), plane: "worker" })).toBe(
      undefined,
    );
    expect(await adapter.authenticate({ request: request(WORKER_KEY), plane: "worker" })).toEqual(
      WORKER_PRINCIPAL,
    );
    expect(
      await adapter.authenticate({ request: request(WORKER_KEY), plane: "operator" }),
    ).toBeUndefined();
  });

  it("allows every approved operator permission with a stable reason and denies workers", async () => {
    const adapter = security();
    for (const [resource, actions] of Object.entries(SECURITY_ACTIONS)) {
      for (const action of actions) {
        const permission = { resource, action } as SecurityPermission;
        const target = {
          kind: resource,
          namespaceId: "team-a",
          id: "resource-1",
        } as SecurityResourceRef;
        expect(
          await adapter.authorize({
            context: { actor: OPERATOR_PRINCIPAL, effective: OPERATOR_PRINCIPAL },
            permission,
            resource: target,
          }),
        ).toEqual({ allowed: true, reason: "operator_allow" });
        expect(
          await adapter.authorize({
            context: { actor: WORKER_PRINCIPAL, effective: WORKER_PRINCIPAL },
            permission,
            resource: target,
          }),
        ).toEqual({ allowed: false, reason: "wrong_plane" });
      }
    }
  });

  it("requires act-for and keeps actor separate from effective identity", async () => {
    const adapter = security();
    const represented = { kind: "user", id: "user-7" } as const;
    expect(
      await adapter.resolveIdentity({ actor: OPERATOR_PRINCIPAL, asserted: represented }),
    ).toEqual({ actor: OPERATOR_PRINCIPAL, effective: represented });
    expect(
      await adapter.resolveIdentity({ actor: WORKER_PRINCIPAL, asserted: represented }),
    ).toBeUndefined();
  });

  it("uses an exactly namespace-bound default identity filter", async () => {
    expect(
      await security().identityFilter({
        context: { actor: OPERATOR_PRINCIPAL, effective: OPERATOR_PRINCIPAL },
        namespaceId: "team-a",
      }),
    ).toEqual({ kind: "namespace", namespaceId: "team-a" });
  });

  it("records structured audit events through the host logger", async () => {
    const messages: string[] = [];
    const adapter = security((message) => messages.push(message));
    await adapter.recordAudit({
      event: "authorization",
      occurredAt: "2026-11-15T00:00:00.000Z",
      context: { actor: OPERATOR_PRINCIPAL, effective: OPERATOR_PRINCIPAL },
      namespaceId: "team-a",
      permission: { resource: "agent", action: "invoke" },
      decision: { allowed: true, reason: "operator_allow" },
    });
    expect(JSON.parse(messages[0]!)).toMatchObject({
      component: "zorya.security",
      event: "authorization",
      namespaceId: "team-a",
      decision: { allowed: true, reason: "operator_allow" },
    });
  });

  it("stores actor and effective identity together on the request", () => {
    const req = new Request("http://zorya.test/api/agents");
    const effective = { kind: "user", id: "user-7" } as const;
    setRequestSecurityContext({
      req,
      context: { actor: OPERATOR_PRINCIPAL, effective },
    });
    expect(requestSecurityContext(req)).toEqual({ actor: OPERATOR_PRINCIPAL, effective });
    expect(requestPrincipal(req)).toEqual(OPERATOR_PRINCIPAL);
  });
});
