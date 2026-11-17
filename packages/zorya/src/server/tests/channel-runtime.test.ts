import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import {
  CoreChannelRuntime,
  InMemoryChannelStateStore,
  channelThreadId,
  type ChannelInboundEvent,
  type ChannelTurnInvocation,
  type CoreChannelRuntimeConfig,
} from "../channel-runtime.ts";
import type {
  AuthorizationDecision,
  Principal,
  SecurityAuditEvent,
  SecurityContext,
} from "../security/index.ts";

const actor: Principal = Object.freeze({
  kind: "service-account",
  id: "channel-slack",
  accessRoles: Object.freeze({ org: Object.freeze(["member"]) }),
  canActFor: true,
});

const user: Principal = Object.freeze({
  kind: "user",
  id: "alice",
  email: "alice@example.test",
  accessRoles: Object.freeze({ team: Object.freeze(["member"]) }),
  canActFor: false,
});

function event(eventId: string, patch: Partial<ChannelInboundEvent> = {}): ChannelInboundEvent {
  return {
    eventId,
    conversation: { id: "conversation-1", kind: "direct" },
    sender: { id: "external-alice", kind: "user" },
    target: { namespaceId: "team", agentId: "helper" },
    message: { text: "What is our travel policy?" },
    ...patch,
  };
}

function environment(
  params: {
    readonly decision?: AuthorizationDecision;
    readonly resolve?: boolean;
    readonly auditFails?: boolean;
    readonly invokeFails?: boolean;
    readonly securityFails?: "link" | "resolve" | "authorize";
  } = {},
) {
  const store = new InMemoryChannelStateStore();
  store.linkIdentity({ channelId: "slack", externalUserId: "external-alice" }, user);
  const invocations: ChannelTurnInvocation[] = [];
  const audits: SecurityAuditEvent[] = [];
  const context: SecurityContext = { actor, effective: { kind: "user", id: user.id } };
  const runtimeStore =
    params.securityFails === "link"
      ? {
          resolveLinkedUser: async () => Promise.reject(new Error("store secret")),
          claimEvent: (input: Parameters<typeof store.claimEvent>[0]) => store.claimEvent(input),
          completeEvent: (input: Parameters<typeof store.completeEvent>[0]) =>
            store.completeEvent(input),
        }
      : store;
  const config: CoreChannelRuntimeConfig = {
    channelId: "slack",
    actor,
    store: runtimeStore,
    clock: FakeWallClock.create(1_800_000_000_000),
    security: {
      resolveIdentity: async () => {
        if (params.securityFails === "resolve") throw new Error("directory secret");
        return params.resolve === false ? undefined : context;
      },
      authorize: async () => {
        if (params.securityFails === "authorize") throw new Error("policy secret");
        return params.decision ?? { allowed: true, reason: "access_role_allow" };
      },
      recordAudit: async (entry) => {
        if (params.auditFails) throw new Error("audit payload must not escape");
        audits.push(entry);
      },
    },
    invoker: {
      invoke: async (input) => {
        invocations.push(input);
        if (params.invokeFails) throw new Error("provider returned a sensitive payload");
        return { runId: `run-${input.eventId}` };
      },
    },
  };
  return { runtime: new CoreChannelRuntime(config), store, invocations, audits };
}

describe("CoreChannelRuntime", () => {
  it("admits a linked user with act-for context and event/run correlation", async () => {
    const env = environment();
    const result = await env.runtime.handle(event("evt-1"));

    expect(result).toEqual({
      kind: "accepted",
      correlationId: expect.any(String),
      threadId: expect.any(String),
      runId: "run-evt-1",
    });
    expect(env.invocations).toHaveLength(1);
    expect(env.invocations[0]).toMatchObject({
      channelId: "slack",
      eventId: "evt-1",
      namespaceId: "team",
      agentId: "helper",
      message: "What is our travel policy?",
      securityContext: { actor: { id: "channel-slack" }, effective: { id: "alice" } },
      knowledgeAudience: { kind: "effective-user" },
    });
    expect(env.audits).toHaveLength(1);
    expect(env.audits[0]).toMatchObject({
      event: "channel.event.authorization",
      decision: { allowed: true },
      metadata: { channelId: "slack", conversationKind: "direct" },
    });
    expect(env.store.event("slack", "evt-1")).toMatchObject({
      status: "invoked",
      runId: "run-evt-1",
    });
  });

  it("uses one opaque thread per conversation/user and restricts shared knowledge", async () => {
    const env = environment();
    env.store.linkIdentity(
      { channelId: "slack", externalUserId: "external-bob" },
      { kind: "user", id: "bob" },
    );
    await env.runtime.handle(
      event("evt-shared", {
        conversation: { id: "shared-conversation", kind: "shared" },
      }),
    );
    await env.runtime.handle(
      event("evt-same-user", {
        conversation: { id: "shared-conversation", kind: "direct" },
      }),
    );

    const bobActor: Principal = { ...actor, id: "channel-slack-bob" };
    const bobContext: SecurityContext = {
      actor: bobActor,
      effective: { kind: "user", id: "bob" },
    };
    const bobInvocations: ChannelTurnInvocation[] = [];
    const bobRuntime = new CoreChannelRuntime({
      channelId: "slack",
      actor: bobActor,
      store: env.store,
      security: {
        resolveIdentity: async () => bobContext,
        authorize: async () => ({ allowed: true, reason: "access_role_allow" }),
        recordAudit: async () => {},
      },
      invoker: {
        invoke: async (input) => {
          bobInvocations.push(input);
          return { runId: "run-bob" };
        },
      },
    });
    await bobRuntime.handle(
      event("evt-bob", {
        conversation: { id: "shared-conversation", kind: "shared" },
        sender: { id: "external-bob", kind: "user" },
      }),
    );

    expect(env.invocations[0]?.knowledgeAudience).toEqual({ kind: "org-wide-only" });
    expect(env.invocations[0]?.threadId).toBe(env.invocations[1]?.threadId);
    expect(bobInvocations[0]?.threadId).not.toBe(env.invocations[0]?.threadId);
    expect(env.invocations[0]?.threadId).toBe(
      channelThreadId({
        channelId: "slack",
        conversationId: "shared-conversation",
        userId: "alice",
      }),
    );
    expect(env.invocations[0]?.threadId).not.toContain("alice");
    expect(env.invocations[0]?.threadId).not.toContain("shared-conversation");
  });

  it("refuses unlinked, guest, bot and currently invalid identities before invocation", async () => {
    const env = environment();
    expect(
      await env.runtime.handle(event("evt-unlinked", { sender: { id: "unknown", kind: "user" } })),
    ).toMatchObject({ kind: "unlinked" });
    expect(
      await env.runtime.handle(event("evt-guest", { sender: { id: "guest", kind: "guest" } })),
    ).toMatchObject({ kind: "refused", reason: "sender_not_allowed" });
    expect(
      await env.runtime.handle(event("evt-bot", { sender: { id: "bot", kind: "bot" } })),
    ).toMatchObject({ kind: "refused", reason: "sender_not_allowed" });
    const invalid = environment({ resolve: false });
    expect(await invalid.runtime.handle(event("evt-disabled"))).toMatchObject({
      kind: "refused",
      reason: "identity_unavailable",
    });
    expect(env.invocations).toHaveLength(0);
    expect(invalid.invocations).toHaveLength(0);
  });

  it("claims duplicate events atomically under concurrency", async () => {
    const env = environment();
    const results = await Promise.all(
      Array.from({ length: 32 }, () => env.runtime.handle(event("evt-duplicate"))),
    );
    expect(results.filter((result) => result.kind === "accepted")).toHaveLength(1);
    expect(results.filter((result) => result.kind === "duplicate")).toHaveLength(31);
    expect(env.invocations).toHaveLength(1);
    expect(env.audits).toHaveLength(1);
  });

  it("fails closed on authorization or audit failure and stores no failure payload", async () => {
    const denied = environment({
      decision: { allowed: false, reason: "permission_denied" },
    });
    expect(await denied.runtime.handle(event("evt-denied"))).toMatchObject({
      kind: "refused",
      reason: "permission_denied",
    });
    expect(denied.invocations).toHaveLength(0);
    expect(denied.store.event("slack", "evt-denied")?.status).toBe("permission_denied");

    const failedAudit = environment({ auditFails: true });
    expect(await failedAudit.runtime.handle(event("evt-audit"))).toMatchObject({
      kind: "failed",
      reason: "audit_failed",
    });
    expect(failedAudit.invocations).toHaveLength(0);
    expect(JSON.stringify(failedAudit.store.event("slack", "evt-audit"))).not.toContain("payload");
  });

  it("records a stable payload-free status when invocation fails", async () => {
    const env = environment({ invokeFails: true });
    expect(await env.runtime.handle(event("evt-failed"))).toMatchObject({
      kind: "failed",
      reason: "invoke_failed",
    });
    expect(env.store.event("slack", "evt-failed")).toMatchObject({ status: "invoke_failed" });
    expect(JSON.stringify(env.store.event("slack", "evt-failed"))).not.toContain("sensitive");
  });

  it("terminates identity and authorization dependency failures without their payload", async () => {
    for (const failure of ["link", "resolve", "authorize"] as const) {
      const env = environment({ securityFails: failure });
      expect(await env.runtime.handle(event(`evt-${failure}`))).toMatchObject({
        kind: "failed",
        reason: "security_failed",
      });
      expect(env.invocations).toHaveLength(0);
      expect(env.store.event("slack", `evt-${failure}`)?.status).toBe("security_failed");
      expect(JSON.stringify(env.store.event("slack", `evt-${failure}`))).not.toContain("secret");
    }
  });

  it("validates the service actor and provider event envelope", async () => {
    const env = environment();
    expect(
      () =>
        new CoreChannelRuntime({
          channelId: "slack",
          actor: { ...actor, canActFor: false },
          store: env.store,
          security: {
            resolveIdentity: async () => undefined,
            authorize: async () => ({ allowed: false, reason: "permission_denied" }),
            recordAudit: async () => {},
          },
          invoker: { invoke: async () => ({ runId: "never" }) },
        }),
    ).toThrow("act-for service account");
    await expect(
      env.runtime.handle(event("", { conversation: { id: "conversation", kind: "direct" } })),
    ).rejects.toThrow("channel event id");
    await expect(
      env.runtime.handle(
        event("evt-bad-kind", {
          conversation: { id: "conversation", kind: "broadcast" as never },
        }),
      ),
    ).rejects.toThrow("direct or shared");
  });
});
