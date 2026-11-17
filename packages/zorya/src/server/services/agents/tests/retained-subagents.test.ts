import { describe, expect, it } from "bun:test";
import {
  admitTurn,
  BudgetExceededError,
  InMemoryUsageLedger,
  type LLMProvider,
} from "@promin/agent";
import { InMemoryWorkflowStorage, SystemWallClock } from "@promin/workflow";
import { ZoryaServer } from "../../../server.ts";
import {
  principalRef,
  type Principal,
  type SecurityAuditEvent,
  type ZoryaSecurity,
} from "../../../security/index.ts";
import { ZoryaAgents } from "../zorya-agents.ts";
import { harness, overrideStorage, parent, waitFor } from "./retained-harness.ts";

function mutableUserSecurity() {
  const user: Principal = Object.freeze({
    kind: "user",
    id: "alice",
    accessRoles: Object.freeze({ acme: Object.freeze(["member"]) }),
    canActFor: false,
  });
  const state = {
    enabled: true,
    allowed: true,
    auditThrows: false,
    events: [] as SecurityAuditEvent[],
  };
  const security: ZoryaSecurity = {
    authenticate: async () => (state.enabled ? user : undefined),
    resolvePrincipal: async (ref) =>
      state.enabled && ref.kind === user.kind && ref.id === user.id ? user : undefined,
    resolveIdentity: async ({ actor, asserted }) =>
      state.enabled ? { actor, effective: asserted ?? principalRef(actor) } : undefined,
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async () =>
      state.enabled && state.allowed
        ? { allowed: true, reason: "access_role_allow" }
        : { allowed: false, reason: state.enabled ? "permission_denied" : "principal_disabled" },
    recordAudit: async (event) => {
      state.events.push(event);
      if (state.auditThrows) throw new Error("audit unavailable");
    },
  };
  return { user, state, security };
}

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

describe("retained subagents", () => {
  it("admits a child, preserves its inbox across a service restart, and isolates ownership", async () => {
    const h = await harness();
    await h.workflows.start();
    const securityContext = { actor: { kind: "user" as const, id: "alice" } };
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "child-1",
      securityContext,
    });
    expect(child.status).toBe("admitted");
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    expect(await h.retained.listChildAgents(parent)).toHaveLength(1);
    await h.workflows.stop();
    const storedBeforeRestart = await h.storage.loadWorkflow(child.workflowId);
    expect(storedBeforeRestart).toBeDefined();
    expect(storedBeforeRestart!.securityContext).toEqual(securityContext);
    expect((storedBeforeRestart!.input as { securityContext?: unknown }).securityContext).toEqual(
      securityContext,
    );
    const restarted = h.make();
    await restarted.retained.reconcileMailboxes();
    await restarted.workflows.start();
    await restarted.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "follow up",
      mode: "follow-up",
      messageId: "m1",
    });
    await waitFor(
      async () =>
        (await restarted.retained.readAgentMessages(parent, child.childId)).transcript.length >= 4,
    );
    const messages = await restarted.retained.readAgentMessages(parent, child.childId);
    expect(messages.transcript.map((m) => m.content)).toContain("follow up");
    expect(
      (
        await restarted.retained.messageAgent({
          parent,
          childId: child.childId,
          task: "follow up",
          mode: "follow-up",
          messageId: "m1",
        })
      ).seq,
    ).toBe(1);
    await expect(
      restarted.retained.messageAgent({
        parent,
        childId: child.childId,
        task: "different",
        mode: "follow-up",
        messageId: "m1",
      }),
    ).rejects.toThrow("id conflict");
    await expect(
      restarted.retained.readAgentMessages({ ...parent, resourceId: "bob" }, child.childId),
    ).rejects.toThrow();
    await restarted.retained.stopAgent(parent, child.childId);
    await restarted.workflows.stop();
  });

  it("blocks a retained mailbox turn after starter revocation before agent code runs", async () => {
    const h = await harness();
    const user: Principal = {
      kind: "user",
      id: "alice",
      accessRoles: { acme: ["member"] },
      canActFor: false,
    };
    let enabled = true;
    const audits: SecurityAuditEvent[] = [];
    const security: ZoryaSecurity = {
      authenticate: async () => (enabled ? user : undefined),
      resolvePrincipal: async (ref) =>
        enabled && ref.kind === user.kind && ref.id === user.id ? user : undefined,
      resolveIdentity: async ({ actor, asserted }) =>
        enabled ? { actor, effective: asserted ?? principalRef(actor) } : undefined,
      identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
      authorize: async () =>
        enabled
          ? { allowed: true, reason: "access_role_allow" }
          : { allowed: false, reason: "principal_disabled" },
      recordAudit: async (event) => void audits.push(event),
    };
    h.workflows.configureExecutionSecurity({
      security,
      clock: SystemWallClock,
      logger: { error: () => {} },
    });
    h.retained.configureExecutionSecurity({ security });
    await expect(
      h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "missing identity",
        childId: "missing-retained-identity",
      }),
    ).rejects.toMatchObject({ code: "forbidden" });
    const missingIdentityAudit = audits.at(-1) as unknown as Record<string, unknown>;
    expect(missingIdentityAudit.event).toBe("retained-agent.authorization");
    expect("context" in missingIdentityAudit).toBe(false);
    expect(missingIdentityAudit.decision).toEqual({
      allowed: false,
      reason: "identity_unknown",
    });
    expect(missingIdentityAudit.metadata).toMatchObject({
      operation: "spawn",
      denialReason: "missing_identity",
      childId: "missing-retained-identity",
      parentAgentId: "parent",
      parentThreadId: "conversation",
      actorId: null,
      effectiveId: null,
    });
    expect(Object.isFrozen(missingIdentityAudit)).toBe(true);
    expect(Object.isFrozen(missingIdentityAudit.metadata)).toBe(true);

    const legacy = await harness();
    const legacyChild = await legacy.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "legacy contextless",
      childId: "legacy-contextless-module-turn",
    });
    legacy.retained.configureExecutionSecurity({ security });
    await expect(
      legacy.retained.messageAgent({
        parent,
        childId: legacyChild.childId,
        task: "must stay blocked",
        messageId: "legacy-contextless-message",
      }),
    ).rejects.toMatchObject({ code: "forbidden" });
    const missingTurnAudit = audits.findLast(
      (event) =>
        event.metadata?.childId === "legacy-contextless-module-turn" &&
        event.metadata.operation === "message",
    ) as unknown as Record<string, unknown>;
    expect("context" in missingTurnAudit).toBe(false);
    expect(missingTurnAudit.metadata).toMatchObject({
      operation: "message",
      denialReason: "missing_identity",
      childId: "legacy-contextless-module-turn",
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "revoked-starter",
      securityContext: { actor: principalRef(user) },
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    const before = (await h.retained.readAgentMessages(parent, child.childId)).transcript.length;
    await h.workflows.stop();
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "must not run",
      messageId: "revoked-message",
    });
    enabled = false;
    await h.workflows.start();
    await waitFor(
      async () => (await h.storage.loadWorkflow(child.workflowId))?.status === "failed",
    );
    expect((await h.retained.readAgentMessages(parent, child.childId)).transcript).toHaveLength(
      before,
    );
    await h.workflows.stop();
  });

  it("reauthorizes after registry, resolver and thread awaits before the next side effect", async () => {
    for (const phase of ["registry", "resolver", "thread"] as const) {
      const auth = mutableUserSecurity();
      const h = await harness();
      h.workflows.configureExecutionSecurity({
        security: auth.security,
        clock: SystemWallClock,
        logger: { error: () => {} },
      });
      h.retained.configureExecutionSecurity({
        security: auth.security,
        logger: { error: () => {} },
      });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "initial",
        childId: `race-${phase}`,
        securityContext: { actor: principalRef(auth.user) },
      });
      await waitFor(
        async () =>
          (await h.retained.readAgentMessages(parent, child.childId)).outcomes.length >= 1,
      );
      await h.workflows.stop();
      await h.retained.messageAgent({
        parent,
        childId: child.childId,
        task: `race ${phase}`,
        messageId: `race-${phase}`,
      });

      const entered = deferred();
      const release = deferred();
      let workerResolveCalls = 0;
      let withScopeCalls = 0;
      let sendCalls = 0;
      const originalResolve = h.agents.resolve.bind(h.agents);
      Object.defineProperty(h.agents, "resolve", {
        configurable: true,
        value: async (
          recipe: Parameters<typeof originalResolve>[0],
          scope: Parameters<typeof originalResolve>[1],
        ) => {
          if (recipe.id !== "worker") return originalResolve(recipe, scope);
          workerResolveCalls++;
          const resolved = await originalResolve(recipe, scope);
          if (phase === "resolver") {
            entered.release();
            await release.promise;
          }
          return new Proxy(resolved, {
            get(target, property, receiver) {
              if (property !== "withScope") return Reflect.get(target, property, receiver);
              return (agentScope: Parameters<typeof target.withScope>[0]) => {
                withScopeCalls++;
                const scoped = target.withScope(agentScope);
                if (phase !== "thread") return scoped;
                return new Proxy(scoped, {
                  get(scopedTarget, scopedProperty, scopedReceiver) {
                    if (scopedProperty !== "thread") {
                      return Reflect.get(scopedTarget, scopedProperty, scopedReceiver);
                    }
                    return async (...args: Parameters<typeof scopedTarget.thread>) => {
                      const thread = await scopedTarget.thread(...args);
                      entered.release();
                      await release.promise;
                      return new Proxy(thread, {
                        get(threadTarget, threadProperty, threadReceiver) {
                          if (threadProperty !== "send") {
                            return Reflect.get(threadTarget, threadProperty, threadReceiver);
                          }
                          return (...sendArgs: Parameters<typeof threadTarget.send>) => {
                            sendCalls++;
                            return threadTarget.send(...sendArgs);
                          };
                        },
                      });
                    };
                  },
                });
              };
            },
          });
        },
      });
      if (phase === "registry") {
        const originalGet = h.agents.registry.get.bind(h.agents.registry);
        let blocked = false;
        Object.defineProperty(h.agents.registry, "get", {
          configurable: true,
          value: async (query: Parameters<typeof originalGet>[0]) => {
            if (!blocked && query.id === "worker" && query.version !== undefined) {
              blocked = true;
              entered.release();
              await release.promise;
            }
            return originalGet(query);
          },
        });
      }

      await h.workflows.start();
      await entered.promise;
      auth.state.enabled = false;
      release.release();
      await waitFor(
        async () =>
          (await h.retained.readAgentMessages(parent, child.childId)).outcomes.length >= 2,
      );
      if (phase === "registry") expect(workerResolveCalls).toBe(0);
      if (phase === "resolver") expect(withScopeCalls).toBe(0);
      if (phase === "thread") expect(sendCalls).toBe(0);
      const denial = auth.state.events.findLast(
        (event) =>
          event.event === "retained-agent.authorization" && event.decision?.allowed === false,
      );
      expect(denial?.metadata?.operation).toBe(
        phase === "registry" ? "turn.resolve" : phase === "resolver" ? "turn.thread" : "turn.send",
      );
      expect(Object.isFrozen(denial)).toBe(true);
      expect(Object.isFrozen(denial?.context)).toBe(true);
      expect(Object.isFrozen(denial?.metadata)).toBe(true);
      await h.workflows.stop();
    }
  });

  it("audits retained operations immutably and never bypasses an unavailable audit sink", async () => {
    const auth = mutableUserSecurity();
    const h = await harness();
    h.retained.configureExecutionSecurity({
      security: auth.security,
      logger: { error: () => {} },
    });
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "audited-operations",
      securityContext: { actor: principalRef(auth.user) },
    });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "queued",
      messageId: "audited-message",
    });
    await h.retained.stopAgent(parent, child.childId);
    const operations = auth.state.events
      .filter((event) => event.event === "retained-agent.authorization")
      .map((event) => event.metadata?.operation)
      .filter((operation) => ["spawn", "message", "stop"].includes(String(operation)));
    expect(operations).toEqual(["spawn", "message", "stop"]);
    for (const event of auth.state.events) {
      expect(Object.isFrozen(event)).toBe(true);
      expect(Object.isFrozen(event.context)).toBe(true);
      expect(Object.isFrozen(event.decision)).toBe(true);
      expect(event.metadata?.childId).toBe(child.childId);
      expect(event.metadata?.actorId).toBe("alice");
      expect(event.metadata?.effectiveId).toBe("alice");
    }

    const auditDown = mutableUserSecurity();
    auditDown.state.auditThrows = true;
    const failClosed = await harness();
    failClosed.retained.configureExecutionSecurity({
      security: auditDown.security,
      logger: { error: () => {} },
    });
    await expect(
      failClosed.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "must not admit",
        childId: "audit-down-allow",
        securityContext: { actor: principalRef(auditDown.user) },
      }),
    ).rejects.toMatchObject({ code: "forbidden" });
    expect(await failClosed.retained.listChildAgents(parent)).toEqual([]);

    auditDown.state.allowed = false;
    await expect(
      failClosed.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "still denied",
        childId: "audit-down-deny",
        securityContext: { actor: principalRef(auditDown.user) },
      }),
    ).rejects.toMatchObject({ code: "forbidden" });
    expect(auditDown.state.events.at(-1)?.decision?.allowed).toBe(false);
    await expect(
      failClosed.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "missing and unauditable",
        childId: "audit-down-missing",
      }),
    ).rejects.toMatchObject({ code: "forbidden" });
    const missingAudit = auditDown.state.events.at(-1) as unknown as Record<string, unknown>;
    expect("context" in missingAudit).toBe(false);
    expect(missingAudit.metadata).toMatchObject({
      denialReason: "missing_identity",
      childId: "audit-down-missing",
    });
  });

  it("enforces capability and depth policy", async () => {
    const h = await harness();
    await expect(
      h.retained.spawnAgent({ parent, agentId: "parent", task: "denied" }),
    ).rejects.toThrow("capability");
  });

  it("steer supersedes queued work at the next turn boundary", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "steered",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.workflows.stop();
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "old plan",
      mode: "queued",
      messageId: "q1",
    });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "new plan",
      mode: "steer",
      messageId: "s2",
    });
    await h.workflows.start();
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 4,
    );
    const transcript = (await h.retained.readAgentMessages(parent, child.childId)).transcript.map(
      (m) => m.content,
    );
    expect(transcript).toContain("new plan");
    expect(transcript).not.toContain("old plan");
    await h.workflows.stop();
  });

  it("exposes scoped HTTP admission and mailbox routes", async () => {
    const h = await harness();
    const audit: Array<Record<string, unknown>> = [];
    const server = new ZoryaServer({
      insecure: true,
      workflows: h.workflows,
      agents: h.agents,
      retainedSubagents: h.retained,
      logger: {
        log: (message) => {
          if (message.startsWith("{")) audit.push(JSON.parse(message) as Record<string, unknown>);
        },
        warn: () => {},
        error: () => {},
      },
    });
    const send = (method: string, path: string, body?: unknown) =>
      server.handle(
        new Request(`http://test${path}`, {
          method,
          headers: { "content-type": "application/json" },
          ...(body !== undefined && { body: JSON.stringify(body) }),
        }),
      );
    const created = await send("POST", "/api/agents/parent/threads/conversation/children", {
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "worker",
      task: "work",
    });
    expect(created.status).toBe(202);
    const { childId } = (await created.json()) as { childId: string };
    const createdState = await h.storage.loadWorkflow(`retained-agent:${childId}`);
    expect(createdState).toBeDefined();
    expect(createdState!.securityContext).toEqual({
      actor: { kind: "operator", id: "operator" },
      effective: { kind: "user", id: "alice" },
    });
    expect((createdState!.input as { securityContext?: unknown }).securityContext).toEqual(
      createdState!.securityContext,
    );
    expect(
      (
        await send("POST", `/api/agents/parent/threads/conversation/children/${childId}/messages`, {
          namespaceId: "acme",
          resourceId: "alice",
          task: "follow up",
        })
      ).status,
    ).toBe(202);
    expect(
      (
        await send("POST", `/api/agents/parent/threads/conversation/children/${childId}/stop`, {
          namespaceId: "acme",
          resourceId: "alice",
        })
      ).status,
    ).toBe(200);
    expect(
      (
        await send(
          "DELETE",
          `/api/agents/parent/threads/conversation/children/${childId}?namespaceId=acme&resourceId=alice`,
        )
      ).status,
    ).toBe(200);
    const mutationAudits = audit.filter((event) =>
      ["/messages", "/stop", `/${childId}`].some((suffix) =>
        String((event.metadata as { path?: string } | undefined)?.path).endsWith(suffix),
      ),
    );
    expect(mutationAudits).toEqual([]);
    expect(
      (
        await send(
          "GET",
          "/api/agents/parent/threads/conversation/children?namespaceId=acme&resourceId=alice",
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await send(
          "GET",
          `/api/agents/parent/threads/conversation/children/${childId}/messages?namespaceId=acme&resourceId=bob`,
        )
      ).status,
    ).toBe(404);
  });

  it("runs concurrent children and cleans up one without changing the other", async () => {
    const h = await harness();
    await h.workflows.start();
    const [a, b] = await Promise.all(
      ["a", "b"].map((childId) =>
        h.retained.spawnAgent({ parent, agentId: "worker", task: childId, childId }),
      ),
    );
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, a.childId)).transcript.length >= 2 &&
        (await h.retained.readAgentMessages(parent, b.childId)).transcript.length >= 2,
    );
    await h.retained.deleteChildAgent(parent, a.childId);
    await expect(h.retained.readAgentMessages(parent, a.childId)).rejects.toThrow("not found");
    expect((await h.retained.readAgentMessages(parent, b.childId)).transcript).toHaveLength(2);
    await h.workflows.stop();
  });

  it("survives storage returning reordered jsonb keys on spawn", async () => {
    const reorder = (value: unknown): unknown =>
      value && typeof value === "object" && !Array.isArray(value)
        ? Object.fromEntries(
            Object.entries(value as Record<string, unknown>)
              .reverse()
              .map(([k, v]) => [k, reorder(v)]),
          )
        : value;
    const h = await harness({
      wrapStorage: (storage) =>
        overrideStorage(storage, "loadWorkflow", (original) => async (id: string) => {
          const state = await original(id);
          return state && state.input ? { ...state, input: reorder(state.input) } : state;
        }) as InMemoryWorkflowStorage,
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "reordered",
    });
    expect(child.childId).toBe("reordered");
    await h.workflows.stop();
  });

  it("snapshots hostile stored input and security getters exactly once", async () => {
    let hostile = false;
    let repeated = false;
    const reads = new Map<string, number>();
    const count = (key: string) => reads.set(key, (reads.get(key) ?? 0) + 1);
    const h = await harness({
      wrapStorage: (storage) =>
        overrideStorage(storage, "loadWorkflow", (original) => async (id: string) => {
          const state = await original(id);
          if (!hostile || !state || id !== "retained-agent:getter-safe") return state;
          const stable = state.input as Record<string, unknown>;
          const localReads = new Map<string, number>();
          const input = Object.create(null) as Record<string, unknown>;
          for (const key of [
            "childId",
            "agentId",
            "agentVersion",
            "parent",
            "depth",
            "task",
            "securityContext",
          ]) {
            Object.defineProperty(input, key, {
              enumerable: true,
              get: () => {
                count(`input.${key}`);
                const localCount = (localReads.get(key) ?? 0) + 1;
                localReads.set(key, localCount);
                if (localCount > 1) repeated = true;
                if (key === "agentId" && localCount > 1) return "attacker";
                return stable[key];
              },
            });
          }
          return Object.defineProperties(
            { ...state, input },
            {
              securityContext: {
                enumerable: true,
                get: () => {
                  count("state.securityContext");
                  const localCount = (localReads.get("state.securityContext") ?? 0) + 1;
                  localReads.set("state.securityContext", localCount);
                  if (localCount > 1) repeated = true;
                  return state.securityContext;
                },
              },
            },
          );
        }) as InMemoryWorkflowStorage,
    });
    const securityContext = { actor: { kind: "user" as const, id: "alice" } };
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "getter-safe",
      securityContext,
    });
    hostile = true;
    expect((await h.retained.readAgentMessages(parent, child.childId)).transcript).toEqual([]);
    expect(repeated).toBe(false);
    expect(reads.get("input.agentId")).toBeGreaterThanOrEqual(1);
    expect(reads.get("input.securityContext")).toBeGreaterThanOrEqual(1);
    expect(reads.get("state.securityContext")).toBeGreaterThanOrEqual(1);
  });

  it("processes a message whose wake signal was lost and the ones after it", async () => {
    let failNext = false;
    const h = await harness({
      retained: { pollIntervalMs: 50 },
      wrapStorage: (storage) =>
        overrideStorage(
          storage,
          "deliverSignal",
          (original) =>
            (async (p: never) => {
              if (failNext) {
                failNext = false;
                throw new Error("signal write failed");
              }
              return original(p);
            }) as never,
        ),
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "lost",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    failNext = true;
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "first",
      messageId: "m1",
    });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "second",
      messageId: "m2",
    });
    await waitFor(async () => {
      const t = (await h.retained.readAgentMessages(parent, child.childId)).transcript;
      return t.some((m) => m.content === "second") && t.some((m) => m.content === "first");
    });
    await h.workflows.stop();
  });

  it("completes an idle child after the idle TTL", async () => {
    const h = await harness({ retained: { pollIntervalMs: 30, idleTtlMs: 90 } });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "idle",
    });
    await waitFor(
      async () => (await h.retained.listChildAgents(parent))[0]?.status === "completed",
    );
    expect(child.childId).toBe("idle");
    await expect(
      h.retained.messageAgent({ parent, childId: "idle", task: "late" }),
    ).rejects.toThrow("stopped");
    await h.workflows.stop();
  });

  it("keeps the child alive after a failed turn and records the failure", async () => {
    const h = await harness({
      retained: { pollIntervalMs: 50 },
      chat: (content) => {
        if (content === "boom") throw new Error("model exploded");
        return `reply:${content}`;
      },
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "sturdy",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.retained.messageAgent({ parent, childId: child.childId, task: "boom", messageId: "b" });
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).outcomes.some((o) =>
        String(o.content).startsWith("failed"),
      ),
    );
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "after",
      messageId: "a",
    });
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).transcript.some(
        (m) => m.content === "after",
      ),
    );
    expect((await h.retained.listChildAgents(parent))[0]?.status).not.toBe("failed");
    await h.workflows.stop();
  });

  it("passes the caller scope to canDelegate and authorizes the child agent on the route", async () => {
    const seen: unknown[] = [];
    const h = await harness({
      retained: {
        canDelegate: (_from, _to, options) => {
          seen.push(options.scope);
          return true;
        },
      },
    });
    await h.retained.spawnAgent({ parent, agentId: "worker", task: "x" });
    expect(seen).toEqual([{ namespaceId: "acme", resourceId: "alice" }]);
    const denied = new ZoryaServer({
      insecure: true,
      workflows: h.workflows,
      agents: new ZoryaAgents({
        registry: h.agents.registry,
        memory: h.memory,
        resolve: h.agents.resolve,
        authorizeScope: ({ scope }) => !("agentId" in scope && scope.agentId === "worker"),
      }),
      retainedSubagents: h.retained,
    });
    const response = await denied.handle(
      new Request("http://test/api/agents/parent/threads/conversation/children", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          namespaceId: "acme",
          resourceId: "alice",
          agentId: "worker",
          task: "w",
        }),
      }),
    );
    expect(response.status).toBe(403);
  });

  it("enforces the child limit under concurrent spawns without a turn gate", async () => {
    const h = await harness({ retained: { maxChildren: 2 }, turnGate: false });
    const results = await Promise.allSettled(
      ["a", "b", "c", "d"].map((childId) =>
        h.retained.spawnAgent({ parent, agentId: "worker", task: childId, childId }),
      ),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(2);
  });

  it("does not list a deleted child and records superseded messages", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "gone",
    });
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    await h.workflows.stop();
    await h.retained.messageAgent({ parent, childId: child.childId, task: "old", messageId: "q" });
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "new",
      mode: "steer",
      messageId: "s",
    });
    await h.workflows.start();
    await waitFor(async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).outcomes.some((o) =>
        String(o.content).startsWith("superseded"),
      ),
    );
    await h.retained.deleteChildAgent(parent, child.childId);
    expect(await h.retained.listChildAgents(parent)).toHaveLength(0);
    await h.workflows.stop();
  });

  it("reconcile does not re-signal consumed messages", async () => {
    let delivered = 0;
    const h = await harness({
      wrapStorage: (storage) =>
        overrideStorage(
          storage,
          "deliverSignal",
          (original) =>
            (async (p: never) => {
              delivered++;
              return original(p);
            }) as never,
        ),
    });
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "rec",
    });
    await h.retained.messageAgent({ parent, childId: child.childId, task: "one", messageId: "o" });
    await waitFor(
      async () => (await h.retained.readAgentMessages(parent, child.childId)).outcomes.length >= 2,
    );
    const before = delivered;
    await h.retained.reconcileMailboxes();
    expect(delivered).toBe(before);
    await h.workflows.stop();
  });

  describe("steering a running turn", () => {
    /** A model that holds the "long" task until released or aborted. */
    function longTaskModel() {
      let release: () => void = () => {};
      const held = new Promise<void>((resolve) => (release = resolve));
      const seen = { started: 0, aborted: 0 };
      const chatAsync = async (content: string, signal: AbortSignal | undefined) => {
        if (!content.includes("long task")) return `reply:${content}`;
        seen.started++;
        await Promise.race([
          held,
          new Promise<void>((resolve) => {
            if (signal?.aborted) resolve();
            signal?.addEventListener("abort", () => resolve(), { once: true });
          }),
        ]);
        if (signal?.aborted) {
          seen.aborted++;
          throw new DOMException("aborted", "AbortError");
        }
        return "long answer";
      };
      return { chatAsync, release, seen };
    }

    it("interrupts the running turn, records it as interrupted and runs the steer next", async () => {
      const model = longTaskModel();
      const h = await harness({
        chatAsync: model.chatAsync,
        retained: { steerPollMs: 10, maxIterations: 50 },
      });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "long task",
        childId: "interrupted",
      });
      await waitFor(async () => model.seen.started === 1);
      await h.retained.messageAgent({
        parent,
        childId: child.childId,
        task: "new direction",
        mode: "steer",
        messageId: "s1",
      });
      await waitFor(async () => {
        const { transcript } = await h.retained.readAgentMessages(parent, child.childId);
        return transcript.some((m) => m.content === "reply:new direction");
      });
      const { outcomes } = await h.retained.readAgentMessages(parent, child.childId);
      expect(
        outcomes.map((m) => (m as { metadata?: { status?: string } }).metadata?.status),
      ).toEqual(["interrupted", "done"]);
      expect(model.seen.aborted).toBe(1);
      await h.workflows.stop();
    });

    it("lets a queued or follow-up message wait for the running turn (negative control)", async () => {
      const model = longTaskModel();
      const h = await harness({
        chatAsync: model.chatAsync,
        retained: { steerPollMs: 10, maxIterations: 50 },
      });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "long task",
        childId: "not-interrupted",
      });
      await waitFor(async () => model.seen.started === 1);
      await h.retained.messageAgent({
        parent,
        childId: child.childId,
        task: "later",
        mode: "follow-up",
        messageId: "f1",
      });
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(model.seen.aborted).toBe(0);
      model.release();
      await waitFor(async () => {
        const { outcomes } = await h.retained.readAgentMessages(parent, child.childId);
        return outcomes.length === 2;
      });
      const { outcomes } = await h.retained.readAgentMessages(parent, child.childId);
      expect(
        outcomes.map((m) => (m as { metadata?: { status?: string } }).metadata?.status),
      ).toEqual(["done", "done"]);
      await h.workflows.stop();
    });
  });

  describe("budget accounting", () => {
    const statuses = async (h: Awaited<ReturnType<typeof harness>>, childId: string) =>
      (await h.retained.readAgentMessages(parent, childId)).outcomes.map(
        (m) => (m as { metadata?: { status?: string } }).metadata?.status,
      );

    it("charges a child's turns to the parent thread budget and refuses once it is used up", async () => {
      const ledger = new InMemoryUsageLedger();
      const budgets = { ledger, thread: { maxTotalTokens: 250 }, minCallTokens: 1 };
      const h = await harness({ replyTokens: 100, retained: { budgets } });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "one",
        childId: "metered",
      });
      for (const [i, task] of ["two", "three", "four"].entries()) {
        await waitFor(async () => (await statuses(h, child.childId)).length === i + 1);
        await h.retained.messageAgent({ parent, childId: child.childId, task, messageId: task });
      }
      await waitFor(async () => (await statuses(h, child.childId)).length === 4);
      // Turns two and three fit under the cap (admitted while 100 and 200 were spent).
      expect(await statuses(h, child.childId)).toEqual(["done", "done", "done", "failed"]);
      const spent = await ledger.total({
        namespaceId: parent.namespaceId,
        threadId: parent.threadId,
      });
      expect(spent.inputTokens).toBe(300);
      // The parent's own next turn sees the same standing.
      await expect(
        admitTurn({
          budgets,
          namespaceId: parent.namespaceId,
          threadId: parent.threadId,
        }),
      ).rejects.toBeInstanceOf(BudgetExceededError);
      await h.workflows.stop();
    });

    it("fails the child once its own run budget is crossed, via ctx.reportUsage", async () => {
      const h = await harness({ replyTokens: 100, retained: { childBudget: { maxTokens: 150 } } });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "one",
        childId: "capped",
      });
      await waitFor(async () => (await statuses(h, child.childId)).length === 1);
      await h.retained.messageAgent({ parent, childId: child.childId, task: "two" });
      await waitFor(async () => (await h.retained.listChildAgents(parent))[0]?.status === "failed");
      const [handle] = await h.retained.listChildAgents(parent);
      expect(handle?.usage?.totalTokens).toBe(200);
      await expect(
        h.retained.messageAgent({ parent, childId: child.childId, task: "three" }),
      ).rejects.toMatchObject({ code: "conflict" });
      await h.workflows.stop();
    });

    it("reports usage and keeps running when no budget is configured (negative control)", async () => {
      const h = await harness({ replyTokens: 100 });
      await h.workflows.start();
      const child = await h.retained.spawnAgent({
        parent,
        agentId: "worker",
        task: "one",
        childId: "free",
      });
      await waitFor(async () => (await statuses(h, child.childId)).length === 1);
      await h.retained.messageAgent({ parent, childId: child.childId, task: "two" });
      await waitFor(async () => (await statuses(h, child.childId)).length === 2);
      const [handle] = await h.retained.listChildAgents(parent);
      expect(handle?.status).not.toBe("failed");
      expect(await statuses(h, child.childId)).toEqual(["done", "done"]);
      await h.workflows.stop();
    });

    describe("a turn that spends and then fails", () => {
      /** Each turn: one priced call that asks for a tool, then a provider error. */
      function failingModel() {
        const seen = { calls: 0 };
        const rawChat: LLMProvider["chat"] = async (request) => {
          seen.calls++;
          if (request.messages.at(-1)?.role === "tool") throw new Error("provider down");
          return {
            content: null,
            finishReason: "tool_calls" as const,
            toolCalls: [{ id: `c${seen.calls}`, name: "missing", input: {} }],
            usage: { inputTokens: 100, outputTokens: 0 },
          };
        };
        return { rawChat, seen };
      }

      it("counts failed turns toward the parent budget and refuses further turns", async () => {
        const ledger = new InMemoryUsageLedger();
        const model = failingModel();
        const h = await harness({
          rawChat: model.rawChat,
          retained: {
            budgets: { ledger, thread: { maxTotalTokens: 250 }, minCallTokens: 1 },
          },
        });
        await h.workflows.start();
        const child = await h.retained.spawnAgent({
          parent,
          agentId: "worker",
          task: "one",
          childId: "failing",
        });
        for (const [i, task] of ["two", "three", "four"].entries()) {
          await waitFor(async () => (await statuses(h, child.childId)).length === i + 1);
          await h.retained.messageAgent({ parent, childId: child.childId, task, messageId: task });
        }
        await waitFor(async () => (await statuses(h, child.childId)).length === 4);
        expect(await statuses(h, child.childId)).toEqual(["failed", "failed", "failed", "failed"]);
        // Three turns reached the model (two calls each); the fourth was refused up front.
        expect(model.seen.calls).toBe(6);
        const spent = await ledger.total({
          namespaceId: parent.namespaceId,
          threadId: parent.threadId,
        });
        expect(spent.inputTokens).toBe(300);
        await h.workflows.stop();
      });

      it("counts failed turns toward the child's run budget", async () => {
        const model = failingModel();
        const h = await harness({
          rawChat: model.rawChat,
          retained: { childBudget: { maxTokens: 150 } },
        });
        await h.workflows.start();
        const child = await h.retained.spawnAgent({
          parent,
          agentId: "worker",
          task: "one",
          childId: "failing-capped",
        });
        await waitFor(async () => (await statuses(h, child.childId)).length === 1);
        await h.retained.messageAgent({ parent, childId: child.childId, task: "two" });
        await waitFor(
          async () => (await h.retained.listChildAgents(parent))[0]?.status === "failed",
        );
        await expect(
          h.retained.messageAgent({ parent, childId: child.childId, task: "three" }),
        ).rejects.toMatchObject({ code: "conflict" });
        await h.workflows.stop();
      });
    });
  });
});
