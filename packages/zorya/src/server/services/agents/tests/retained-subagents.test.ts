import { describe, expect, it } from "bun:test";
import { admitTurn, BudgetExceededError, InMemoryUsageLedger } from "@promin/agent";
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { ZoryaServer } from "../../../server.ts";
import { ZoryaAgents } from "../zorya-agents.ts";
import { harness, overrideStorage, parent, waitFor } from "./retained-harness.ts";

describe("retained subagents", () => {
  it("admits a child, preserves its inbox across a service restart, and isolates ownership", async () => {
    const h = await harness();
    await h.workflows.start();
    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "initial",
      childId: "child-1",
    });
    expect(child.status).toBe("admitted");
    await waitFor(
      async () =>
        (await h.retained.readAgentMessages(parent, child.childId)).transcript.length >= 2,
    );
    expect(await h.retained.listChildAgents(parent)).toHaveLength(1);
    await h.workflows.stop();
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
    const server = new ZoryaServer({
      insecure: true,
      workflows: h.workflows,
      agents: h.agents,
      retainedSubagents: h.retained,
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
  });
});
