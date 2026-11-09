// ---------------------------------------------------------------------------
// Retained subagents on real Postgres: the child's input and metadata, the
// inbox and outcome messages and the signal wake all go through jsonb, which
// reorders keys and rewrites numbers. The in-memory stores keep objects as
// they were written, so only this suite sees those differences.
// ---------------------------------------------------------------------------

import { expect, it } from "bun:test";
import { PostgresMemoryStore, PostgresWorkflowStorage, migrate } from "@promin/postgres";
import { postgresDescribe } from "@promin/postgres/testing";
import { harness, parent, waitFor } from "./retained-harness.ts";

postgresDescribe("retained subagents on Postgres (jsonb)", { migrate }, (pg) => {
  it("runs a child through spawn, steer, restart, list, read and delete", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: true });
    const memory = new PostgresMemoryStore({ db: pg.db });
    const h = await harness({ stores: { storage, memory }, replyTokens: 10 });
    await h.workflows.start();

    const child = await h.retained.spawnAgent({
      parent,
      agentId: "worker",
      task: "first",
      childId: "pg-child",
    });
    // Admission compares the stored input to the one sent: jsonb reorders keys.
    expect(child.status).toBe("admitted");
    const statuses = async () =>
      (await h.retained.readAgentMessages(parent, child.childId)).outcomes.map(
        (m) => (m as { metadata?: { status?: string; seq?: number } }).metadata,
      );
    await waitFor(async () => (await statuses()).length === 1);

    // A follow-up and a steer: the mode and message id ride in jsonb metadata.
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "second",
      mode: "follow-up",
      messageId: "m-2",
    });
    await waitFor(async () => (await statuses()).length === 2);
    await h.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "third",
      mode: "steer",
      messageId: "m-3",
    });
    await waitFor(async () => (await statuses()).length === 3);
    expect(await statuses()).toEqual([
      { seq: 0, status: "done" },
      { seq: 1, status: "done" },
      { seq: 2, status: "done" },
    ]);
    const read = await h.retained.readAgentMessages(parent, child.childId);
    expect(read.inbox.map((m) => m.content)).toEqual(["second", "third"]);
    expect(read.transcript.map((m) => m.content)).toContain("reply:third");

    // Listing filters by parent through jsonb metadata containment.
    const [listed] = await h.retained.listChildAgents(parent);
    expect(listed).toMatchObject({ childId: child.childId, agentId: "worker", depth: 1 });
    expect(
      await h.retained.listChildAgents({ ...parent, threadId: "elsewhere" }).catch(() => []),
    ).toEqual([]);

    // A restarted service sees the stored mailbox and does not re-run consumed messages.
    await h.workflows.stop();
    const restarted = h.make();
    await restarted.retained.reconcileMailboxes();
    await restarted.workflows.start();
    await restarted.retained.messageAgent({
      parent,
      childId: child.childId,
      task: "after restart",
      mode: "follow-up",
      messageId: "m-4",
    });
    await waitFor(async () => (await statuses()).length === 4);
    expect(
      (await restarted.retained.readAgentMessages(parent, child.childId)).transcript.filter(
        (m) => m.content === "reply:second",
      ),
    ).toHaveLength(1);

    await restarted.retained.deleteChildAgent(parent, child.childId);
    expect(await restarted.retained.listChildAgents(parent)).toEqual([]);
    await expect(
      restarted.retained.messageAgent({ parent, childId: child.childId, task: "gone" }),
    ).rejects.toMatchObject({ code: "not_found" });
    await restarted.workflows.stop();
  }, 60_000);
});
