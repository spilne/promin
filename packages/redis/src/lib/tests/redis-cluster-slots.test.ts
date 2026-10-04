import { describe, expect, it } from "bun:test";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import * as scripts from "../redis-workflow-scripts.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Redis Cluster readiness of RedisWorkflowStorage: every script call and
// every multi-key command must stay within one hash slot. The test records
// each call a full workload makes and computes the CRC16 slot of every key
// it passes — plus the key bases scripts derive further keys from in Lua.
// ---------------------------------------------------------------------------

/** CRC16-XMODEM, the checksum Redis Cluster hashes keys with. */
function crc16(input: string): number {
  let crc = 0;
  for (const byte of new TextEncoder().encode(input)) {
    crc ^= byte << 8;
    for (let i = 0; i < 8; i++) {
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
  }
  return crc;
}

/** The cluster slot of a key, honoring `{hash tags}`. */
function keySlot(key: string): number {
  const open = key.indexOf("{");
  if (open !== -1) {
    const close = key.indexOf("}", open + 1);
    if (close > open + 1) return crc16(key.slice(open + 1, close)) % 16_384;
  }
  return crc16(key) % 16_384;
}

interface RecordedCall {
  readonly command: string;
  readonly script?: string;
  readonly keys: readonly string[];
  readonly derivedBases: readonly string[];
}

/** Wrap a client, recording the keys of every eval and multi-key command. */
function recording(params: { client: RedisStoreClient; prefix: string }): {
  client: RedisStoreClient;
  calls: RecordedCall[];
} {
  const calls: RecordedCall[] = [];
  const isBase = (arg: unknown): arg is string =>
    typeof arg === "string" && arg.startsWith(`${params.prefix}:{`);
  const client = new Proxy(params.client, {
    get(target, prop) {
      const value = (target as unknown as Record<string | symbol, unknown>)[prop];
      if (typeof value !== "function") return value;
      const fn = value.bind(target) as (...args: unknown[]) => unknown;
      if (prop === "eval") {
        return (script: string, numKeys: number, ...rest: unknown[]) => {
          calls.push({
            command: "eval",
            script,
            keys: rest.slice(0, numKeys) as string[],
            derivedBases: rest.slice(numKeys).filter(isBase),
          });
          return fn(script, numKeys, ...rest);
        };
      }
      if (prop === "sinter" || prop === "del") {
        return (...keys: unknown[]) => {
          calls.push({ command: prop, keys: keys as string[], derivedBases: [] });
          return fn(...keys);
        };
      }
      return fn;
    },
  });
  return { client, calls };
}

describe("keySlot", () => {
  it("matches Redis Cluster's CRC16 slots", () => {
    expect(crc16("123456789")).toBe(0x31c3);
    expect(keySlot("foo")).toBe(12_182);
    expect(keySlot("{user1000}.following")).toBe(keySlot("{user1000}.followers"));
    expect(keySlot("foo{}{bar}")).toBe(crc16("foo{}{bar}") % 16_384);
  });
});

redisDescribe("RedisWorkflowStorage on Redis Cluster", (redis) => {
  it("keeps every script call and multi-key command within one slot", async () => {
    const prefix = uniquePrefix("slots");
    const { client, calls } = recording({ client: redis.client(), prefix });
    const s = new RedisWorkflowStorage({
      redis: client,
      prefix,
      retention: { completedTtlMs: 60_000 },
    });

    // Parent with a fenced child, steps, tasks, a suspension, signals.
    await s.createWorkflow({ workflowId: "p", workflowName: "parent", input: 1, namespace: "ns" });
    const { token } = await s.tryLock({ workflowId: "p", lockDurationMs: 30_000 });
    const guard = { fenceToken: token! };
    await s.createWorkflow({
      workflowId: "c",
      workflowName: "child",
      input: 2,
      parentWorkflowId: "p",
      workflowType: "t",
      guard,
    });
    await s.heartbeat({ workflowId: "p", lockDurationMs: 30_000, guard });
    await s.saveStepResult({
      workflowId: "p",
      stepName: "a",
      result: 1,
      durationMs: 1,
      startedAt: new Date(),
      guard,
    });
    await s.saveTaskResult({ workflowId: "p", stepName: "m", taskIndex: 0, result: 1, guard });
    await s.saveTaskFailure({ workflowId: "p", stepName: "m", taskIndex: 1, error: "x", guard });
    await s.saveStepFailure({
      workflowId: "p",
      stepName: "b",
      error: "x",
      durationMs: 1,
      startedAt: new Date(),
      guard,
    });
    await s.saveStepAttempt({
      record: {
        workflowId: "p",
        stepName: "b",
        attempt: 1,
        status: "failed",
        error: "x",
        startedAt: new Date(),
        completedAt: new Date(),
        durationMs: 1,
      } as never,
      guard,
    });
    await s.setWorkflowMetadata({ workflowId: "p", patch: { k: 1 }, guard });
    await s.appendStreamChunk({
      workflowId: "p",
      streamId: "out",
      payload: 1,
      appendedBy: "workflow",
      guard,
    });
    await s.readStreamChunks({ workflowId: "p", streamId: "out" });
    await s.createSignalToken({
      tokenId: "tk",
      workflowId: "p",
      signalName: "go",
      bearer: "b",
      tags: [],
      idempotencyKey: "k",
      expiresAt: new Date(Date.now() + 60_000),
    });
    await s.markSignalTokenCompleted({ tokenId: "tk", value: 1, now: new Date() });

    // Journal: completed, pending sleep and signal, completion, discard.
    await s.appendEntry({
      workflowId: "p",
      stepName: "j",
      activityIndex: 0,
      activityName: "fetch",
      exit: { tag: "Success", value: 1 },
      guard,
    });
    await s.appendPendingEntry({
      workflowId: "p",
      stepName: "j",
      activityIndex: 1,
      activityName: "nap",
      stepType: "sleep",
      wakeAt: new Date(Date.now() - 1_000),
      guard,
    });
    await s.appendPendingEntry({
      workflowId: "p",
      stepName: "j",
      activityIndex: 2,
      activityName: "approve",
      stepType: "signal",
      guard,
    });
    await s.findDueSleeps({ now: new Date(), limit: 10 });
    await s.findPendingSignal({ workflowId: "p", stepName: "j", signalName: "approve" });
    await s.completePendingEntry({
      workflowId: "p",
      stepName: "j",
      activityIndex: 1,
      exit: { tag: "Success", value: 1 },
      guard,
    });
    await s.discardJournalEntries({
      workflowId: "p",
      stepName: "j",
      slots: [{ activityIndex: 2, branchPath: "" }],
      guard,
    });
    await s.loadJournal({ workflowId: "p", stepName: "j" });

    await s.suspendWorkflow({
      workflowId: "p",
      stepName: "w",
      stepUpdate: { status: "waiting_for_signal", signalName: "go" },
      guard,
    });
    await s.deliverSignal({ workflowId: "p", signalName: "go", payload: 1 });
    await s.listSignalWakeups({ limit: 10 });
    await s.listDueTimers({ now: new Date(), limit: 10 });
    await s.listOrphanedRuns({ now: new Date(), updatedBefore: new Date(), limit: 10 });
    await s.beginCompensation({ workflowId: "p", error: "boom", guard });
    await s.saveStepCompensation({ workflowId: "p", stepName: "a", status: "compensated", guard });
    await s.failWorkflow({ workflowId: "p", error: "boom", guard });
    await s.releaseLock({ workflowId: "p", guard });

    // Reads, listing, rewinds, fresh runs, purge.
    await s.tryLockAndLoad({ workflowId: "c", lockDurationMs: 30_000 });
    await s.loadWorkflowStatus("p");
    await s.loadRunHistory({ workflowId: "p" });
    await s.loadStepAttempts({ workflowId: "p" });
    await s.loadSignals("p");
    await s.listSignalTokensForWorkflow("p");
    await s.listWorkflows({ name: "child", parentId: "p", status: "pending" });
    await s.listWorkflows({ orderBy: "startedAt" });
    await s.listWorkflows({ namespace: "ns", type: "t", metadata: { k: 1 } });
    await s.listWorkflowSummaries({ limit: 5 });
    await s.countWorkflows({ name: "child", parentId: "p" });
    await s.countWorkflows({ version: "1" });
    await s.distinctWorkflowNames({ namespace: "ns" });
    await s.resetSteps({ workflowId: "p", stepNames: ["b", "j"] });
    await s.completeWorkflow({ workflowId: "p", result: 1 });
    await s.startFreshRun({ workflowId: "p" });
    await s.cancelWorkflow({ workflowId: "p", cascade: true });
    await s.tripwireWorkflow({ workflowId: "c", reason: "x" });
    expect(await s.purgeCompleted({ olderThanMs: -60_000, limit: 10 })).toBe(2);

    // Every recorded call stays within one slot.
    const offenders = calls.filter((call) => {
      const slots = new Set([...call.keys, ...call.derivedBases].map(keySlot));
      return slots.size > 1;
    });
    expect(offenders).toEqual([]);

    // Workflow keys spread over slots; the indexes share one.
    const used = new Set(calls.flatMap((c) => c.keys));
    const slotOf = (id: string) => keySlot(`${prefix}:{wf:${id}}`);
    expect(slotOf("p")).not.toBe(slotOf("c"));
    expect(keySlot(`${prefix}:{idx}:status:pending`)).toBe(keySlot(`${prefix}:{idx}:sleeps`));
    expect(used.has(`${prefix}:{idx}:ver`)).toBe(true);

    // The workload exercised every script the storage ships.
    const shipped = Object.entries(scripts).filter(
      (entry): entry is [string, string] =>
        entry[0].endsWith("_LUA") && typeof entry[1] === "string",
    );
    const ran = new Set(calls.map((c) => c.script));
    const unused = shipped
      .filter(([, lua]) => !ran.has(lua) && !ran.has(scripts.fencedLua(lua)))
      .map(([name]) => name);
    expect(unused).toEqual([]);
  });
});
