import { describe, expect, test } from "bun:test";
import { createElevatedTool } from "@promin/agent";
import { z } from "zod";
import type { Authorizer, SecurityContext } from "../../server/security/security.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import {
  DurableSecurityAudit,
  SecurityAuditAccessDeniedError,
  SecurityAuditReader,
  SecurityToolAuditBridge,
  verifySecurityAuditRecords,
} from "../security-audit.ts";
import type { SecurityAuditRecord, SecurityStoreV2 } from "../security-store.ts";

const KEY_A = "a".repeat(32);
const KEY_B = "b".repeat(32);

function audit(
  store: SecurityStoreV2,
  options: { keyId?: string; keys?: Readonly<Record<string, string>>; clock?: number } = {},
) {
  let nextId = 0;
  const now = options.clock ?? 2_000;
  return new DurableSecurityAudit({
    store,
    activeKeyId: options.keyId ?? "key-a",
    keys: options.keys ?? { "key-a": KEY_A },
    clock: { currentTimeMs: () => now },
    idFactory: () => `event-${++nextId}`,
  });
}

async function rows(store: SecurityStoreV2, namespaceId = "acme"): Promise<SecurityAuditRecord[]> {
  return [...(await store.listAuditRecords({ namespaceId, limit: 1_000 })).items];
}

describe("DurableSecurityAudit", () => {
  test("concurrent replicas retry the atomic head boundary and survive restart/key rotation", async () => {
    const store = new InMemorySecurityStore();
    let nextId = 0;
    const config = {
      store,
      activeKeyId: "key-a",
      keys: { "key-a": KEY_A, "key-b": KEY_B },
      clock: { currentTimeMs: () => 2_000 },
      idFactory: () => `concurrent-${++nextId}`,
    } as const;
    const replicas = [new DurableSecurityAudit(config), new DurableSecurityAudit(config)];

    await Promise.all(
      Array.from({ length: 40 }, (_, index) =>
        replicas[index % 2]!.append({
          namespaceId: "acme",
          occurredAt: 1_000,
          event: "authorization",
          metadata: { index },
        }),
      ),
    );

    const restarted = new DurableSecurityAudit({ ...config, activeKeyId: "key-b" });
    await restarted.append({ namespaceId: "acme", occurredAt: 1_500, event: "server.restarted" });
    const result = await restarted.verify("acme");
    expect(result.valid).toBe(true);
    expect((await rows(store)).map((row) => row.sequence)).toEqual(
      Array.from({ length: 41 }, (_, index) => index + 1),
    );
    expect((await rows(store)).at(-1)?.keyId).toBe("key-b");
  });

  test("detects edits, insertion/deletion/reordering, and checkpointed suffix truncation", async () => {
    const store = new InMemorySecurityStore();
    const sink = audit(store);
    for (let index = 0; index < 4; index += 1) {
      await sink.append({ namespaceId: "acme", occurredAt: 1_000, event: `event.${index}` });
    }
    const original = await rows(store);
    const keys = { "key-a": KEY_A };
    const verified = verifySecurityAuditRecords(original, keys);
    expect(verified.valid).toBe(true);
    if (!verified.valid || verified.checkpoint === undefined) throw new Error("missing checkpoint");

    expect(
      verifySecurityAuditRecords(
        [{ ...original[0]!, event: "edited" }, ...original.slice(1)],
        keys,
      ),
    ).toMatchObject({ valid: false, reason: "hash_mismatch" });
    expect(verifySecurityAuditRecords([original[0]!, ...original.slice(2)], keys)).toMatchObject({
      valid: false,
      reason: "sequence_mismatch",
    });
    expect(
      verifySecurityAuditRecords([original[1]!, original[0]!, ...original.slice(2)], keys),
    ).toMatchObject({
      valid: false,
      reason: "sequence_mismatch",
    });
    expect(
      verifySecurityAuditRecords(
        [original[0]!, { ...original[1]!, sequence: 2, id: "inserted" }, ...original.slice(1)],
        keys,
      ),
    ).toMatchObject({ valid: false });
    expect(verifySecurityAuditRecords(original.slice(0, -1), keys, verified.checkpoint)).toEqual({
      valid: false,
      reason: "suffix_truncated",
      sequence: 4,
    });
  });

  test("maps core route events with actor/effective/correlation and redacts bounded metadata", async () => {
    const store = new InMemorySecurityStore();
    const sink = audit(store);
    await sink.recordAudit({
      event: "authorization",
      occurredAt: new Date(1_000).toISOString(),
      namespaceId: "acme",
      context: context,
      permission: { resource: "thread", action: "read" },
      resource: { kind: "thread", namespaceId: "acme", id: "thread-1" },
      decision: { allowed: true, reason: "access_role_allow" },
      metadata: {
        requestId: "request-1",
        runId: "run-1",
        threadId: "thread-1",
        token: "do-not-store",
        query: "private question",
        safe: "visible",
      },
    });
    const [row] = await rows(store);
    expect(row).toMatchObject({
      actor: { kind: "user", id: "alice" },
      effective: { kind: "service-account", id: "research-bot" },
      correlation: { requestId: "request-1", runId: "run-1", threadId: "thread-1" },
      metadata: { token: "[REDACTED]", query: "[REDACTED]", safe: "visible" },
    });
    expect(JSON.stringify(row)).not.toContain("do-not-store");
    expect(JSON.stringify(row)).not.toContain("private question");
  });

  test("atomically creates optional delivery outbox rows", async () => {
    const store = new InMemorySecurityStore();
    const sink = new DurableSecurityAudit({
      store,
      activeKeyId: "key-a",
      keys: { "key-a": KEY_A },
      clock: { currentTimeMs: () => 2_000 },
      idFactory: () => "with-outbox",
      outbox: { nextAttemptAt: (recordedAt) => recordedAt + 500 },
    });
    await sink.append({ namespaceId: "acme", occurredAt: 1_000, event: "export.ready" });
    expect(await store.getAuditOutbox("with-outbox")).toMatchObject({
      eventId: "with-outbox",
      status: "pending",
      nextAttemptAt: 2_500,
    });
  });

  test("rejects an event-id collision instead of treating it as a retry", async () => {
    const store = new InMemorySecurityStore();
    const sink = new DurableSecurityAudit({
      store,
      activeKeyId: "key-a",
      keys: { "key-a": KEY_A },
      clock: { currentTimeMs: () => 2_000 },
      idFactory: () => "collision",
    });
    await sink.append({ namespaceId: "acme", occurredAt: 1_000, event: "first" });
    await expect(
      sink.append({ namespaceId: "acme", occurredAt: 1_000, event: "second" }),
    ).rejects.toThrow("Conflicting audit event: collision");
  });
});

describe("SecurityToolAuditBridge", () => {
  test("copies ToolScope identity into intent/outcome and never stores raw sensitive/error payloads", async () => {
    const store = new InMemorySecurityStore();
    const sink = audit(store);
    const bridge = new SecurityToolAuditBridge(sink, { currentTimeMs: () => 1_500 });
    const elevated = createElevatedTool({
      name: "filesystem_write",
      description: "write a file",
      parameters: z.object({}),
      execute: async (_input, toolContext) => {
        await toolContext.audit({
          action: "write",
          target: "src/proof.ts",
          meta: {
            safe: "yes",
            secret: "top-secret",
            query: "raw-query",
            nested: { result: "raw" },
          },
        });
        return "raw-tool-result";
      },
    });

    await elevated.execute(
      {},
      {
        scope: {
          namespaceId: "acme",
          resourceId: "alice-resource",
          agentId: "coding-agent",
          securityContext: {
            actor: { kind: "user", id: "alice" },
            effective: { kind: "service-account", id: "coding-bot" },
          },
        },
        toolCallId: "call-1",
        toolAuditLogger: bridge,
      },
    );

    const auditRows = await rows(store);
    expect(auditRows.map((row) => row.event)).toEqual([
      "tool.elevated.intent",
      "tool.elevated.completed",
    ]);
    for (const row of auditRows) {
      expect(row.actor).toEqual({ kind: "user", id: "alice" });
      expect(row.effective).toEqual({ kind: "service-account", id: "coding-bot" });
      expect(row.correlation).toEqual({ toolCallId: "call-1" });
      expect(row.metadata).toMatchObject({
        action: "write",
        "toolMeta.safe": "yes",
        "toolMeta.secret": "[REDACTED]",
        "toolMeta.query": "[REDACTED]",
      });
      expect(JSON.stringify(row)).not.toContain("top-secret");
      expect(JSON.stringify(row)).not.toContain("raw-query");
      expect(JSON.stringify(row)).not.toContain("raw-tool-result");
    }
  });

  test("fails an elevated action closed when durable intent persistence fails", async () => {
    const store = new InMemorySecurityStore();
    const broken = new Proxy(store, {
      get(target, property, receiver) {
        if (property === "appendAuditWithOutbox")
          return async () => Promise.reject(new Error("store offline"));
        const value = Reflect.get(target, property, receiver);
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as SecurityStoreV2;
    const bridge = new SecurityToolAuditBridge(audit(broken));
    const elevated = createElevatedTool({
      name: "dangerous",
      description: "dangerous action",
      parameters: z.object({}),
      execute: async (_input, toolContext) => {
        await toolContext.audit({ action: "mutate" });
        throw new Error("should not reach side effect");
      },
    });
    await expect(
      elevated.execute(
        {},
        { scope: { namespaceId: "acme", resourceId: "alice" }, toolAuditLogger: bridge },
      ),
    ).rejects.toThrow("store offline");
  });

  test("maps failed outcomes without persisting the raw error", async () => {
    const store = new InMemorySecurityStore();
    const bridge = new SecurityToolAuditBridge(audit(store), { currentTimeMs: () => 1_500 });
    const elevated = createElevatedTool({
      name: "connector_admin",
      description: "change a connector",
      parameters: z.object({}),
      execute: async (_input, toolContext) => {
        await toolContext.audit({ action: "disable", target: "connector-1" });
        throw new Error("upstream included secret-token");
      },
    });

    await expect(
      elevated.execute(
        {},
        {
          scope: { namespaceId: "acme", resourceId: "alice" },
          toolCallId: "failed-call",
          toolAuditLogger: bridge,
        },
      ),
    ).rejects.toThrow("upstream included secret-token");
    const auditRows = await rows(store);
    expect(auditRows.map((row) => row.event)).toEqual([
      "tool.elevated.intent",
      "tool.elevated.failed",
    ]);
    expect(auditRows[1]?.metadata).toMatchObject({ failed: true, phase: "failed" });
    expect(JSON.stringify(auditRows)).not.toContain("secret-token");
  });
});

describe("SecurityAuditReader", () => {
  test("authorizes and audits cursor search/export without persisting filter values", async () => {
    const store = new InMemorySecurityStore();
    const sink = audit(store);
    await sink.append({
      namespaceId: "acme",
      occurredAt: 1_000,
      event: "wanted",
      correlation: { toolCallId: "private-call" },
    });
    await sink.append({ namespaceId: "acme", occurredAt: 1_000, event: "other" });
    const allow: Authorizer = {
      authorize: async () => ({ allowed: true, reason: "operator_allow" }),
    };
    const reader = new SecurityAuditReader(store, allow, sink, { currentTimeMs: () => 2_000 });

    const search = await reader.search({
      context,
      namespaceId: "acme",
      filter: { event: "wanted", toolCallId: "private-call" },
      limit: 1,
    });
    expect(search.items.map((row) => row.event)).toEqual(["wanted"]);
    const exported = await reader.exportPage({ context, namespaceId: "acme", limit: 1 });
    expect(exported.jsonl.split("\n")).toHaveLength(1);
    const all = await rows(store);
    expect(all.map((row) => row.event)).toContain("audit.search.authorization");
    expect(all.map((row) => row.event)).toContain("audit.export.authorization");
    expect(all.find((row) => row.event === "audit.search.authorization")?.metadata).toEqual({
      byActor: false,
      byDecision: false,
      byEffective: false,
      byEvent: true,
      byTime: false,
      byToolCall: true,
      filtered: true,
    });
  });

  test("denies namespace reads and still records the denial", async () => {
    const store = new InMemorySecurityStore();
    const sink = audit(store);
    const deny: Authorizer = {
      authorize: async () => ({ allowed: false, reason: "namespace_denied" }),
    };
    const reader = new SecurityAuditReader(store, deny, sink, { currentTimeMs: () => 2_000 });
    await expect(reader.search({ context, namespaceId: "acme" })).rejects.toBeInstanceOf(
      SecurityAuditAccessDeniedError,
    );
    expect((await rows(store))[0]).toMatchObject({
      event: "audit.search.authorization",
      decision: { allowed: false, reason: "namespace_denied" },
    });
  });
});

const context: SecurityContext = {
  actor: {
    kind: "user",
    id: "alice",
    accessRoles: { acme: ["admin"] },
    canActFor: true,
  },
  effective: { kind: "service-account", id: "research-bot" },
};
