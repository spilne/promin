import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

const when = new Date("2026-04-16T12:00:00Z");

describe("sqlite: workflow result codec", () => {
  it("a completed run reads back with real Date / Map / BigInt from a second runner on the same db", async () => {
    const db = new Database(":memory:");
    const wf = workflow({ name: "sqlite-result-codec" })
      .step("produce", async () => ({
        at: when,
        big: 12345678901234567890n,
        m: new Map([["k", 1]]),
      }))
      .build({ idempotency: { ttl: 60_000 } });

    const first = createWorkflowRunner({ storage: SqliteWorkflowStorage.make({ db }) });
    await first.run({ workflow: wf, workflowId: "s1", input: {} });

    const other = createWorkflowRunner({ storage: SqliteWorkflowStorage.make({ db }) });
    const check = (v: unknown) => {
      const r = v as { at: Date; big: bigint; m: Map<string, number> };
      expect(r.at).toBeInstanceOf(Date);
      expect(r.at.getTime()).toBe(when.getTime());
      expect(r.big).toBe(12345678901234567890n);
      expect(r.m.get("k")).toBe(1);
    };
    check(await other.run({ workflow: wf, workflowId: "s1", input: {} }));
    check(await other.handle("s1").result());
  });
});
