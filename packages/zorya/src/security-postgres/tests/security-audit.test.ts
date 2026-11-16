import { expect, it } from "bun:test";
import { postgresDescribe } from "@promin/postgres/testing";
import { execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import { DurableSecurityAudit } from "../../security-module/security-audit.ts";
import { migrateSecurityStore } from "../migrate.ts";
import { PostgresSecurityStore } from "../security-store.ts";

postgresDescribe(
  "Postgres durable security audit",
  { migrate: migrateSecurityStore, timeout: 120_000 },
  (pg) => {
    it("serializes concurrent replicas and verifies after restart", async () => {
      await execRaw(
        pg.db,
        sql`TRUNCATE zorya_security_data.zsec_audit_outbox,
          zorya_security_data.zsec_audit_record CASCADE`,
      );
      let leftId = 0;
      let rightId = 0;
      const base = {
        activeKeyId: "audit-key",
        keys: { "audit-key": "a".repeat(32) },
        clock: { currentTimeMs: () => 2_000 },
      } as const;
      const left = new DurableSecurityAudit({
        ...base,
        store: new PostgresSecurityStore({ db: pg.db }),
        idFactory: () => `left-${++leftId}`,
      });
      const right = new DurableSecurityAudit({
        ...base,
        store: new PostgresSecurityStore({ db: pg.db }),
        idFactory: () => `right-${++rightId}`,
      });

      await Promise.all(
        Array.from({ length: 20 }, (_, index) =>
          (index % 2 === 0 ? left : right).append({
            namespaceId: "audit-replicas",
            occurredAt: 1_000,
            event: "concurrent.event",
            metadata: { index },
          }),
        ),
      );

      const restarted = new DurableSecurityAudit({
        ...base,
        store: new PostgresSecurityStore({ db: pg.db }),
      });
      expect(await restarted.verify("audit-replicas")).toMatchObject({ valid: true });
      expect(
        (await restarted.list({ namespaceId: "audit-replicas", limit: 100 })).items.map(
          (row) => row.sequence,
        ),
      ).toEqual(Array.from({ length: 20 }, (_, index) => index + 1));
    });
  },
);
