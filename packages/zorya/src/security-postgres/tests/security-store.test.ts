import { expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { postgresDescribe } from "@promin/postgres/testing";
import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import { securityStoreV2TestSuite } from "../../security-module/tests/security-store-v2-test-suite.ts";
import { migrateSecurityStore } from "../migrate.ts";
import { PostgresSecurityStore } from "../security-store.ts";

postgresDescribe(
  "PostgresSecurityStore",
  { migrate: migrateSecurityStore, timeout: 120_000 },
  (pg) => {
    async function reset(): Promise<void> {
      await execRaw(
        pg.db,
        sql`TRUNCATE zorya_security_data.zsec_break_glass_grant,
          zorya_security_data.zsec_denial_bucket,
          zorya_security_data.zsec_audit_outbox,
          zorya_security_data.zsec_audit_record,
          zorya_security_data.zsec_session,
          zorya_security_data.zsec_service_account_credential,
          zorya_security_data.zsec_knowledge_audience_grant,
          zorya_security_data.zsec_access_binding,
          zorya_security_data.zsec_group_membership,
          zorya_security_data.zsec_provider_identity,
          zorya_security_data.zsec_service_account,
          zorya_security_data.zsec_access_group,
          zorya_security_data.zsec_user,
          zorya_security_data.zsec_subject CASCADE`,
      );
    }

    securityStoreV2TestSuite("v2 conformance", {
      create: async () => {
        await reset();
        return new PostgresSecurityStore({ db: pg.db });
      },
      createClocked: async () => {
        await reset();
        const clock = FakeWallClock.create(1_000);
        return {
          store: new PostgresSecurityStore({ db: pg.db, clock }),
          currentTimeMs: () => clock.currentTimeMs(),
          advance: (ms: number) => clock.advance(ms),
        };
      },
    });

    it("resolves access from one snapshot across a concurrent disable", async () => {
      await reset();
      const admin = new PostgresSecurityStore({ db: pg.db });
      const { user } = await admin.findOrCreateProviderUser({
        identity: { providerId: "test", issuer: "snapshot", subject: "subject" },
        email: "snapshot@example.com",
      });
      await admin.putGroup({ id: "snapshot-group", status: "active" });
      await admin.putGroupMembership({
        groupId: "snapshot-group",
        subject: { kind: "user", id: user.id },
      });
      await admin.putSubjectBinding({
        subject: { kind: "user", id: user.id },
        namespaceId: "team",
        accessRole: "direct",
      });
      await admin.putSubjectBinding({
        subject: { kind: "group", id: "snapshot-group" },
        namespaceId: "team",
        accessRole: "group-role",
      });

      let statements = 0;
      const racedDb = {
        execute: async (query: Parameters<DrizzleDb["execute"]>[0]) => {
          statements += 1;
          const result = await pg.db.execute(query);
          if (statements === 1) {
            await admin.setUserStatus({ id: user.id, status: "disabled" });
            await admin.putGroup({ id: "snapshot-group", status: "disabled" });
          }
          return result;
        },
      } as DrizzleDb;
      const raced = new PostgresSecurityStore({ db: racedDb });

      expect(
        await raced.resolveAccess({ subject: { kind: "user", id: user.id }, namespaceId: "team" }),
      ).toEqual({ accessRoles: ["direct", "group-role"], groups: ["snapshot-group"] });
      expect(statements).toBe(1);
      expect(
        await admin.resolveAccess({ subject: { kind: "user", id: user.id }, namespaceId: "team" }),
      ).toEqual({ accessRoles: [], groups: [] });
    });

    it("ignores a hostile search_path for runtime reads and writes", async () => {
      await reset();
      await execRaw(
        pg.db,
        sql`DROP SCHEMA IF EXISTS zsec_attacker CASCADE;
            CREATE SCHEMA zsec_attacker;
            CREATE TABLE zsec_attacker.zsec_subject
              (kind text, id text, status text, created_at bigint, updated_at bigint);
            CREATE TABLE zsec_attacker.zsec_access_group (subject_kind text, id text)`,
      );
      try {
        await pg.db.transaction(async (rawTx) => {
          const tx = rawTx as unknown as DrizzleDb;
          await execRaw(tx, sql`SET LOCAL search_path = zsec_attacker, public`);
          const store = new PostgresSecurityStore({ db: tx });
          await store.putGroup({ id: "qualified-group", status: "active" });
          expect(await store.getGroup("qualified-group")).toEqual({
            id: "qualified-group",
            status: "active",
          });
        });

        const [real] = await execRaw(
          pg.db,
          sql`SELECT count(*)::int AS count FROM zorya_security_data.zsec_access_group WHERE id='qualified-group'`,
        );
        const [decoy] = await execRaw(
          pg.db,
          sql`SELECT count(*)::int AS count FROM zsec_attacker.zsec_access_group`,
        );
        expect(real?.count).toBe(1);
        expect(decoy?.count).toBe(0);
      } finally {
        await execRaw(pg.db, sql`DROP SCHEMA IF EXISTS zsec_attacker CASCADE`);
      }
    });

    it("checks lease expiry after a blocked acknowledgement or release acquires its row", async () => {
      await reset();
      const store = new PostgresSecurityStore({ db: pg.db });

      async function createLease(
        eventId: string,
      ): Promise<{ eventId: string; leaseToken: string }> {
        await store.appendAuditWithOutbox({
          record: {
            id: eventId,
            namespaceId: eventId,
            sequence: 1,
            occurredAt: 1,
            recordedAt: 1,
            event: "lease.test",
            hash: `${eventId}-hash`,
            keyId: "test-key",
          },
          outbox: { nextAttemptAt: 0 },
        });
        const [lease] = await store.claimAuditOutbox({ owner: "worker", leaseMs: 200, limit: 1 });
        return { eventId, leaseToken: lease!.leaseToken! };
      }

      async function holdLeaseRow(eventId: string): Promise<() => Promise<void>> {
        let rowLocked!: () => void;
        let releaseRow!: () => void;
        const locked = new Promise<void>((resolve) => (rowLocked = resolve));
        const released = new Promise<void>((resolve) => (releaseRow = resolve));
        const blocker = pg.db.transaction(async (rawTx) => {
          const tx = rawTx as unknown as DrizzleDb;
          await execRaw(
            tx,
            sql`SELECT event_id FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${eventId} FOR UPDATE`,
          );
          rowLocked();
          await released;
        });
        await locked;
        return async () => {
          releaseRow();
          await blocker;
        };
      }

      const ackLease = await createLease("blocked-ack");
      const releaseAckBlock = await holdLeaseRow(ackLease.eventId);
      const acknowledgement = store.ackAuditOutbox(ackLease);
      await Bun.sleep(300);
      await releaseAckBlock();
      expect(await acknowledgement).toBe(false);

      const releaseLease = await createLease("blocked-release");
      const releaseRetryBlock = await holdLeaseRow(releaseLease.eventId);
      const retry = store.releaseAuditOutbox({
        ...releaseLease,
        nextAttemptAt: Date.now() + 10_000,
        lastError: "retry",
      });
      await Bun.sleep(300);
      await releaseRetryBlock();
      expect(await retry).toBe(false);
    });
  },
);
