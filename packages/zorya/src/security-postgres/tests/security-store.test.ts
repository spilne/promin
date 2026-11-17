import { expect, it } from "bun:test";
import { FakeWallClock, SystemWallClock } from "@promin/workflow";
import { postgresDescribe } from "@promin/postgres/testing";
import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import { securityStoreTestSuite } from "../../security-module/tests/security-store-test-suite.ts";
import { BreakGlassAccessManager } from "../../security-module/break-glass.ts";
import { DurableSecurityAudit } from "../../security-module/security-audit.ts";
import { issueServiceAccountCredential } from "../../security-module/service-account-credentials.ts";
import { hashCsrfToken, hashSessionToken } from "../../security-module/sessions.ts";
import { createZoryaSecurity } from "../../security-module/zorya-security.ts";
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

    securityStoreTestSuite("conformance", {
      create: async () => {
        await reset();
        return new PostgresSecurityStore({ db: pg.db });
      },
      createClocked: async () => {
        await reset();
        return {
          store: new PostgresSecurityStore({ db: pg.db }),
          currentTimeMs: () => SystemWallClock.currentTimeMs(),
          advance: async (ms: number) => {
            await execRaw(pg.db, sql`SELECT pg_catalog.pg_sleep(${ms / 1_000})`);
          },
        };
      },
    });

    it("authenticates and revokes a durable credential across store instances", async () => {
      await reset();
      const clock = FakeWallClock.create(1_000);
      const writer = new PostgresSecurityStore({ db: pg.db, clock });
      await writer.putServiceAccount({
        id: "restart-agent",
        status: "active",
        canActFor: true,
      });
      await writer.putSubjectBinding({
        subject: { kind: "service-account", id: "restart-agent" },
        namespaceId: "team",
        accessRole: "member",
      });
      const issued = await issueServiceAccountCredential({
        store: writer,
        serviceAccountId: "restart-agent",
        pepper: "0123456789abcdef0123456789abcdef",
      });
      expect(issued.createdAt).toBeGreaterThan(clock.currentTimeMs());

      const reader = new PostgresSecurityStore({ db: pg.db, clock });
      const security = createZoryaSecurity({
        store: reader,
        loginProviders: [
          {
            id: "unused",
            authorizationUrl: async () => "https://identity.test/authorize",
            exchange: async () => {
              throw new Error("not used");
            },
          },
        ],
        publicBaseUrl: "https://zorya.test",
        allowedDomains: ["example.com"],
        auditSink: { recordAudit: async () => {} },
        secret: "0123456789abcdef0123456789abcdef",
        clock,
      });
      const request = () =>
        new Request("https://zorya.test/api/agents", {
          headers: { authorization: `Bearer ${issued.token}` },
        });
      expect(await security.authenticate({ request: request(), plane: "operator" })).toMatchObject({
        id: "restart-agent",
        accessRoles: { team: ["member"] },
        canActFor: true,
      });

      await writer.revokeServiceAccountCredential({
        id: issued.id,
        revokedAt: 1_001,
      });
      expect(
        await security.authenticate({ request: request(), plane: "operator" }),
      ).toBeUndefined();
    });

    it("issues, authenticates and revokes break-glass access across store instances", async () => {
      await reset();
      const clock = FakeWallClock.create(1_000);
      const writer = new PostgresSecurityStore({ db: pg.db, clock });
      const { user } = await writer.findOrCreateProviderUser({
        identity: {
          providerId: "test",
          issuer: "https://identity.test",
          subject: "emergency-admin",
        },
        email: "emergency-admin@example.com",
        bindings: [{ namespaceId: "org", accessRole: "admin" }],
      });
      let nextAuditId = 0;
      const writerAudit = new DurableSecurityAudit({
        store: writer,
        activeKeyId: "audit-key",
        keys: { "audit-key": "a".repeat(32) },
        clock,
        idFactory: () => `break-glass-${++nextAuditId}`,
      });
      const issued = await new BreakGlassAccessManager({
        store: writer,
        audit: writerAudit,
        publicBaseUrl: "https://zorya.test",
        idFactory: () => "postgres_grant",
        tokenSecretFactory: () => "s".repeat(43),
      }).issue({
        principal: { kind: "user", id: user.id },
        reason: "OIDC outage",
        createdBy: "host-operator",
      });
      expect(issued.createdAt).toBeGreaterThan(clock.currentTimeMs());

      const reader = new PostgresSecurityStore({ db: pg.db, clock });
      const readerAudit = new DurableSecurityAudit({
        store: reader,
        activeKeyId: "audit-key",
        keys: { "audit-key": "a".repeat(32) },
        clock,
      });
      const security = createZoryaSecurity({
        store: reader,
        loginProviders: [
          {
            id: "unused",
            authorizationUrl: async () => "https://identity.test/authorize",
            exchange: async () => {
              throw new Error("not used");
            },
          },
        ],
        publicBaseUrl: "https://zorya.test",
        allowedDomains: ["example.com"],
        auditSink: readerAudit,
        secret: "0123456789abcdef0123456789abcdef",
        clock,
      });
      const request = () =>
        new Request("https://zorya.test/api/agents", {
          headers: { authorization: issued.authorizationHeader },
        });
      expect(await security.authenticate({ request: request(), plane: "operator" })).toMatchObject({
        id: user.id,
        accessRoles: { org: ["admin"] },
      });

      const restartedManager = new BreakGlassAccessManager({
        store: reader,
        audit: readerAudit,
        publicBaseUrl: "https://zorya.test",
      });
      expect(
        await restartedManager.revoke({ grantId: issued.grantId, revokedBy: "recovery-operator" }),
      ).toBe("revoked");
      expect(
        await security.authenticate({ request: request(), plane: "operator" }),
      ).toBeUndefined();
      expect(await readerAudit.verify("org")).toMatchObject({ valid: true });
    });

    it("authenticates and invalidates a CSRF-bound OIDC session across store instances", async () => {
      await reset();
      const clock = FakeWallClock.create(1_000);
      const writer = new PostgresSecurityStore({ db: pg.db, clock });
      const { user } = await writer.findOrCreateProviderUser({
        identity: {
          providerId: "oidc",
          issuer: "https://identity.test",
          subject: "restart-user",
        },
        email: "restart-user@example.com",
        bindings: [{ namespaceId: "team", accessRole: "member" }],
      });
      const csrf = "csrf-restart-token";
      const firstToken = "session-restart-token";
      const firstCreatedAt = await writer.currentTimeMs();
      await writer.putSession({
        tokenHash: hashSessionToken(firstToken),
        principal: { kind: "user", id: user.id },
        createdAt: firstCreatedAt,
        expiresAt: firstCreatedAt + 60_000,
        csrfTokenHash: hashCsrfToken(csrf),
        authMethod: "oidc",
      });

      const reader = new PostgresSecurityStore({ db: pg.db, clock });
      const security = createZoryaSecurity({
        store: reader,
        loginProviders: [
          {
            id: "unused",
            authorizationUrl: async () => "https://identity.test/authorize",
            exchange: async () => {
              throw new Error("not used");
            },
          },
        ],
        publicBaseUrl: "https://zorya.test",
        allowedDomains: ["example.com"],
        auditSink: { recordAudit: async () => {} },
        secret: "0123456789abcdef0123456789abcdef",
        clock,
      });
      const request = (token: string) =>
        new Request("https://zorya.test/api/agents", {
          headers: {
            cookie: `__Host-zorya_session=${token}; __Host-zorya_csrf=${csrf}`,
          },
        });
      expect(
        await security.authenticate({
          request: request(firstToken),
          plane: "operator",
        }),
      ).toMatchObject({ id: user.id, accessRoles: { team: ["member"] } });
      await writer.revokeSession({
        tokenHash: hashSessionToken(firstToken),
        revokedAt: firstCreatedAt + 1,
      });
      expect(
        await security.authenticate({
          request: request(firstToken),
          plane: "operator",
        }),
      ).toBeUndefined();

      const secondToken = "session-disabled-user-token";
      const secondCreatedAt = await writer.currentTimeMs();
      await writer.putSession({
        tokenHash: hashSessionToken(secondToken),
        principal: { kind: "user", id: user.id },
        createdAt: secondCreatedAt,
        expiresAt: secondCreatedAt + 60_000,
        csrfTokenHash: hashCsrfToken(csrf),
        authMethod: "oidc",
      });
      await writer.setUserStatus({ id: user.id, status: "disabled" });
      expect(
        await security.authenticate({
          request: request(secondToken),
          plane: "operator",
        }),
      ).toBeUndefined();
    });

    it("resolves access from one snapshot across a concurrent disable", async () => {
      await reset();
      const admin = new PostgresSecurityStore({ db: pg.db });
      const { user } = await admin.findOrCreateProviderUser({
        identity: {
          providerId: "test",
          issuer: "snapshot",
          subject: "subject",
        },
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
        await raced.resolveAccess({
          subject: { kind: "user", id: user.id },
          namespaceId: "team",
        }),
      ).toEqual({
        accessRoles: ["direct", "group-role"],
        groups: ["snapshot-group"],
      });
      expect(statements).toBe(1);
      expect(
        await admin.resolveAccess({
          subject: { kind: "user", id: user.id },
          namespaceId: "team",
        }),
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
            CREATE TABLE zsec_attacker.zsec_access_group (subject_kind text, id text);
            CREATE DOMAIN zsec_attacker.text AS pg_catalog.text CHECK (VALUE IS NULL);
            CREATE DOMAIN zsec_attacker.int8 AS pg_catalog.int8 CHECK (VALUE IS NULL);
            CREATE DOMAIN zsec_attacker.float8 AS pg_catalog.float8 CHECK (VALUE IS NULL);
            CREATE DOMAIN zsec_attacker.jsonb AS pg_catalog.jsonb CHECK (VALUE IS NULL);
            CREATE FUNCTION zsec_attacker.clock_timestamp()
              RETURNS pg_catalog.timestamptz LANGUAGE sql IMMUTABLE
              AS 'SELECT ''1970-01-01 UTC''::pg_catalog.timestamptz';
            CREATE FUNCTION zsec_attacker.random()
              RETURNS pg_catalog.float8 LANGUAGE sql IMMUTABLE
              AS 'SELECT 0::pg_catalog.float8';
            CREATE FUNCTION zsec_attacker.md5(pg_catalog.text)
              RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE
              AS 'SELECT ''attacker-token''::pg_catalog.text';
            CREATE FUNCTION zsec_attacker.date_part(pg_catalog.text, pg_catalog.timestamptz)
              RETURNS pg_catalog.float8 LANGUAGE sql IMMUTABLE
              AS 'SELECT 0::pg_catalog.float8';
            CREATE FUNCTION zsec_attacker.floor(pg_catalog.float8)
              RETURNS pg_catalog.float8 LANGUAGE sql IMMUTABLE
              AS 'SELECT 0::pg_catalog.float8';
            CREATE FUNCTION zsec_attacker.hashtextextended(pg_catalog.text, pg_catalog.int8)
              RETURNS pg_catalog.int8 LANGUAGE sql IMMUTABLE
              AS 'SELECT 0::pg_catalog.int8';
            CREATE FUNCTION zsec_attacker.pg_advisory_xact_lock(pg_catalog.int8)
              RETURNS pg_catalog.void LANGUAGE plpgsql
              AS 'BEGIN RAISE EXCEPTION ''attacker advisory lock called''; END';
            CREATE FUNCTION zsec_attacker.poison_array(pg_catalog.text[], pg_catalog.text)
              RETURNS pg_catalog.text[] LANGUAGE sql IMMUTABLE
              AS 'SELECT ARRAY[''attacker-value'']::pg_catalog.text[]';
            CREATE AGGREGATE zsec_attacker.array_agg(pg_catalog.text) (
              SFUNC = zsec_attacker.poison_array,
              STYPE = pg_catalog.text[],
              INITCOND = '{}'
            )`,
      );
      try {
        await pg.db.transaction(async (rawTx) => {
          const tx = rawTx as unknown as DrizzleDb;
          await execRaw(tx, sql`SET LOCAL search_path = zsec_attacker, pg_catalog, public`);
          const store = new PostgresSecurityStore({ db: tx });
          await store.putGroup({ id: "qualified-group", status: "active" });
          expect(await store.getGroup("qualified-group")).toEqual({
            id: "qualified-group",
            status: "active",
          });
          const { user } = await store.findOrCreateProviderUser({
            identity: {
              providerId: "qualified-builtins",
              issuer: "https://qualified.example",
              subject: "qualified-builtins",
            },
            email: "qualified-builtins@example.com",
          });
          await store.putSubjectBinding({
            subject: { kind: "user", id: user.id },
            namespaceId: "team",
            accessRole: "member",
          });
          expect(
            await store.resolveAccess({
              subject: { kind: "user", id: user.id },
              namespaceId: "team",
            }),
          ).toEqual({ accessRoles: ["member"], groups: [] });

          await store.appendAuditWithOutbox({
            record: {
              id: "qualified-builtins",
              namespaceId: "qualified-builtins",
              sequence: 1,
              occurredAt: 1,
              recordedAt: 1,
              event: "qualified-builtins",
              metadata: { source: "qualified-builtins" },
              hash: "qualified-builtins-hash",
              keyId: "test-key",
            },
            outbox: { nextAttemptAt: 0 },
          });
          const [lease] = await store.claimAuditOutbox({
            owner: "qualified-worker",
            leaseMs: 100,
            limit: 1,
          });
          expect(lease?.leaseToken).not.toBe("attacker-token");
          expect(lease?.leaseExpiresAt).toBeGreaterThan(Date.now());
          await Bun.sleep(150);
          expect(
            await store.ackAuditOutbox({
              eventId: lease!.eventId,
              leaseToken: lease!.leaseToken!,
            }),
          ).toBe(false);
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
        const [lease] = await store.claimAuditOutbox({
          owner: "worker",
          leaseMs: 200,
          limit: 1,
        });
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
        nextAttemptAt: (await store.currentTimeMs()) + 10_000,
        lastError: "retry",
      });
      await Bun.sleep(300);
      await releaseRetryBlock();
      expect(await retry).toBe(false);
    });
  },
);
