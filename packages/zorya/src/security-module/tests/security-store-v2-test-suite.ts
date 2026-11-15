import { describe, expect, it } from "bun:test";
import {
  SECURITY_STORE_LIMITS,
  type BreakGlassGrant,
  type SecurityAuditRecord,
  type SecuritySession,
  type SecurityStoreV2,
} from "../security-store.ts";

export interface SecurityStoreV2TestFactory {
  readonly create: () => SecurityStoreV2 | Promise<SecurityStoreV2>;
  readonly createClocked: () =>
    | SecurityStoreV2ClockedFixture
    | Promise<SecurityStoreV2ClockedFixture>;
}

export interface SecurityStoreV2ClockedFixture {
  readonly store: SecurityStoreV2;
  readonly currentTimeMs: () => number;
  readonly advance: (ms: number) => void | Promise<void>;
}

/** Reusable contract suite for the in-memory and future PostgreSQL stores. */
export function securityStoreV2TestSuite(name: string, factory: SecurityStoreV2TestFactory): void {
  describe(name, () => {
    it("keys OIDC identity by provider, issuer and subject instead of email", async () => {
      const store = await factory.create();
      const identity = {
        providerId: "oidc",
        issuer: "https://id.example",
        subject: "sub-1",
      };
      const [first, concurrent] = await Promise.all([
        store.findOrCreateProviderUser({
          identity,
          email: "first@example.com",
        }),
        store.findOrCreateProviderUser({
          identity,
          email: "first@example.com",
        }),
      ]);
      expect(first.user.id).toBe(concurrent.user.id);
      expect([first.created, concurrent.created].filter(Boolean)).toHaveLength(1);
      const renamed = await store.findOrCreateProviderUser({
        identity,
        email: "renamed@example.com",
      });
      expect(renamed.user).toMatchObject({
        id: first.user.id,
        email: "renamed@example.com",
      });
      const otherSubject = await store.findOrCreateProviderUser({
        identity: { ...identity, subject: "sub-2" },
        email: "renamed@example.com",
      });
      expect(otherSubject.user.id).not.toBe(first.user.id);

      const rejectedIdentity = { ...identity, subject: "rejected" };
      await expect(
        store.findOrCreateProviderUser({
          identity: rejectedIdentity,
          email: "rejected@example.com",
          bindings: [{ namespaceId: "team\u0000", accessRole: "member" }],
        }),
      ).rejects.toThrow("safe string");
      expect(await store.getProviderIdentity(rejectedIdentity)).toBeUndefined();
    });

    it("requires existing collision-safe principals for every privilege-bearing edge", async () => {
      const store = await factory.create();
      const user = await createUser(store, "same-id-user");
      await store.putServiceAccount({
        id: user.id,
        status: "active",
        canActFor: false,
      });
      await store.putSubjectBinding({
        subject: { kind: "user", id: user.id },
        namespaceId: "team",
        accessRole: "member",
      });
      await store.putSubjectBinding({
        subject: { kind: "service-account", id: user.id },
        namespaceId: "team",
        accessRole: "agent-manager",
      });
      expect(
        await store.resolveAccess({
          subject: { kind: "user", id: user.id },
          namespaceId: "team",
        }),
      ).toEqual({ accessRoles: ["member"], groups: [] });
      expect(
        await store.resolveAccess({
          subject: { kind: "service-account", id: user.id },
          namespaceId: "team",
        }),
      ).toEqual({ accessRoles: ["agent-manager"], groups: [] });

      const orphan = { kind: "service-account", id: "future-admin" } as const;
      await expect(
        store.putSubjectBinding({
          subject: orphan,
          namespaceId: "org",
          accessRole: "admin",
        }),
      ).rejects.toThrow("Unknown service-account");
      await store.putGroup({ id: "admins", status: "active" });
      await expect(
        store.putGroupMembership({ groupId: "admins", subject: orphan }),
      ).rejects.toThrow("Unknown service-account");
      await expect(
        store.putSession({
          tokenHash: "orphan-session",
          principal: orphan,
          createdAt: 1,
          expiresAt: 2,
          authMethod: "service-account",
        }),
      ).rejects.toThrow("Unknown or disabled service-account");
      await expect(
        store.createSession({
          tokenHash: "legacy-orphan-session",
          userId: orphan.id,
          ttlMs: 1_000,
        }),
      ).rejects.toThrow("Unknown or disabled user");
      expect(await store.resolveAccess({ subject: orphan, namespaceId: "team" })).toEqual({
        accessRoles: [],
        groups: [],
      });
    });

    it("unions direct, group and org roles and revokes membership immediately", async () => {
      const store = await factory.create();
      const user = await createUser(store, "group-user");
      const subject = { kind: "user", id: user.id } as const;
      await store.putGroup({ id: "finance", status: "active" });
      await store.putGroupMembership({ groupId: "finance", subject });
      await store.putSubjectBinding({
        subject,
        namespaceId: "team",
        accessRole: "member",
      });
      await store.putSubjectBinding({
        subject: { kind: "group", id: "finance" },
        namespaceId: "team",
        accessRole: "approver",
      });
      await store.putSubjectBinding({
        subject: { kind: "group", id: "finance" },
        namespaceId: "org",
        accessRole: "auditor",
      });
      const access = await store.resolveAccess({
        subject,
        namespaceId: "team",
      });
      expect(new Set(access.accessRoles)).toEqual(new Set(["member", "approver", "auditor"]));
      expect(new Set(access.groups)).toEqual(new Set(["finance"]));
      await store.putGroup({ id: "finance", status: "disabled" });
      expect(await store.resolveAccess({ subject, namespaceId: "team" })).toEqual({
        accessRoles: ["member"],
        groups: [],
      });
      await store.putGroup({ id: "finance", status: "active" });
      await store.removeGroupMembership({ groupId: "finance", subject });
      expect(await store.resolveAccess({ subject, namespaceId: "team" })).toEqual({
        accessRoles: ["member"],
        groups: [],
      });
    });

    it("round-trips and revokes role and group knowledge audiences", async () => {
      const store = await factory.create();
      await store.putGroup({ id: "finance", status: "active" });
      const role = {
        namespaceId: "team",
        baseId: "docs",
        audience: { kind: "access-role", id: "member" },
      } as const;
      const group = {
        namespaceId: "org",
        baseId: "docs",
        audience: { kind: "group", id: "finance" },
      } as const;
      await store.putKnowledgeAudienceGrant(role);
      await store.putKnowledgeAudienceGrant(group);
      const grants = await store.listKnowledgeAudienceGrants({
        namespaceId: "team",
        baseId: "docs",
      });
      expect(grants).toHaveLength(2);
      expect(grants).toEqual(expect.arrayContaining([role, group]));
      await store.removeKnowledgeAudienceGrant(group);
      expect(
        await store.listKnowledgeAudienceGrants({
          namespaceId: "team",
          baseId: "docs",
        }),
      ).toEqual([role]);
    });

    it("stores hash-only overlapping service-account credentials and supports revoke/disable", async () => {
      const store = await factory.create();
      await store.putServiceAccount({
        id: "slack",
        status: "active",
        canActFor: true,
      });
      await store.putServiceAccountCredential({
        id: "old",
        serviceAccountId: "slack",
        secretHash: "hash-old",
        createdAt: 1,
        expiresAt: 100,
      });
      await store.putServiceAccountCredential({
        id: "new",
        serviceAccountId: "slack",
        secretHash: "hash-new",
        createdAt: 2,
      });
      await store.revokeServiceAccountCredential({ id: "old", revokedAt: 3 });
      const credentials = await store.listServiceAccountCredentials("slack");
      expect(credentials).toHaveLength(2);
      expect(credentials).toEqual(
        expect.arrayContaining([
          {
            id: "old",
            serviceAccountId: "slack",
            secretHash: "hash-old",
            createdAt: 1,
            expiresAt: 100,
            revokedAt: 3,
          },
          {
            id: "new",
            serviceAccountId: "slack",
            secretHash: "hash-new",
            createdAt: 2,
          },
        ]),
      );
      await store.putServiceAccount({
        id: "slack",
        status: "disabled",
        canActFor: true,
      });
      expect(await store.getServiceAccount("slack")).toEqual({
        id: "slack",
        status: "disabled",
        canActFor: true,
      });
    });

    it("atomically creates a service account with exactly one initial credential", async () => {
      const store = await factory.create();
      const attempts = await Promise.all(
        [false, true].map((canActFor, index) =>
          store.createServiceAccountWithCredential({
            account: { id: "concurrent", status: "active", canActFor },
            credential: {
              id: `credential-${index}`,
              serviceAccountId: "concurrent",
              secretHash: `hash-${index}`,
              createdAt: index + 1,
            },
          }),
        ),
      );
      expect(attempts.filter(Boolean)).toHaveLength(1);
      const winner = attempts.findIndex(Boolean);
      expect(await store.getServiceAccount("concurrent")).toEqual({
        id: "concurrent",
        status: "active",
        canActFor: winner === 1,
      });
      expect(await store.listServiceAccountCredentials("concurrent")).toEqual([
        {
          id: `credential-${winner}`,
          serviceAccountId: "concurrent",
          secretHash: `hash-${winner}`,
          createdAt: winner + 1,
        },
      ]);
    });

    it("makes session hashes immutable across reassignment, revoke and expiry", async () => {
      const store = await factory.create();
      const user = await createUser(store, "session-user");
      await store.putServiceAccount({
        id: user.id,
        status: "active",
        canActFor: false,
      });
      const now = Date.now();
      const session = {
        tokenHash: "session-hash",
        principal: { kind: "user", id: user.id },
        createdAt: now,
        expiresAt: now + 10_000,
        csrfTokenHash: "csrf-hash",
        authMethod: "oidc",
      } as const;
      await store.putSession(session);
      await store.putSession(session);
      await expect(
        store.putSession({
          ...session,
          principal: { kind: "service-account", id: user.id },
          authMethod: "service-account",
        }),
      ).rejects.toThrow("Conflicting session token hash");
      await store.revokeSession({
        tokenHash: session.tokenHash,
        revokedAt: now + 1,
      });
      expect(await store.getSessionRecord(session.tokenHash)).toBeUndefined();
      await store.putSession({ ...session, revokedAt: now + 1 });
      await expect(store.putSession(session)).rejects.toThrow("Conflicting session token hash");

      const deleted = { ...session, tokenHash: "deleted-session-hash" };
      await store.putSession(deleted);
      await store.deleteSession(deleted.tokenHash);
      await store.deleteSession(deleted.tokenHash);
      expect(await store.getSessionRecord(deleted.tokenHash)).toBeUndefined();

      const expired = {
        ...session,
        tokenHash: "expired-session-hash",
        createdAt: now - 2_000,
        expiresAt: now - 1_000,
      };
      await store.putSession(expired);
      expect(await store.getSessionRecord(expired.tokenHash)).toBeUndefined();
      await expect(store.putSession({ ...expired, expiresAt: now + 1_000 })).rejects.toThrow(
        "Conflicting session token hash",
      );

      const disabledSession = {
        ...session,
        tokenHash: "disabled-session-hash",
      };
      await store.putSession(disabledSession);
      await store.setUserStatus({ id: user.id, status: "disabled" });
      await expect(
        store.createSession({
          tokenHash: "attempted-while-disabled",
          userId: user.id,
          ttlMs: 1_000,
        }),
      ).rejects.toThrow("Unknown or disabled user");
      await store.setUserStatus({ id: user.id, status: "active" });
      expect(await store.getSessionRecord(disabledSession.tokenHash)).toBeUndefined();
      await store.putSession(disabledSession);
      expect(await store.getSessionRecord(disabledSession.tokenHash)).toBeUndefined();
      expect(await store.getSession("attempted-while-disabled")).toBeUndefined();
    });

    it("atomically appends structured audit rows and rejects chain races/conflicts", async () => {
      const store = await factory.create();
      const first = auditRecord({ id: "event-1", sequence: 1, hash: "hash-1" });
      expect(
        await store.appendAuditWithOutbox({
          record: first,
          outbox: { nextAttemptAt: 1 },
        }),
      ).toBe("appended");
      expect(
        await store.appendAuditWithOutbox({
          record: first,
          outbox: { nextAttemptAt: 1 },
        }),
      ).toBe("already-exists");
      await expect(
        store.appendAuditWithOutbox({
          record: first,
          outbox: { nextAttemptAt: 2 },
        }),
      ).rejects.toThrow("Conflicting audit event");
      await expect(
        store.appendAuditWithOutbox({
          record: { ...first, event: "changed" },
          outbox: { nextAttemptAt: 1 },
        }),
      ).rejects.toThrow("Conflicting audit event");

      const contenders = [
        auditRecord({
          id: "event-2a",
          sequence: 2,
          previousHash: "hash-1",
          hash: "hash-2a",
        }),
        auditRecord({
          id: "event-2b",
          sequence: 2,
          previousHash: "hash-1",
          hash: "hash-2b",
        }),
      ];
      const outcomes = await Promise.allSettled(
        contenders.map((record) => store.appendAuditWithOutbox({ record })),
      );
      expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
      expect(outcomes.filter((outcome) => outcome.status === "rejected")).toHaveLength(1);
      const firstPage = await store.listAuditRecords({
        namespaceId: "team",
        limit: 1,
      });
      expect(firstPage.items).toEqual([first]);
      expect(firstPage.nextCursor).toBe(first.id);
      const secondPage = await store.listAuditRecords({
        namespaceId: "team",
        cursor: firstPage.nextCursor,
        limit: 1,
      });
      expect(secondPage.items).toHaveLength(1);
      expect(secondPage.nextCursor).toBeUndefined();
      expect(Object.isFrozen(firstPage)).toBe(true);
      expect(Object.isFrozen(firstPage.items)).toBe(true);
      expect(Object.isFrozen(firstPage.items[0]?.metadata)).toBe(true);
      expect(await store.getAuditRecord(first.id)).toEqual(first);
      expect((await store.listAuditOutbox()).items).toHaveLength(1);
    });

    it("leases outbox rows exclusively and rejects stale acknowledgements", async () => {
      const fixture = await factory.createClocked();
      const { store } = fixture;
      await store.appendAuditWithOutbox({
        record: auditRecord({ id: "event-1", sequence: 1, hash: "hash-1" }),
        outbox: { nextAttemptAt: fixture.currentTimeMs() },
      });
      const [one, two] = await Promise.all([
        store.claimAuditOutbox({ owner: "worker-1", leaseMs: 10, limit: 1 }),
        store.claimAuditOutbox({ owner: "worker-2", leaseMs: 10, limit: 1 }),
      ]);
      expect(one.length + two.length).toBe(1);
      const firstLease = (one[0] ?? two[0])!;
      expect(Object.isFrozen(one.length > 0 ? one : two)).toBe(true);
      expect(Object.isFrozen(firstLease)).toBe(true);
      expect(await store.getAuditRecord(firstLease.eventId)).toMatchObject({
        event: "authorization",
      });
      await fixture.advance(10);
      expect(
        await store.ackAuditOutbox({
          eventId: firstLease.eventId,
          leaseToken: firstLease.leaseToken!,
        }),
      ).toBe(false);
      expect(
        await store.releaseAuditOutbox({
          eventId: firstLease.eventId,
          leaseToken: firstLease.leaseToken!,
          nextAttemptAt: 20,
        }),
      ).toBe(false);
      const reclaimed = await store.claimAuditOutbox({
        owner: "worker-3",
        leaseMs: 10,
        limit: 1,
      });
      expect(reclaimed).toHaveLength(1);
      expect(reclaimed[0]?.leaseToken).not.toBe(firstLease.leaseToken);
      const current = reclaimed[0]!;
      expect(
        await store.ackAuditOutbox({
          eventId: current.eventId,
          leaseToken: current.leaseToken!,
        }),
      ).toBe(true);
      expect(
        await store.ackAuditOutbox({
          eventId: current.eventId,
          leaseToken: current.leaseToken!,
        }),
      ).toBe(true);

      await store.appendAuditWithOutbox({
        record: auditRecord({
          id: "event-2",
          sequence: 2,
          previousHash: "hash-1",
          hash: "hash-2",
        }),
        outbox: { nextAttemptAt: fixture.currentTimeMs() },
      });
      const releaseLease = (
        await store.claimAuditOutbox({
          owner: "worker-4",
          leaseMs: 10,
          limit: 1,
        })
      )[0]!;
      const retryAt = fixture.currentTimeMs() + 30;
      expect(
        await store.releaseAuditOutbox({
          eventId: releaseLease.eventId,
          leaseToken: releaseLease.leaseToken!,
          nextAttemptAt: retryAt,
          lastError: "temporary",
        }),
      ).toBe(true);
      expect(await store.claimAuditOutbox({ owner: "early", leaseMs: 10, limit: 1 })).toEqual([]);
      await fixture.advance(29);
      expect(await store.claimAuditOutbox({ owner: "early", leaseMs: 10, limit: 1 })).toEqual([]);
      await fixture.advance(1);
      expect(await store.claimAuditOutbox({ owner: "due", leaseMs: 10, limit: 1 })).toHaveLength(1);

      const outboxPage = await store.listAuditOutbox({ limit: 1 });
      expect(outboxPage.items).toHaveLength(1);
      expect(outboxPage.nextCursor).toBeDefined();
      const outboxPage2 = await store.listAuditOutbox({
        cursor: outboxPage.nextCursor,
        limit: 1,
      });
      expect(outboxPage2.items).toHaveLength(1);
      expect(outboxPage2.nextCursor).toBeUndefined();
    });

    it("increments and rolls denial windows atomically", async () => {
      const store = await factory.create();
      const results = await Promise.all(
        Array.from({ length: 10 }, () =>
          store.incrementDenialBucket({
            key: "actor:action",
            now: 1,
            windowMs: 100,
            emitLimit: 3,
          }),
        ),
      );
      expect(new Set(results.map((result) => result.bucket.count))).toEqual(
        new Set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
      );
      expect(results.filter((result) => result.shouldEmit)).toHaveLength(3);
      expect(await store.getDenialBucket("actor:action")).toEqual({
        key: "actor:action",
        windowStartedAt: 1,
        windowEndsAt: 101,
        count: 10,
        suppressed: 7,
      });
      expect(
        await store.incrementDenialBucket({
          key: "actor:action",
          now: 101,
          windowMs: 100,
          emitLimit: 3,
        }),
      ).toEqual({
        bucket: {
          key: "actor:action",
          windowStartedAt: 101,
          windowEndsAt: 201,
          count: 1,
          suppressed: 0,
        },
        shouldEmit: true,
      });
    });

    it("mints and revokes break-glass access as one idempotent transaction", async () => {
      const store = await factory.create();
      const user = await createUser(store, "break-glass-user");
      const principal = { kind: "user", id: user.id } as const;
      const grant: BreakGlassGrant = {
        id: "grant-1",
        principal,
        tokenHash: "break-glass-hash",
        reason: "r".repeat(SECURITY_STORE_LIMITS.reason.maxCharacters),
        createdBy: "operator",
        createdAt: 10,
        expiresAt: Date.now() + 10_000,
      };
      const session: SecuritySession = {
        tokenHash: grant.tokenHash,
        principal,
        createdAt: grant.createdAt,
        expiresAt: grant.expiresAt,
        authMethod: "break-glass",
      };
      const createdAudit = breakGlassAudit({
        id: "bg-created",
        sequence: 1,
        hash: "bg-hash-1",
        event: "break-glass.created",
        grant,
      });
      await expect(
        store.createBreakGlassAccess({
          grant,
          session: { ...session, tokenHash: "wrong" },
          audit: createdAudit,
          outbox: { nextAttemptAt: 1 },
        }),
      ).rejects.toThrow("token must match");
      expect(await store.getBreakGlassGrant(grant.id)).toBeUndefined();
      expect((await store.listAuditRecords({ namespaceId: "org" })).items).toEqual([]);

      const outcomes = await Promise.all([
        store.createBreakGlassAccess({
          grant,
          session,
          audit: createdAudit,
          outbox: { nextAttemptAt: 1 },
        }),
        store.createBreakGlassAccess({
          grant,
          session,
          audit: createdAudit,
          outbox: { nextAttemptAt: 1 },
        }),
      ]);
      expect(new Set(outcomes)).toEqual(new Set(["created", "already-exists"]));
      const storedGrant = await store.getBreakGlassGrant(grant.id);
      expect(storedGrant).toEqual(grant);
      expect(Object.isFrozen(storedGrant)).toBe(true);
      expect(Object.isFrozen(storedGrant?.principal)).toBe(true);
      const storedSession = await store.getSessionRecord(session.tokenHash);
      expect(storedSession).toMatchObject({
        authMethod: "break-glass",
      });
      expect(Object.isFrozen(storedSession)).toBe(true);
      expect((await store.listAuditRecords({ namespaceId: "org" })).items).toHaveLength(1);
      expect((await store.listAuditOutbox()).items).toHaveLength(1);
      await expect(store.putSession(session)).rejects.toThrow("transactionally");

      const duplicateTokenGrant = { ...grant, id: "grant-2" };
      await expect(
        store.createBreakGlassAccess({
          grant: duplicateTokenGrant,
          session,
          audit: breakGlassAudit({
            id: "bg-duplicate-token",
            sequence: 2,
            previousHash: "bg-hash-1",
            hash: "bg-duplicate-hash",
            event: "break-glass.created",
            grant: duplicateTokenGrant,
          }),
        }),
      ).rejects.toThrow("already used");

      const revokedAt = 20;
      const revokedAudit = breakGlassAudit({
        id: "bg-revoked",
        sequence: 2,
        previousHash: "bg-hash-1",
        hash: "bg-hash-2",
        event: "break-glass.revoked",
        grant,
      });
      expect(
        await store.revokeBreakGlassAccess({
          grantId: grant.id,
          revokedAt,
          audit: revokedAudit,
          outbox: { nextAttemptAt: 2 },
        }),
      ).toBe("revoked");
      expect(
        await store.revokeBreakGlassAccess({
          grantId: grant.id,
          revokedAt,
          audit: revokedAudit,
          outbox: { nextAttemptAt: 2 },
        }),
      ).toBe("already-revoked");
      expect(await store.getBreakGlassGrant(grant.id)).toMatchObject({
        revokedAt,
      });
      expect(await store.getSessionRecord(session.tokenHash)).toBeUndefined();
      await expect(
        store.createBreakGlassAccess({
          grant,
          session,
          audit: createdAudit,
          outbox: { nextAttemptAt: 1 },
        }),
      ).rejects.toThrow("Conflicting or incomplete");
    });

    it("serializes high-contention denial increments and outbox claims", async () => {
      const fixture = await factory.createClocked();
      const { store } = fixture;
      const increments = await Promise.all(
        Array.from({ length: 5_000 }, () =>
          store.incrementDenialBucket({
            key: "stress",
            now: 1,
            windowMs: 1_000,
            emitLimit: 17,
          }),
        ),
      );
      expect(new Set(increments.map((result) => result.bucket.count)).size).toBe(5_000);
      expect(increments.filter((result) => result.shouldEmit)).toHaveLength(17);
      expect(await store.getDenialBucket("stress")).toMatchObject({
        count: 5_000,
        suppressed: 4_983,
      });

      await store.appendAuditWithOutbox({
        record: auditRecord({
          id: "stress-event",
          sequence: 1,
          hash: "stress-hash",
        }),
        outbox: { nextAttemptAt: fixture.currentTimeMs() },
      });
      const claims = await Promise.all(
        Array.from({ length: 100 }, (_, index) =>
          store.claimAuditOutbox({
            owner: `worker-${index}`,
            leaseMs: 1_000,
            limit: 1,
          }),
        ),
      );
      expect(claims.flat()).toHaveLength(1);
      expect(new Set(claims.flat().map((row) => row.eventId))).toEqual(new Set(["stress-event"]));
    });

    it("snapshots hostile fields once and returns detached immutable records", async () => {
      const store = await factory.create();
      const user = await createUser(store, "hostile-user");
      let reads = 0;
      const subject = {
        get kind() {
          reads += 1;
          return "user" as const;
        },
        get id() {
          reads += 1;
          return user.id;
        },
      };
      await store.putSubjectBinding({
        subject,
        namespaceId: "team",
        accessRole: "member",
      });
      expect(reads).toBe(2);
      await expect(
        store.putSubjectBinding({
          subject: { kind: "user", id: "__proto__\u0000" },
          namespaceId: "team",
          accessRole: "member",
        }),
      ).rejects.toThrow("safe string");
      const result = await store.incrementDenialBucket({
        key: "immutable",
        now: 1,
        windowMs: 10,
        emitLimit: 1,
      });
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.bucket)).toBe(true);

      let eventReads = 0;
      let metadataReads = 0;
      const baseAudit = auditRecord({
        id: "hostile-audit",
        sequence: 1,
        hash: "hostile-hash",
      });
      const hostileAudit = {
        ...baseAudit,
        get event() {
          eventReads += 1;
          return baseAudit.event;
        },
        metadata: {
          get method() {
            metadataReads += 1;
            return "POST";
          },
        },
      };
      await store.appendAuditWithOutbox({ record: hostileAudit });
      expect(eventReads).toBe(1);
      expect(metadataReads).toBe(1);
      const storedAudit = (await store.listAuditRecords({ namespaceId: "team" })).items[0];
      expect(Object.isFrozen(storedAudit)).toBe(true);
      expect(Object.isFrozen(storedAudit?.metadata)).toBe(true);
    });

    it("enforces shared character, UTF-8 byte and metadata limits atomically", async () => {
      const store = await factory.create();
      const idAtLimit = "i".repeat(SECURITY_STORE_LIMITS.identifier.maxCharacters);
      await store.putGroup({ id: idAtLimit, status: "active" });
      expect(await store.getGroup(idAtLimit)).toBeDefined();
      await expect(store.putGroup({ id: `${idAtLimit}i`, status: "active" })).rejects.toThrow(
        "character limit",
      );
      const idAtByteLimit = "😀".repeat(SECURITY_STORE_LIMITS.identifier.maxBytes / 4);
      await store.putGroup({ id: idAtByteLimit, status: "active" });
      await expect(store.putGroup({ id: `${idAtByteLimit}😀`, status: "active" })).rejects.toThrow(
        "byte limit",
      );
      await expect(store.putGroup({ id: "\ud800", status: "active" })).rejects.toThrow(
        "safe string",
      );
      await store.putGroup({ id: "\ufffd", status: "active" });
      expect(await store.getGroup("\ufffd")).toBeDefined();

      const user = await createUser(store, "limit-user");
      const emailAtLimit = `${"e".repeat(SECURITY_STORE_LIMITS.email.maxCharacters - 2)}@x`;
      await store.findOrCreateProviderUser({
        identity: {
          providerId: "test",
          issuer: "email-limit",
          subject: "email-limit",
        },
        email: emailAtLimit,
      });
      const rejectedEmailIdentity = {
        providerId: "test",
        issuer: "email-limit",
        subject: "email-overflow",
      };
      await expect(
        store.findOrCreateProviderUser({
          identity: rejectedEmailIdentity,
          email: `e${emailAtLimit}`,
        }),
      ).rejects.toThrow("character limit");
      expect(await store.getProviderIdentity(rejectedEmailIdentity)).toBeUndefined();

      const issuerAtLimit = "u".repeat(SECURITY_STORE_LIMITS.issuer.maxCharacters);
      await store.findOrCreateProviderUser({
        identity: {
          providerId: "test",
          issuer: issuerAtLimit,
          subject: "issuer-limit",
        },
        email: "issuer-limit@example.com",
      });
      await expect(
        store.findOrCreateProviderUser({
          identity: {
            providerId: "test",
            issuer: `${issuerAtLimit}u`,
            subject: "issuer-overflow",
          },
          email: "issuer-overflow@example.com",
        }),
      ).rejects.toThrow("character limit");

      const namespaceAtLimit = "n".repeat(SECURITY_STORE_LIMITS.namespace.maxCharacters);
      await store.putSubjectBinding({
        subject: { kind: "user", id: user.id },
        namespaceId: namespaceAtLimit,
        accessRole: "member",
      });
      await expect(
        store.putSubjectBinding({
          subject: { kind: "user", id: user.id },
          namespaceId: `${namespaceAtLimit}n`,
          accessRole: "member",
        }),
      ).rejects.toThrow("character limit");

      await store.putServiceAccount({
        id: "limit-sa",
        status: "active",
        canActFor: false,
      });
      const hashAtLimit = "h".repeat(SECURITY_STORE_LIMITS.hash.maxCharacters);
      await store.putServiceAccountCredential({
        id: "limit-credential",
        serviceAccountId: "limit-sa",
        secretHash: hashAtLimit,
        createdAt: 1,
      });
      await expect(
        store.putServiceAccountCredential({
          id: "too-large-credential",
          serviceAccountId: "limit-sa",
          secretHash: `${hashAtLimit}h`,
          createdAt: 1,
        }),
      ).rejects.toThrow("character limit");

      const metadataKeyAtLimit = "k".repeat(SECURITY_STORE_LIMITS.metadataKey.maxCharacters);
      const metadataStringAtLimit = "m".repeat(SECURITY_STORE_LIMITS.metadataString.maxCharacters);
      const metadata = Object.fromEntries([
        [metadataKeyAtLimit, metadataStringAtLimit],
        ...Array.from({ length: SECURITY_STORE_LIMITS.metadataEntries - 1 }, (_, index) => [
          `k${index}`,
          index,
        ]),
      ]);
      const eventAtLimit = "a".repeat(SECURITY_STORE_LIMITS.event.maxCharacters);
      await store.appendAuditWithOutbox({
        record: {
          ...auditRecord({
            id: "bounded-audit",
            sequence: 1,
            hash: "bounded-hash",
            metadata,
          }),
          event: eventAtLimit,
        },
        outbox: { nextAttemptAt: 1 },
      });
      await expect(
        store.appendAuditWithOutbox({
          record: {
            ...auditRecord({
              id: "oversized-event",
              sequence: 2,
              previousHash: "bounded-hash",
              hash: "oversized-event-hash",
            }),
            event: `${eventAtLimit}a`,
          },
        }),
      ).rejects.toThrow("character limit");
      await expect(
        store.appendAuditWithOutbox({
          record: auditRecord({
            id: "oversized-audit",
            sequence: 2,
            previousHash: "bounded-hash",
            hash: "oversized-hash",
            metadata: { ...metadata, overflow: true },
          }),
          outbox: { nextAttemptAt: 1 },
        }),
      ).rejects.toThrow("too many entries");
      expect((await store.listAuditRecords({ namespaceId: "team" })).items).toHaveLength(1);
      expect((await store.listAuditOutbox()).items).toHaveLength(1);

      const grant: BreakGlassGrant = {
        id: "oversized-grant",
        principal: { kind: "user", id: user.id },
        tokenHash: "oversized-grant-hash",
        reason: "r".repeat(SECURITY_STORE_LIMITS.reason.maxCharacters + 1),
        createdBy: "operator",
        createdAt: 1,
        expiresAt: 100,
      };
      await expect(
        store.createBreakGlassAccess({
          grant,
          session: {
            tokenHash: grant.tokenHash,
            principal: grant.principal,
            createdAt: 1,
            expiresAt: 100,
            authMethod: "break-glass",
          },
          audit: breakGlassAudit({
            id: "oversized-grant-audit",
            sequence: 1,
            hash: "oversized-grant-audit-hash",
            event: "break-glass.created",
            grant,
          }),
        }),
      ).rejects.toThrow("character limit");
      expect(await store.getBreakGlassGrant(grant.id)).toBeUndefined();
      expect(await store.getSessionRecord(grant.tokenHash)).toBeUndefined();
      expect((await store.listAuditRecords({ namespaceId: "org" })).items).toEqual([]);
    });

    it("rejects fractional milliseconds before durable mutation", async () => {
      const store = await factory.create();
      const user = await createUser(store, "integer-milliseconds");

      await expect(
        store.putSession({
          tokenHash: "fractional-session",
          principal: { kind: "user", id: user.id },
          createdAt: 1.5,
          expiresAt: 2_000,
          authMethod: "oidc",
        }),
      ).rejects.toThrow("safe integer");
      expect(await store.getSessionRecord("fractional-session")).toBeUndefined();

      await expect(
        store.appendAuditWithOutbox({
          record: auditRecord({
            id: "fractional-outbox",
            sequence: 1,
            hash: "hash",
          }),
          outbox: { nextAttemptAt: 1.5 },
        }),
      ).rejects.toThrow("safe integer");
      expect(await store.getAuditRecord("fractional-outbox")).toBeUndefined();

      await expect(
        store.incrementDenialBucket({
          key: "fractional-denial",
          now: 1.5,
          windowMs: 100,
          emitLimit: 1,
        }),
      ).rejects.toThrow("safe integer");
      expect(await store.getDenialBucket("fractional-denial")).toBeUndefined();
    });
  });
}

async function createUser(store: SecurityStoreV2, subject: string) {
  return (
    await store.findOrCreateProviderUser({
      identity: { providerId: "test", issuer: "https://id.example", subject },
      email: `${subject}@example.com`,
    })
  ).user;
}

function auditRecord(params: {
  readonly id: string;
  readonly sequence: number;
  readonly hash: string;
  readonly previousHash?: string;
  readonly metadata?: Readonly<Record<string, string | number | boolean | null>>;
}): SecurityAuditRecord {
  return {
    id: params.id,
    namespaceId: "team",
    sequence: params.sequence,
    occurredAt: params.sequence,
    recordedAt: params.sequence,
    event: "authorization",
    actor: { kind: "operator", id: "operator" },
    effective: { kind: "user", id: "alice" },
    permission: { resource: "agent", action: "invoke" },
    resource: { kind: "agent", namespaceId: "team", id: "helper" },
    decision: { allowed: true, reason: "operator_allow" },
    correlation: { requestId: `request-${params.id}` },
    metadata: params.metadata ?? { method: "POST" },
    ...(params.previousHash !== undefined && {
      previousHash: params.previousHash,
    }),
    hash: params.hash,
    keyId: "audit-key",
  };
}

function breakGlassAudit(params: {
  readonly id: string;
  readonly sequence: number;
  readonly hash: string;
  readonly previousHash?: string;
  readonly event: "break-glass.created" | "break-glass.revoked";
  readonly grant: BreakGlassGrant;
}): SecurityAuditRecord {
  return {
    id: params.id,
    namespaceId: "org",
    sequence: params.sequence,
    occurredAt: params.sequence,
    recordedAt: params.sequence,
    event: params.event,
    actor: { kind: "operator", id: params.grant.createdBy },
    effective: params.grant.principal,
    decision: { allowed: true, reason: "operator_allow" },
    metadata: {
      breakGlassGrantId: params.grant.id,
      ...(params.event === "break-glass.revoked" && { revokedBy: params.grant.createdBy }),
    },
    ...(params.previousHash !== undefined && {
      previousHash: params.previousHash,
    }),
    hash: params.hash,
    keyId: "audit-key",
  };
}
