import { describe, expect, it } from "bun:test";
import type { SecurityStoreV2 } from "../security-store.ts";

export interface SecurityStoreV2TestFactory {
  readonly create: () => SecurityStoreV2 | Promise<SecurityStoreV2>;
}

/** Reusable contract suite for the in-memory and future PostgreSQL stores. */
export function securityStoreV2TestSuite(name: string, factory: SecurityStoreV2TestFactory): void {
  describe(name, () => {
    it("keys OIDC identity by provider, issuer and subject instead of email", async () => {
      const store = await factory.create();
      const identity = { providerId: "oidc", issuer: "https://id.example", subject: "sub-1" };
      const [first, concurrent] = await Promise.all([
        store.findOrCreateProviderUser({ identity, email: "first@example.com" }),
        store.findOrCreateProviderUser({ identity, email: "first@example.com" }),
      ]);
      expect(first.user.id).toBe(concurrent.user.id);
      expect([first.created, concurrent.created].filter(Boolean)).toHaveLength(1);

      const renamed = await store.findOrCreateProviderUser({
        identity,
        email: "renamed@example.com",
      });
      expect(renamed.user).toMatchObject({ id: first.user.id, email: "renamed@example.com" });
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

    it("keeps same-id user and service-account bindings collision safe", async () => {
      const store = await factory.create();
      await store.putSubjectBinding({
        subject: { kind: "user", id: "same" },
        namespaceId: "team",
        accessRole: "member",
      });
      await store.putSubjectBinding({
        subject: { kind: "service-account", id: "same" },
        namespaceId: "team",
        accessRole: "agent-manager",
      });

      expect(
        await store.resolveAccess({ subject: { kind: "user", id: "same" }, namespaceId: "team" }),
      ).toEqual({
        accessRoles: ["member"],
        groups: [],
      });
      expect(
        await store.resolveAccess({
          subject: { kind: "service-account", id: "same" },
          namespaceId: "team",
        }),
      ).toEqual({ accessRoles: ["agent-manager"], groups: [] });
    });

    it("unions direct, group and org roles and revokes membership immediately", async () => {
      const store = await factory.create();
      const subject = { kind: "user", id: "alice" } as const;
      await store.putGroup({ id: "finance", status: "active" });
      await store.putGroupMembership({ groupId: "finance", subject });
      await store.putSubjectBinding({ subject, namespaceId: "team", accessRole: "member" });
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

      const access = await store.resolveAccess({ subject, namespaceId: "team" });
      expect(new Set(access.accessRoles)).toEqual(new Set(["member", "approver", "auditor"]));
      expect(new Set(access.groups)).toEqual(new Set(["finance"]));
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
        await store.listKnowledgeAudienceGrants({ namespaceId: "team", baseId: "docs" }),
      ).toEqual([role]);
    });

    it("stores hash-only overlapping service-account credentials and supports revoke/disable", async () => {
      const store = await factory.create();
      await store.putServiceAccount({ id: "slack", status: "active", canActFor: true });
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
          { id: "new", serviceAccountId: "slack", secretHash: "hash-new", createdAt: 2 },
        ]),
      );
      await store.putServiceAccount({ id: "slack", status: "disabled", canActFor: true });
      expect(await store.getServiceAccount("slack")).toEqual({
        id: "slack",
        status: "disabled",
        canActFor: true,
      });
    });

    it("preserves session CSRF/auth metadata and enforces revocation and expiry", async () => {
      const store = await factory.create();
      const now = Date.now();
      await store.putSession({
        tokenHash: "session-hash",
        principal: { kind: "user", id: "alice" },
        createdAt: now,
        expiresAt: now + 10_000,
        csrfTokenHash: "csrf-hash",
        authMethod: "oidc",
      });
      expect(await store.getSessionRecord("session-hash")).toMatchObject({
        principal: { kind: "user", id: "alice" },
        csrfTokenHash: "csrf-hash",
        authMethod: "oidc",
      });
      await store.revokeSession({ tokenHash: "session-hash", revokedAt: now + 1 });
      expect(await store.getSessionRecord("session-hash")).toBeUndefined();
      await store.putSession({
        tokenHash: "expired-session-hash",
        principal: { kind: "user", id: "alice" },
        createdAt: now - 2_000,
        expiresAt: now - 1_000,
        authMethod: "oidc",
      });
      expect(await store.getSessionRecord("expired-session-hash")).toBeUndefined();
    });

    it("appends an audit row and outbox atomically and preserves chain order", async () => {
      const store = await factory.create();
      const first = {
        id: "event-1",
        namespaceId: "team",
        sequence: 1,
        occurredAt: 1,
        eventJson: "{}",
        hash: "hash-1",
        keyId: "key-1",
      } as const;
      await store.appendAuditRecord(first, {
        eventId: first.id,
        status: "pending",
        attempts: 0,
        nextAttemptAt: 1,
      });
      await store.appendAuditRecord({
        ...first,
        id: "event-2",
        sequence: 2,
        previousHash: "hash-1",
        hash: "hash-2",
      });
      expect(await store.listAuditRecords("team")).toHaveLength(2);
      expect(await store.getAuditOutbox("event-1")).toMatchObject({
        eventId: "event-1",
        status: "pending",
      });
      await expect(
        store.appendAuditRecord({ ...first, id: "bad", sequence: 4, previousHash: "wrong" }),
      ).rejects.toThrow("Audit chain boundary mismatch");
      expect(await store.listAuditRecords("team")).toHaveLength(2);
    });

    it("round-trips denial and break-glass records with detached immutable reads", async () => {
      const store = await factory.create();
      const bucket = { key: "actor:action", windowStartedAt: 1, count: 2, suppressed: 1 };
      await store.putDenialBucket(bucket);
      bucket.count = 99;
      expect(await store.getDenialBucket("actor:action")).toEqual({
        key: "actor:action",
        windowStartedAt: 1,
        count: 2,
        suppressed: 1,
      });
      const grant = {
        id: "grant-1",
        tokenHash: "hash-only",
        reason: "incident-42",
        createdBy: "host:alice",
        createdAt: 1,
        expiresAt: 100,
      };
      await store.putBreakGlassGrant(grant);
      await store.revokeBreakGlassGrant({ id: grant.id, revokedAt: 2 });
      const stored = await store.getBreakGlassGrant(grant.id);
      expect(stored).toEqual({ ...grant, revokedAt: 2 });
      expect(Object.isFrozen(stored)).toBe(true);
    });

    it("snapshots hostile fields once and rejects unsafe identifiers", async () => {
      const store = await factory.create();
      let reads = 0;
      const subject = {
        get kind() {
          reads += 1;
          return "user" as const;
        },
        get id() {
          reads += 1;
          return "alice";
        },
      };
      await store.putSubjectBinding({ subject, namespaceId: "team", accessRole: "member" });
      expect(reads).toBe(2);
      await expect(
        store.putSubjectBinding({
          subject: { kind: "user", id: "__proto__\u0000" },
          namespaceId: "team",
          accessRole: "member",
        }),
      ).rejects.toThrow("safe string");
    });
  });
}
