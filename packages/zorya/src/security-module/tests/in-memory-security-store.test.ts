import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { AccessBinding, KnowledgeGrant } from "../security-store.ts";

const INITIAL_BINDINGS = [{ namespaceId: "acme", accessRole: "member" }] as const;

describe("InMemorySecurityStore", () => {
  it("findOrCreateUser creates once and returns the same user after", async () => {
    const store = new InMemorySecurityStore();
    const first = await store.findOrCreateUser({
      email: "user@example.com",
      bindings: INITIAL_BINDINGS,
    });
    const second = await store.findOrCreateUser({
      email: "user@example.com",
      bindings: [{ namespaceId: "other", accessRole: "admin" }],
    });

    expect(first.created).toBe(true);
    expect(second).toEqual({ user: first.user, created: false });
    expect(await store.listBindings(first.user.id)).toEqual([
      { userId: first.user.id, ...INITIAL_BINDINGS[0] },
    ]);
  });

  it("findOrCreateUser yields one user for ten parallel calls", async () => {
    const store = new InMemorySecurityStore();
    const results = await Promise.all(
      Array.from({ length: 10 }, () =>
        store.findOrCreateUser({ email: "parallel@example.com", bindings: INITIAL_BINDINGS }),
      ),
    );

    expect(new Set(results.map((result) => result.user.id)).size).toBe(1);
    expect(results.filter((result) => result.created)).toHaveLength(1);
    expect(await store.listUsers()).toHaveLength(1);
  });

  it("emails are matched case-insensitively", async () => {
    const store = new InMemorySecurityStore();
    const first = await store.findOrCreateUser({
      email: "User@Example.COM",
      bindings: INITIAL_BINDINGS,
    });
    const second = await store.findOrCreateUser({
      email: "user@example.com",
      bindings: INITIAL_BINDINGS,
    });

    expect(first.user.email).toBe("user@example.com");
    expect(second.user.id).toBe(first.user.id);
    expect(second.created).toBe(false);
  });

  it("bindings: put is idempotent; remove removes exactly one", async () => {
    const store = new InMemorySecurityStore();
    const user = (await store.findOrCreateUser({ email: "user@example.com", bindings: [] })).user;
    const member: AccessBinding = {
      userId: user.id,
      namespaceId: "acme",
      accessRole: "member",
    };
    const auditor: AccessBinding = { ...member, accessRole: "auditor" };

    await store.putBinding(member);
    await store.putBinding(member);
    await store.putBinding(auditor);
    expect(await store.listBindings(user.id)).toEqual([member, auditor]);

    await store.removeBinding(member);
    expect(await store.listBindings(user.id)).toEqual([auditor]);
  });

  it("listKnowledgeGrants returns namespace and org-wide grants, not other namespaces", async () => {
    const store = new InMemorySecurityStore();
    const namespaceGrant: KnowledgeGrant = {
      namespaceId: "acme",
      baseId: "handbook",
      accessRole: "member",
    };
    const orgGrant: KnowledgeGrant = {
      namespaceId: "org",
      baseId: "handbook",
      accessRole: "auditor",
    };
    await store.putKnowledgeGrant(namespaceGrant);
    await store.putKnowledgeGrant(orgGrant);
    await store.putKnowledgeGrant({
      namespaceId: "other",
      baseId: "handbook",
      accessRole: "admin",
    });
    await store.putKnowledgeGrant({
      namespaceId: "acme",
      baseId: "runbooks",
      accessRole: "member",
    });

    expect(await store.listKnowledgeGrants({ namespaceId: "acme", baseId: "handbook" })).toEqual([
      namespaceGrant,
      orgGrant,
    ]);
  });

  it("a session is found until its ttl passes, then not", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemorySecurityStore({ clock });
    const user = (
      await store.findOrCreateUser({ email: "session@example.com", bindings: INITIAL_BINDINGS })
    ).user;
    await store.createSession({ tokenHash: "hash", userId: user.id, ttlMs: 500 });

    expect(await store.getSession("hash")).toEqual({ userId: user.id });
    clock.advance(501);
    expect(await store.getSession("hash")).toBeUndefined();
  });

  it("disabling a user deletes their sessions", async () => {
    const store = new InMemorySecurityStore();
    const user = (
      await store.findOrCreateUser({ email: "user@example.com", bindings: INITIAL_BINDINGS })
    ).user;
    const other = (
      await store.findOrCreateUser({ email: "other@example.com", bindings: INITIAL_BINDINGS })
    ).user;
    await store.createSession({ tokenHash: "one", userId: user.id, ttlMs: 10_000 });
    await store.createSession({ tokenHash: "two", userId: other.id, ttlMs: 10_000 });

    await store.setUserStatus({ id: user.id, status: "disabled" });

    expect(await store.getSession("one")).toBeUndefined();
    expect(await store.getSession("two")).toEqual({ userId: other.id });
    expect(await store.getUser(user.id)).toEqual({ ...user, status: "disabled" });
  });

  it("legacy session adapters reject missing and disabled users without delayed revival", async () => {
    const store = new InMemorySecurityStore();
    await expect(
      store.createSession({ tokenHash: "missing", userId: "missing", ttlMs: 10_000 }),
    ).rejects.toThrow("Unknown or disabled user");
    expect(await store.getSession("missing")).toBeUndefined();

    const user = (
      await store.findOrCreateUser({ email: "disabled@example.com", bindings: INITIAL_BINDINGS })
    ).user;
    await store.setUserStatus({ id: user.id, status: "disabled" });
    await expect(
      store.createSession({ tokenHash: "disabled", userId: user.id, ttlMs: 10_000 }),
    ).rejects.toThrow("Unknown or disabled user");
    await store.setUserStatus({ id: user.id, status: "active" });
    expect(await store.getSession("disabled")).toBeUndefined();
  });
});
