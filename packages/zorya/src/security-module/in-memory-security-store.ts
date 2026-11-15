import { SystemWallClock, type WallClock } from "@promin/workflow";
import { ORG_WIDE_NAMESPACE } from "../server/security/index.ts";
import type {
  AccessBinding,
  KnowledgeGrant,
  SecurityStore,
  SecurityUser,
} from "./security-store.ts";

interface StoredSession {
  readonly userId: string;
  readonly expiresAt: number;
}

export interface InMemorySecurityStoreConfig {
  readonly clock?: WallClock;
}

/** Single-process security storage for tests and development hosts. */
export class InMemorySecurityStore implements SecurityStore {
  private readonly clock: WallClock;
  private readonly usersById = new Map<string, SecurityUser>();
  private readonly userIdByEmail = new Map<string, string>();
  private readonly bindings = new Map<string, AccessBinding>();
  private readonly knowledgeGrants = new Map<string, KnowledgeGrant>();
  private readonly sessions = new Map<string, StoredSession>();

  constructor(config: InMemorySecurityStoreConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async findOrCreateUser(params: {
    readonly email: string;
    readonly bindings: ReadonlyArray<Omit<AccessBinding, "userId">>;
  }): Promise<{ user: SecurityUser; created: boolean }> {
    const email = normalizeEmail(params.email);
    const existingId = this.userIdByEmail.get(email);
    if (existingId !== undefined) {
      const existing = this.usersById.get(existingId);
      if (existing !== undefined) return { user: existing, created: false };
    }

    // No await occurs before both indexes and the initial bindings are stored,
    // so concurrent calls in this single-process implementation cannot create
    // two users for the same normalized email.
    const user: SecurityUser = {
      id: crypto.randomUUID(),
      email,
      status: "active",
    };
    this.usersById.set(user.id, user);
    this.userIdByEmail.set(email, user.id);
    for (const binding of params.bindings) {
      const row: AccessBinding = { userId: user.id, ...binding };
      this.bindings.set(bindingKey(row), row);
    }
    return { user, created: true };
  }

  async getUser(id: string): Promise<SecurityUser | undefined> {
    return this.usersById.get(id);
  }

  async listUsers(): Promise<SecurityUser[]> {
    return [...this.usersById.values()];
  }

  async setUserStatus(params: { id: string; status: SecurityUser["status"] }): Promise<void> {
    const existing = this.usersById.get(params.id);
    if (existing === undefined) return;
    this.usersById.set(params.id, { ...existing, status: params.status });
    if (params.status === "disabled") {
      for (const [tokenHash, session] of this.sessions) {
        if (session.userId === params.id) this.sessions.delete(tokenHash);
      }
    }
  }

  async listBindings(userId: string): Promise<AccessBinding[]> {
    return [...this.bindings.values()].filter((binding) => binding.userId === userId);
  }

  async putBinding(binding: AccessBinding): Promise<void> {
    this.bindings.set(bindingKey(binding), binding);
  }

  async removeBinding(binding: AccessBinding): Promise<void> {
    this.bindings.delete(bindingKey(binding));
  }

  async putKnowledgeGrant(grant: KnowledgeGrant): Promise<void> {
    this.knowledgeGrants.set(knowledgeGrantKey(grant), grant);
  }

  async removeKnowledgeGrant(grant: KnowledgeGrant): Promise<void> {
    this.knowledgeGrants.delete(knowledgeGrantKey(grant));
  }

  async listKnowledgeGrants(params: {
    namespaceId: string;
    baseId: string;
  }): Promise<KnowledgeGrant[]> {
    return [...this.knowledgeGrants.values()].filter(
      (grant) =>
        grant.baseId === params.baseId &&
        (grant.namespaceId === params.namespaceId || grant.namespaceId === ORG_WIDE_NAMESPACE),
    );
  }

  async createSession(params: { tokenHash: string; userId: string; ttlMs: number }): Promise<void> {
    this.sessions.set(params.tokenHash, {
      userId: params.userId,
      expiresAt: this.clock.currentTimeMs() + params.ttlMs,
    });
  }

  async getSession(tokenHash: string): Promise<{ userId: string } | undefined> {
    const session = this.sessions.get(tokenHash);
    if (session === undefined) return undefined;
    if (session.expiresAt <= this.clock.currentTimeMs()) {
      this.sessions.delete(tokenHash);
      return undefined;
    }
    return { userId: session.userId };
  }

  async deleteSession(tokenHash: string): Promise<void> {
    this.sessions.delete(tokenHash);
  }
}

function normalizeEmail(email: string): string {
  return email.toLowerCase();
}

function bindingKey(binding: AccessBinding): string {
  return JSON.stringify([binding.userId, binding.namespaceId, binding.accessRole]);
}

function knowledgeGrantKey(grant: KnowledgeGrant): string {
  return JSON.stringify([grant.namespaceId, grant.baseId, grant.accessRole]);
}
