import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  AUTHORIZATION_REASONS,
  ORG_WIDE_NAMESPACE,
  SECURITY_ACTIONS,
} from "../server/security/index.ts";
import { SECURITY_STORE_LIMITS } from "./security-store.ts";
import type {
  AccessGroup,
  AccessBinding,
  AccessSubjectRef,
  BreakGlassGrant,
  GroupMembership,
  KnowledgeAudienceGrant,
  KnowledgeGrant,
  ProviderIdentity,
  SecurityAuditOutboxRecord,
  SecurityAuditRecord,
  SecurityDenialBucket,
  SecurityPrincipalRef,
  SecurityServiceAccount,
  SecuritySession,
  SecurityStoreV2,
  SecurityStoreTextLimit,
  SecurityUser,
  ServiceAccountCredential,
  SubjectAccessBinding,
} from "./security-store.ts";

export interface InMemorySecurityStoreConfig {
  readonly clock?: WallClock;
}

/** Single-process security storage for tests and development hosts. */
export class InMemorySecurityStore implements SecurityStoreV2 {
  private readonly clock: WallClock;
  private readonly usersById = new Map<string, SecurityUser>();
  private readonly userIdByEmail = new Map<string, string>();
  private readonly providerIdentities = new Map<string, ProviderIdentity>();
  private readonly groups = new Map<string, AccessGroup>();
  private readonly memberships = new Map<string, GroupMembership>();
  private readonly bindings = new Map<string, SubjectAccessBinding>();
  private readonly knowledgeGrants = new Map<string, KnowledgeAudienceGrant>();
  private readonly serviceAccounts = new Map<string, SecurityServiceAccount>();
  private readonly serviceAccountCredentials = new Map<string, ServiceAccountCredential>();
  private readonly sessions = new Map<string, SecuritySession>();
  private readonly legacySessionTokenHashes = new Set<string>();
  private readonly invalidatedSessionTokenHashes = new Set<string>();
  private readonly auditRecords = new Map<string, SecurityAuditRecord[]>();
  private readonly auditIds = new Set<string>();
  private readonly auditIdsWithOutbox = new Set<string>();
  private readonly auditOutboxInitialNextAttemptAt = new Map<string, number>();
  private readonly auditOutbox = new Map<string, SecurityAuditOutboxRecord>();
  private readonly denialBuckets = new Map<string, SecurityDenialBucket>();
  private readonly breakGlassGrants = new Map<string, BreakGlassGrant>();
  private readonly breakGlassGrantIdByTokenHash = new Map<string, string>();

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
      if (existing !== undefined) return { user: clone(existing), created: false };
    }
    const initialBindings = normalizeInitialBindings(params.bindings);

    // No await occurs before both indexes and the initial bindings are stored,
    // so concurrent calls in this single-process implementation cannot create
    // two users for the same normalized email.
    const user = freeze<SecurityUser>({
      id: crypto.randomUUID(),
      email,
      status: "active",
    });
    this.usersById.set(user.id, user);
    this.userIdByEmail.set(email, user.id);
    for (const binding of initialBindings) {
      this.storeSubjectBinding({
        subject: { kind: "user", id: user.id },
        ...binding,
      });
    }
    return { user: clone(user), created: true };
  }

  async getUser(id: string): Promise<SecurityUser | undefined> {
    const row = this.usersById.get(requiredString(id, "user id"));
    return row && clone(row);
  }

  async listUsers(): Promise<SecurityUser[]> {
    return [...this.usersById.values()].map(clone);
  }

  async setUserStatus(params: { id: string; status: SecurityUser["status"] }): Promise<void> {
    const id = requiredString(params.id, "user id");
    const status = statusValue(params.status);
    const existing = this.usersById.get(id);
    if (existing === undefined) return;
    this.usersById.set(id, freeze({ ...existing, status }));
    if (status === "disabled") this.invalidateSessionsForPrincipal({ kind: "user", id });
  }

  async listBindings(userId: string): Promise<AccessBinding[]> {
    const rows = await this.listSubjectBindings({ kind: "user", id: userId });
    return rows.map((binding) =>
      freeze({
        userId: binding.subject.id,
        namespaceId: binding.namespaceId,
        accessRole: binding.accessRole,
      }),
    );
  }

  async putBinding(binding: AccessBinding): Promise<void> {
    await this.putSubjectBinding({
      subject: { kind: "user", id: binding.userId },
      namespaceId: binding.namespaceId,
      accessRole: binding.accessRole,
    });
  }

  async removeBinding(binding: AccessBinding): Promise<void> {
    await this.removeSubjectBinding({
      subject: { kind: "user", id: binding.userId },
      namespaceId: binding.namespaceId,
      accessRole: binding.accessRole,
    });
  }

  async putKnowledgeGrant(grant: KnowledgeGrant): Promise<void> {
    await this.putKnowledgeAudienceGrant({
      namespaceId: grant.namespaceId,
      baseId: grant.baseId,
      audience: { kind: "access-role", id: grant.accessRole },
    });
  }

  async removeKnowledgeGrant(grant: KnowledgeGrant): Promise<void> {
    await this.removeKnowledgeAudienceGrant({
      namespaceId: grant.namespaceId,
      baseId: grant.baseId,
      audience: { kind: "access-role", id: grant.accessRole },
    });
  }

  async listKnowledgeGrants(params: {
    namespaceId: string;
    baseId: string;
  }): Promise<KnowledgeGrant[]> {
    const grants = await this.listKnowledgeAudienceGrants(params);
    return grants
      .filter((grant) => grant.audience.kind === "access-role")
      .map((grant) =>
        freeze({
          namespaceId: grant.namespaceId,
          baseId: grant.baseId,
          accessRole: grant.audience.id,
        }),
      );
  }

  async createSession(params: { tokenHash: string; userId: string; ttlMs: number }): Promise<void> {
    const now = this.clock.currentTimeMs();
    const row = normalizeSession({
      tokenHash: params.tokenHash,
      principal: { kind: "user", id: params.userId },
      createdAt: now,
      expiresAt: now + positiveNumber(params.ttlMs, "session ttl"),
      authMethod: "oidc",
    });
    const existing = this.sessions.get(row.tokenHash);
    if (existing !== undefined && !recordsEqual(existing, row)) {
      throw new Error(`Conflicting session token hash: ${row.tokenHash}`);
    }
    this.sessions.set(row.tokenHash, row);
    this.legacySessionTokenHashes.add(row.tokenHash);
  }

  async getSession(tokenHash: string): Promise<{ userId: string } | undefined> {
    const key = requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    if (this.legacySessionTokenHashes.has(key)) {
      const legacy = this.sessions.get(key);
      if (
        legacy === undefined ||
        this.invalidatedSessionTokenHashes.has(key) ||
        legacy.revokedAt !== undefined ||
        legacy.expiresAt <= this.clock.currentTimeMs() ||
        (legacy.principal.kind === "user" &&
          this.usersById.get(legacy.principal.id)?.status === "disabled")
      ) {
        return undefined;
      }
      return freeze({ userId: legacy.principal.id });
    }
    const session = await this.getSessionRecord(tokenHash);
    if (session === undefined) return undefined;
    if (session.principal.kind !== "user") return undefined;
    return freeze({ userId: session.principal.id });
  }

  async deleteSession(tokenHash: string): Promise<void> {
    const key = requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    const existing = this.sessions.get(key);
    if (!existing) return;
    if (existing.authMethod === "break-glass") {
      throw new Error("Break-glass sessions must be revoked transactionally");
    }
    if (existing.revokedAt === undefined) {
      this.sessions.set(key, freeze({ ...existing, revokedAt: this.clock.currentTimeMs() }));
    }
  }

  async findOrCreateProviderUser(params: {
    readonly identity: Omit<ProviderIdentity, "userId">;
    readonly email: string;
    readonly bindings?: ReadonlyArray<Omit<SubjectAccessBinding, "subject">>;
  }): Promise<{ user: SecurityUser; identity: ProviderIdentity; created: boolean }> {
    const identity = normalizeProviderIdentity(params.identity);
    const key = providerIdentityKey(identity);
    const existingIdentity = this.providerIdentities.get(key);
    if (existingIdentity) {
      const existingUser = this.usersById.get(existingIdentity.userId);
      if (!existingUser) throw new Error("Provider identity refers to a missing user");
      const email = normalizeEmail(params.email);
      let user = existingUser;
      if (email !== existingUser.email) {
        user = freeze({ ...existingUser, email });
        this.usersById.set(user.id, user);
        if (this.userIdByEmail.get(existingUser.email) === user.id) {
          this.userIdByEmail.delete(existingUser.email);
        }
        if (!this.userIdByEmail.has(email)) this.userIdByEmail.set(email, user.id);
      }
      return { user: clone(user), identity: clone(existingIdentity), created: false };
    }

    const initialBindings = normalizeInitialBindings(params.bindings ?? []);
    const email = normalizeEmail(params.email);
    const user = freeze<SecurityUser>({ id: crypto.randomUUID(), email, status: "active" });
    const row = freeze<ProviderIdentity>({ ...identity, userId: user.id });
    this.usersById.set(user.id, user);
    if (!this.userIdByEmail.has(email)) this.userIdByEmail.set(email, user.id);
    this.providerIdentities.set(key, row);
    for (const binding of initialBindings) {
      this.storeSubjectBinding({
        subject: { kind: "user", id: user.id },
        ...binding,
      });
    }
    return { user: clone(user), identity: clone(row), created: true };
  }

  async getProviderIdentity(params: {
    readonly providerId: string;
    readonly issuer: string;
    readonly subject: string;
  }): Promise<ProviderIdentity | undefined> {
    const row = this.providerIdentities.get(providerIdentityKey(normalizeProviderIdentity(params)));
    return row && clone(row);
  }

  async putGroup(group: AccessGroup): Promise<void> {
    const row = normalizeGroup(group);
    this.groups.set(row.id, row);
  }

  async getGroup(id: string): Promise<AccessGroup | undefined> {
    const row = this.groups.get(requiredString(id, "group id"));
    return row && clone(row);
  }

  async putGroupMembership(membership: GroupMembership): Promise<void> {
    const row = normalizeMembership(membership);
    if (!this.groups.has(row.groupId)) throw new Error(`Unknown group: ${row.groupId}`);
    this.assertPrincipalExists(row.subject);
    this.memberships.set(membershipKey(row), row);
  }

  async removeGroupMembership(membership: GroupMembership): Promise<void> {
    this.memberships.delete(membershipKey(normalizeMembership(membership)));
  }

  async listGroupsForSubject(subject: SecurityPrincipalRef): Promise<AccessGroup[]> {
    const normalized = normalizePrincipalRef(subject);
    if (!this.principalExists(normalized)) return [];
    const ids = new Set(
      [...this.memberships.values()]
        .filter((row) => samePrincipal(row.subject, normalized))
        .map((row) => row.groupId),
    );
    return [...ids]
      .map((id) => this.groups.get(id))
      .filter((group): group is AccessGroup => group?.status === "active")
      .map(clone);
  }

  async putSubjectBinding(binding: SubjectAccessBinding): Promise<void> {
    const row = normalizeSubjectBinding(binding);
    if (row.subject.kind === "group" && !this.groups.has(row.subject.id)) {
      throw new Error(`Unknown group: ${row.subject.id}`);
    }
    if (row.subject.kind !== "group") this.assertPrincipalExists(row.subject);
    this.storeSubjectBinding(row);
  }

  async removeSubjectBinding(binding: SubjectAccessBinding): Promise<void> {
    this.bindings.delete(subjectBindingKey(normalizeSubjectBinding(binding)));
  }

  async listSubjectBindings(subject: AccessSubjectRef): Promise<SubjectAccessBinding[]> {
    const normalized = normalizeAccessSubject(subject);
    return [...this.bindings.values()]
      .filter((row) => sameAccessSubject(row.subject, normalized))
      .map(clone);
  }

  async resolveAccess(params: {
    readonly subject: SecurityPrincipalRef;
    readonly namespaceId: string;
  }): Promise<{ accessRoles: ReadonlyArray<string>; groups: ReadonlyArray<string> }> {
    const subject = normalizePrincipalRef(params.subject);
    const namespaceId = requiredString(params.namespaceId, "namespace id");
    if (!this.principalActive(subject)) {
      return freeze({ accessRoles: freeze([]), groups: freeze([]) });
    }
    const groups = await this.listGroupsForSubject(subject);
    const subjects: AccessSubjectRef[] = [
      subject,
      ...groups.map((group) => ({ kind: "group" as const, id: group.id })),
    ];
    const roles = new Set<string>();
    for (const binding of this.bindings.values()) {
      if (binding.namespaceId !== namespaceId && binding.namespaceId !== ORG_WIDE_NAMESPACE)
        continue;
      if (subjects.some((candidate) => sameAccessSubject(candidate, binding.subject))) {
        roles.add(binding.accessRole);
      }
    }
    return freeze({
      accessRoles: freeze([...roles]),
      groups: freeze(groups.map((group) => group.id)),
    });
  }

  async putKnowledgeAudienceGrant(grant: KnowledgeAudienceGrant): Promise<void> {
    const row = normalizeKnowledgeAudienceGrant(grant);
    if (row.audience.kind === "group" && !this.groups.has(row.audience.id)) {
      throw new Error(`Unknown group: ${row.audience.id}`);
    }
    this.knowledgeGrants.set(knowledgeAudienceKey(row), row);
  }

  async removeKnowledgeAudienceGrant(grant: KnowledgeAudienceGrant): Promise<void> {
    this.knowledgeGrants.delete(knowledgeAudienceKey(normalizeKnowledgeAudienceGrant(grant)));
  }

  async listKnowledgeAudienceGrants(params: {
    readonly namespaceId: string;
    readonly baseId: string;
  }): Promise<KnowledgeAudienceGrant[]> {
    const namespaceId = requiredString(params.namespaceId, "namespace id");
    const baseId = requiredString(params.baseId, "knowledge base id");
    return [...this.knowledgeGrants.values()]
      .filter(
        (grant) =>
          grant.baseId === baseId &&
          (grant.namespaceId === namespaceId || grant.namespaceId === ORG_WIDE_NAMESPACE),
      )
      .map(clone);
  }

  async putServiceAccount(account: SecurityServiceAccount): Promise<void> {
    const row = normalizeServiceAccount(account);
    this.serviceAccounts.set(row.id, row);
    if (row.status === "disabled") {
      this.invalidateSessionsForPrincipal({ kind: "service-account", id: row.id });
    }
  }

  async getServiceAccount(id: string): Promise<SecurityServiceAccount | undefined> {
    const row = this.serviceAccounts.get(requiredString(id, "service account id"));
    return row && clone(row);
  }

  async listServiceAccounts(): Promise<SecurityServiceAccount[]> {
    return [...this.serviceAccounts.values()].map(clone);
  }

  async putServiceAccountCredential(credential: ServiceAccountCredential): Promise<void> {
    const row = normalizeCredential(credential);
    if (!this.serviceAccounts.has(row.serviceAccountId)) {
      throw new Error(`Unknown service account: ${row.serviceAccountId}`);
    }
    const existing = this.serviceAccountCredentials.get(row.id);
    if (existing && JSON.stringify(existing) !== JSON.stringify(row)) {
      throw new Error(`Conflicting credential id: ${row.id}`);
    }
    this.serviceAccountCredentials.set(row.id, row);
  }

  async getServiceAccountCredential(id: string): Promise<ServiceAccountCredential | undefined> {
    const row = this.serviceAccountCredentials.get(requiredString(id, "credential id"));
    return row && clone(row);
  }

  async listServiceAccountCredentials(
    serviceAccountId: string,
  ): Promise<ServiceAccountCredential[]> {
    const id = requiredString(serviceAccountId, "service account id");
    return [...this.serviceAccountCredentials.values()]
      .filter((row) => row.serviceAccountId === id)
      .map(clone);
  }

  async revokeServiceAccountCredential(params: {
    readonly id: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const id = requiredString(params.id, "credential id");
    const existing = this.serviceAccountCredentials.get(id);
    if (!existing) return;
    this.serviceAccountCredentials.set(
      id,
      freeze({ ...existing, revokedAt: finiteNumber(params.revokedAt, "revoked at") }),
    );
  }

  async putSession(session: SecuritySession): Promise<void> {
    const row = normalizeSession(session);
    if (row.authMethod === "break-glass") {
      throw new Error("Break-glass sessions must be created transactionally");
    }
    this.assertPrincipalActive(row.principal);
    const breakGlassGrantId = this.breakGlassGrantIdByTokenHash.get(row.tokenHash);
    if (breakGlassGrantId !== undefined) {
      throw new Error(`Session token belongs to break-glass grant: ${breakGlassGrantId}`);
    }
    const existing = this.sessions.get(row.tokenHash);
    if (existing !== undefined && !recordsEqual(existing, row)) {
      throw new Error(`Conflicting session token hash: ${row.tokenHash}`);
    }
    this.sessions.set(row.tokenHash, row);
  }

  async getSessionRecord(tokenHash: string): Promise<SecuritySession | undefined> {
    const key = requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    const row = this.sessions.get(key);
    if (!row) return undefined;
    if (
      row.revokedAt !== undefined ||
      this.invalidatedSessionTokenHashes.has(key) ||
      row.expiresAt <= this.clock.currentTimeMs() ||
      !this.principalActive(row.principal)
    ) {
      return undefined;
    }
    return clone(row);
  }

  async revokeSession(params: {
    readonly tokenHash: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const key = requiredString(params.tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    const existing = this.sessions.get(key);
    if (!existing) return;
    if (existing.authMethod === "break-glass") {
      throw new Error("Break-glass sessions must be revoked transactionally");
    }
    const revokedAt = finiteNumber(params.revokedAt, "revoked at");
    if (existing.revokedAt !== undefined) {
      if (existing.revokedAt === revokedAt) return;
      throw new Error(`Session is already revoked: ${key}`);
    }
    this.sessions.set(key, freeze({ ...existing, revokedAt }));
  }

  async appendAuditWithOutbox(params: {
    readonly record: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"appended" | "already-exists"> {
    const prepared = this.prepareAuditAppend(params);
    if (prepared.result === "already-exists") return prepared.result;
    this.commitAuditAppend(prepared);
    return "appended";
  }

  async listAuditRecords(namespaceId: string): Promise<SecurityAuditRecord[]> {
    return (this.auditRecords.get(requiredString(namespaceId, "namespace id")) ?? []).map(clone);
  }

  async getAuditOutbox(eventId: string): Promise<SecurityAuditOutboxRecord | undefined> {
    const row = this.auditOutbox.get(requiredString(eventId, "event id"));
    return row && clone(row);
  }

  async listAuditOutbox(
    params: {
      readonly status?: SecurityAuditOutboxRecord["status"];
      readonly limit?: number;
    } = {},
  ): Promise<SecurityAuditOutboxRecord[]> {
    const status = params.status;
    if (
      status !== undefined &&
      status !== "pending" &&
      status !== "leased" &&
      status !== "delivered"
    ) {
      throw new TypeError("invalid outbox status");
    }
    const limit =
      params.limit === undefined
        ? SECURITY_STORE_LIMITS.outboxBatch
        : positiveInteger(params.limit, "outbox list limit");
    if (limit > SECURITY_STORE_LIMITS.outboxBatch) {
      throw new TypeError("outbox list limit is too large");
    }
    return [...this.auditOutbox.values()]
      .filter((row) => status === undefined || row.status === status)
      .slice(0, limit)
      .map(clone);
  }

  async claimAuditOutbox(params: {
    readonly owner: string;
    readonly now: number;
    readonly leaseMs: number;
    readonly limit: number;
  }): Promise<ReadonlyArray<SecurityAuditOutboxRecord>> {
    const owner = requiredString(params.owner, "outbox lease owner");
    const now = finiteNumber(params.now, "outbox claim time");
    const leaseMs = positiveNumber(params.leaseMs, "outbox lease duration");
    const limit = positiveInteger(params.limit, "outbox claim limit");
    if (limit > SECURITY_STORE_LIMITS.outboxBatch) {
      throw new TypeError("outbox claim limit is too large");
    }
    const due = [...this.auditOutbox.values()]
      .filter(
        (row) =>
          (row.status === "pending" && row.nextAttemptAt <= now) ||
          (row.status === "leased" &&
            row.leaseExpiresAt !== undefined &&
            row.leaseExpiresAt <= now),
      )
      .sort(
        (left, right) =>
          left.nextAttemptAt - right.nextAttemptAt || left.eventId.localeCompare(right.eventId),
      )
      .slice(0, limit);
    const claimed: SecurityAuditOutboxRecord[] = [];
    for (const row of due) {
      const next = freeze<SecurityAuditOutboxRecord>({
        eventId: row.eventId,
        status: "leased",
        attempts: safeIncrement(row.attempts, "outbox attempts"),
        nextAttemptAt: row.nextAttemptAt,
        leaseToken: crypto.randomUUID(),
        leaseOwner: owner,
        leaseExpiresAt: finiteSum(now, leaseMs, "outbox lease expiry"),
        ...(row.lastError !== undefined && { lastError: row.lastError }),
      });
      this.auditOutbox.set(next.eventId, next);
      claimed.push(clone(next));
    }
    return freeze(claimed);
  }

  async ackAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
    readonly deliveredAt: number;
  }): Promise<boolean> {
    const eventId = requiredString(params.eventId, "event id");
    const leaseToken = requiredString(params.leaseToken, "outbox lease token");
    const deliveredAt = finiteNumber(params.deliveredAt, "outbox delivery time");
    const row = this.auditOutbox.get(eventId);
    if (!row) return false;
    if (row.status === "delivered") {
      return row.leaseToken === leaseToken && row.deliveredAt === deliveredAt;
    }
    if (
      row.status !== "leased" ||
      row.leaseToken !== leaseToken ||
      row.leaseExpiresAt === undefined ||
      deliveredAt > row.leaseExpiresAt
    ) {
      return false;
    }
    this.auditOutbox.set(eventId, freeze({ ...row, status: "delivered", deliveredAt }));
    return true;
  }

  async releaseAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
    readonly nextAttemptAt: number;
    readonly lastError?: string;
  }): Promise<boolean> {
    const eventId = requiredString(params.eventId, "event id");
    const leaseToken = requiredString(params.leaseToken, "outbox lease token");
    const nextAttemptAt = finiteNumber(params.nextAttemptAt, "outbox next attempt");
    const lastError = optionalBoundedString(
      params.lastError,
      "outbox last error",
      SECURITY_STORE_LIMITS.reason,
    );
    const row = this.auditOutbox.get(eventId);
    if (!row) return false;
    if (row.status === "pending") {
      return (
        row.leaseToken === leaseToken &&
        row.nextAttemptAt === nextAttemptAt &&
        row.lastError === lastError
      );
    }
    if (row.status !== "leased" || row.leaseToken !== leaseToken) return false;
    this.auditOutbox.set(
      eventId,
      freeze({
        eventId,
        status: "pending",
        attempts: row.attempts,
        nextAttemptAt,
        leaseToken,
        ...(lastError !== undefined && { lastError }),
      }),
    );
    return true;
  }

  async incrementDenialBucket(params: {
    readonly key: string;
    readonly now: number;
    readonly windowMs: number;
    readonly emitLimit: number;
  }): Promise<{ readonly bucket: SecurityDenialBucket; readonly shouldEmit: boolean }> {
    const key = requiredString(params.key, "denial bucket key");
    const now = finiteNumber(params.now, "denial time");
    const windowMs = positiveNumber(params.windowMs, "denial window");
    const emitLimit = nonNegativeInteger(params.emitLimit, "denial emit limit");
    const existing = this.denialBuckets.get(key);
    if (existing !== undefined && now < existing.windowStartedAt) {
      throw new Error("Denial time precedes the current window");
    }
    const resets = existing === undefined || now >= existing.windowEndsAt;
    const count = resets ? 1 : safeIncrement(existing.count, "denial count");
    const shouldEmit = count <= emitLimit;
    const suppressed = resets
      ? shouldEmit
        ? 0
        : 1
      : shouldEmit
        ? existing.suppressed
        : safeIncrement(existing.suppressed, "denial suppressed count");
    const row = freeze<SecurityDenialBucket>({
      key,
      windowStartedAt: resets ? now : existing.windowStartedAt,
      windowEndsAt: resets ? finiteSum(now, windowMs, "denial window end") : existing.windowEndsAt,
      count,
      suppressed,
    });
    this.denialBuckets.set(key, row);
    return freeze({ bucket: clone(row), shouldEmit });
  }

  async getDenialBucket(key: string): Promise<SecurityDenialBucket | undefined> {
    const row = this.denialBuckets.get(requiredString(key, "denial bucket key"));
    return row && clone(row);
  }

  async getBreakGlassGrant(id: string): Promise<BreakGlassGrant | undefined> {
    const row = this.breakGlassGrants.get(requiredString(id, "break-glass grant id"));
    return row && clone(row);
  }

  async createBreakGlassAccess(params: {
    readonly grant: BreakGlassGrant;
    readonly session: SecuritySession;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"created" | "already-exists"> {
    const grant = normalizeBreakGlassGrant(params.grant);
    const session = normalizeSession(params.session);
    this.assertPrincipalActive(grant.principal);
    validateBreakGlassSession(grant, session);
    const audit = this.prepareAuditAppend({ record: params.audit, outbox: params.outbox });
    validateBreakGlassAudit(audit.row, grant, "break-glass.created");

    const tokenGrantId = this.breakGlassGrantIdByTokenHash.get(grant.tokenHash);
    if (tokenGrantId !== undefined && tokenGrantId !== grant.id) {
      throw new Error(`Break-glass token hash is already used by grant: ${tokenGrantId}`);
    }
    const existingGrant = this.breakGlassGrants.get(grant.id);
    const existingSession = this.sessions.get(session.tokenHash);
    const anyExists =
      existingGrant !== undefined ||
      existingSession !== undefined ||
      audit.result === "already-exists";
    if (anyExists) {
      if (
        existingGrant !== undefined &&
        existingSession !== undefined &&
        audit.result === "already-exists" &&
        recordsEqual(existingGrant, grant) &&
        recordsEqual(existingSession, session)
      ) {
        return "already-exists";
      }
      throw new Error("Conflicting or incomplete break-glass transaction");
    }

    this.breakGlassGrants.set(grant.id, grant);
    this.breakGlassGrantIdByTokenHash.set(grant.tokenHash, grant.id);
    this.sessions.set(session.tokenHash, session);
    this.commitAuditAppend(audit);
    return "created";
  }

  async revokeBreakGlassAccess(params: {
    readonly grantId: string;
    readonly revokedAt: number;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"revoked" | "already-revoked"> {
    const grantId = requiredString(params.grantId, "break-glass grant id");
    const revokedAt = finiteNumber(params.revokedAt, "break-glass revoked at");
    const grant = this.breakGlassGrants.get(grantId);
    if (!grant) throw new Error(`Unknown break-glass grant: ${grantId}`);
    const session = this.sessions.get(grant.tokenHash);
    if (!session || session.authMethod !== "break-glass") {
      throw new Error("Break-glass grant has no matching session");
    }
    const audit = this.prepareAuditAppend({ record: params.audit, outbox: params.outbox });
    validateBreakGlassAudit(audit.row, grant, "break-glass.revoked");
    if (grant.revokedAt !== undefined || session.revokedAt !== undefined) {
      if (
        grant.revokedAt === revokedAt &&
        session.revokedAt === revokedAt &&
        audit.result === "already-exists"
      ) {
        return "already-revoked";
      }
      throw new Error("Conflicting or incomplete break-glass revocation");
    }
    if (audit.result === "already-exists") {
      throw new Error("Audit event already exists for an unrevoked break-glass grant");
    }
    this.breakGlassGrants.set(grantId, freeze({ ...grant, revokedAt }));
    this.sessions.set(session.tokenHash, freeze({ ...session, revokedAt }));
    this.commitAuditAppend(audit);
    return "revoked";
  }

  private storeSubjectBinding(row: SubjectAccessBinding): void {
    this.bindings.set(subjectBindingKey(row), freeze(row));
  }

  private principalExists(subject: SecurityPrincipalRef): boolean {
    return subject.kind === "user"
      ? this.usersById.has(subject.id)
      : this.serviceAccounts.has(subject.id);
  }

  private principalActive(subject: SecurityPrincipalRef): boolean {
    return subject.kind === "user"
      ? this.usersById.get(subject.id)?.status === "active"
      : this.serviceAccounts.get(subject.id)?.status === "active";
  }

  private assertPrincipalExists(subject: SecurityPrincipalRef): void {
    if (!this.principalExists(subject)) {
      throw new Error(`Unknown ${subject.kind}: ${subject.id}`);
    }
  }

  private assertPrincipalActive(subject: SecurityPrincipalRef): void {
    if (!this.principalActive(subject)) {
      throw new Error(`Unknown or disabled ${subject.kind}: ${subject.id}`);
    }
  }

  private invalidateSessionsForPrincipal(subject: SecurityPrincipalRef): void {
    for (const [tokenHash, session] of this.sessions) {
      if (samePrincipal(session.principal, subject)) {
        this.invalidatedSessionTokenHashes.add(tokenHash);
      }
    }
  }

  private prepareAuditAppend(params: {
    readonly record: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): PreparedAuditAppend {
    const recordInput = params.record;
    const outboxInput = params.outbox;
    const row = normalizeAuditRecord(recordInput);
    const outbox =
      outboxInput === undefined ? undefined : pendingAuditOutbox(row.id, outboxInput.nextAttemptAt);
    const existing = this.auditRecords
      .get(row.namespaceId)
      ?.find((candidate) => candidate.id === row.id);
    if (this.auditIds.has(row.id)) {
      const expectedOutbox = this.auditIdsWithOutbox.has(row.id);
      const expectedNextAttemptAt = this.auditOutboxInitialNextAttemptAt.get(row.id);
      if (
        existing !== undefined &&
        recordsEqual(existing, row) &&
        expectedOutbox === (outbox !== undefined) &&
        expectedNextAttemptAt === outbox?.nextAttemptAt
      ) {
        return { result: "already-exists", row, outbox };
      }
      throw new Error(`Conflicting audit event: ${row.id}`);
    }
    const chain = this.auditRecords.get(row.namespaceId) ?? [];
    const expectedSequence = chain.length + 1;
    const expectedPrevious = chain.at(-1)?.hash;
    if (row.sequence !== expectedSequence || row.previousHash !== expectedPrevious) {
      throw new Error("Audit chain boundary mismatch");
    }
    if (outbox !== undefined && this.auditOutbox.has(row.id)) {
      throw new Error(`Conflicting audit outbox event: ${row.id}`);
    }
    return { result: "appended", row, outbox };
  }

  private commitAuditAppend(prepared: PreparedAuditAppend): void {
    if (prepared.result !== "appended") return;
    const chain = this.auditRecords.get(prepared.row.namespaceId) ?? [];
    this.auditIds.add(prepared.row.id);
    this.auditRecords.set(prepared.row.namespaceId, [...chain, prepared.row]);
    if (prepared.outbox !== undefined) {
      this.auditIdsWithOutbox.add(prepared.row.id);
      this.auditOutboxInitialNextAttemptAt.set(prepared.row.id, prepared.outbox.nextAttemptAt);
      this.auditOutbox.set(prepared.row.id, prepared.outbox);
    }
  }
}

interface PreparedAuditAppend {
  readonly result: "appended" | "already-exists";
  readonly row: SecurityAuditRecord;
  readonly outbox?: SecurityAuditOutboxRecord;
}

function normalizeEmail(email: unknown): string {
  const normalized = requiredString(email, "email", SECURITY_STORE_LIMITS.email)
    .trim()
    .toLowerCase();
  if (!normalized.includes("@")) throw new TypeError("email must be an address");
  return requiredString(normalized, "email", SECURITY_STORE_LIMITS.email);
}

function requiredString(
  value: unknown,
  label: string,
  limit: SecurityStoreTextLimit = SECURITY_STORE_LIMITS.identifier,
): string {
  if (typeof value !== "string") {
    throw new TypeError(`${label} must be a non-empty safe string`);
  }
  if (value.length > limit.maxCharacters * 2) {
    throw new TypeError(`${label} exceeds the character limit`);
  }
  if (value.trim().length === 0 || hasUnsafeStringCharacter(value)) {
    throw new TypeError(`${label} must be a non-empty safe string`);
  }
  assertStringWithinLimit(value, label, limit);
  return value;
}

function optionalBoundedString(
  value: unknown,
  label: string,
  limit: SecurityStoreTextLimit,
): string | undefined {
  return value === undefined ? undefined : requiredString(value, label, limit);
}

function assertStringWithinLimit(
  value: string,
  label: string,
  limit: SecurityStoreTextLimit,
): void {
  if (value.length > limit.maxCharacters * 2) {
    throw new TypeError(`${label} exceeds the character limit`);
  }
  let characters = 0;
  for (const _character of value) {
    characters += 1;
    if (characters > limit.maxCharacters) {
      throw new TypeError(`${label} exceeds the character limit`);
    }
  }
  if (new TextEncoder().encode(value).byteLength > limit.maxBytes) {
    throw new TypeError(`${label} exceeds the byte limit`);
  }
}

function hasUnsafeStringCharacter(value: string): boolean {
  for (const character of value) {
    const point = character.codePointAt(0)!;
    if (
      point <= 0x1f ||
      point === 0x7f ||
      (point >= 0x200b && point <= 0x200f) ||
      (point >= 0x202a && point <= 0x202e) ||
      (point >= 0x2060 && point <= 0x206f) ||
      point === 0xfeff
    ) {
      return true;
    }
  }
  return false;
}

function finiteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new TypeError(`${label} must be finite`);
  }
  return value;
}

function positiveNumber(value: unknown, label: string): number {
  const normalized = finiteNumber(value, label);
  if (normalized <= 0) throw new TypeError(`${label} must be positive`);
  return normalized;
}

function finiteSum(left: number, right: number, label: string): number {
  const result = left + right;
  if (!Number.isFinite(result)) throw new TypeError(`${label} must be finite`);
  return result;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${label} must be a non-negative safe integer`);
  }
  return value;
}

function positiveInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${label} must be a positive safe integer`);
  }
  return value;
}

function safeIncrement(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value >= Number.MAX_SAFE_INTEGER) {
    throw new Error(`${label} overflow`);
  }
  return value + 1;
}

function boundedArray<T>(value: ReadonlyArray<T>, label: string): ReadonlyArray<T> {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const length = value.length;
  if (!Number.isSafeInteger(length) || length > SECURITY_STORE_LIMITS.initialBindings) {
    throw new TypeError(`${label} is too large`);
  }
  const rows: T[] = [];
  for (let index = 0; index < length; index += 1) rows.push(value[index]!);
  return rows;
}

function normalizeInitialBindings(
  value: ReadonlyArray<{ readonly namespaceId: string; readonly accessRole: string }>,
): ReadonlyArray<{ readonly namespaceId: string; readonly accessRole: string }> {
  return boundedArray(value, "initial bindings").map((binding) => {
    if (!binding || typeof binding !== "object") {
      throw new TypeError("initial binding must be an object");
    }
    const source = binding as unknown as Record<string, unknown>;
    const namespaceId = source.namespaceId;
    const accessRole = source.accessRole;
    return freeze({
      namespaceId: requiredString(namespaceId, "namespace id"),
      accessRole: requiredString(accessRole, "access role"),
    });
  });
}

function statusValue(value: unknown): "active" | "disabled" {
  if (value !== "active" && value !== "disabled") throw new TypeError("invalid status");
  return value;
}

function normalizePrincipalRef(value: SecurityPrincipalRef): SecurityPrincipalRef {
  if (!value || typeof value !== "object") throw new TypeError("principal must be an object");
  const source = value as unknown as Record<string, unknown>;
  const kind = source.kind;
  const id = source.id;
  if (kind !== "user" && kind !== "service-account") throw new TypeError("invalid principal kind");
  return freeze({ kind, id: requiredString(id, "principal id") });
}

function normalizeAccessSubject(value: AccessSubjectRef): AccessSubjectRef {
  if (!value || typeof value !== "object") throw new TypeError("access subject must be an object");
  const source = value as unknown as Record<string, unknown>;
  const kind = source.kind;
  const id = source.id;
  if (kind !== "user" && kind !== "service-account" && kind !== "group") {
    throw new TypeError("invalid access subject kind");
  }
  return freeze({ kind, id: requiredString(id, "access subject id") }) as AccessSubjectRef;
}

function normalizeProviderIdentity(
  value: Omit<ProviderIdentity, "userId">,
): Omit<ProviderIdentity, "userId"> {
  if (!value || typeof value !== "object")
    throw new TypeError("provider identity must be an object");
  const source = value as unknown as Record<string, unknown>;
  const providerId = source.providerId;
  const issuer = source.issuer;
  const subject = source.subject;
  return freeze({
    providerId: requiredString(providerId, "provider id"),
    issuer: requiredString(issuer, "issuer", SECURITY_STORE_LIMITS.issuer),
    subject: requiredString(subject, "provider subject"),
  });
}

function normalizeGroup(value: AccessGroup): AccessGroup {
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const status = source.status;
  return freeze({ id: requiredString(id, "group id"), status: statusValue(status) });
}

function normalizeMembership(value: GroupMembership): GroupMembership {
  const source = value as unknown as Record<string, unknown>;
  const groupId = source.groupId;
  const subject = source.subject;
  return freeze({
    groupId: requiredString(groupId, "group id"),
    subject: normalizePrincipalRef(subject as SecurityPrincipalRef),
  });
}

function normalizeSubjectBinding(value: SubjectAccessBinding): SubjectAccessBinding {
  const source = value as unknown as Record<string, unknown>;
  const subject = source.subject;
  const namespaceId = source.namespaceId;
  const accessRole = source.accessRole;
  return freeze({
    subject: normalizeAccessSubject(subject as AccessSubjectRef),
    namespaceId: requiredString(namespaceId, "namespace id"),
    accessRole: requiredString(accessRole, "access role"),
  });
}

function normalizeKnowledgeAudienceGrant(value: KnowledgeAudienceGrant): KnowledgeAudienceGrant {
  const source = value as unknown as Record<string, unknown>;
  const namespaceId = source.namespaceId;
  const baseId = source.baseId;
  const audienceSource = source.audience;
  if (!audienceSource || typeof audienceSource !== "object") {
    throw new TypeError("knowledge audience must be an object");
  }
  const audienceRecord = audienceSource as Record<string, unknown>;
  const kind = audienceRecord.kind;
  const id = audienceRecord.id;
  if (kind !== "access-role" && kind !== "group") throw new TypeError("invalid audience kind");
  return freeze({
    namespaceId: requiredString(namespaceId, "namespace id"),
    baseId: requiredString(baseId, "knowledge base id"),
    audience: freeze({ kind, id: requiredString(id, "audience id") }),
  });
}

function normalizeServiceAccount(value: SecurityServiceAccount): SecurityServiceAccount {
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const status = source.status;
  const canActFor = source.canActFor;
  if (typeof canActFor !== "boolean") throw new TypeError("canActFor must be boolean");
  return freeze({
    id: requiredString(id, "service account id"),
    status: statusValue(status),
    canActFor,
  });
}

function normalizeCredential(value: ServiceAccountCredential): ServiceAccountCredential {
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const serviceAccountId = source.serviceAccountId;
  const secretHash = source.secretHash;
  const createdAt = source.createdAt;
  const expiresAt = source.expiresAt;
  const revokedAt = source.revokedAt;
  const row: ServiceAccountCredential = {
    id: requiredString(id, "credential id"),
    serviceAccountId: requiredString(serviceAccountId, "service account id"),
    secretHash: requiredString(secretHash, "credential hash", SECURITY_STORE_LIMITS.hash),
    createdAt: finiteNumber(createdAt, "credential created at"),
    ...(expiresAt !== undefined && { expiresAt: finiteNumber(expiresAt, "credential expires at") }),
    ...(revokedAt !== undefined && { revokedAt: finiteNumber(revokedAt, "credential revoked at") }),
  };
  if (row.expiresAt !== undefined && row.expiresAt <= row.createdAt) {
    throw new TypeError("credential expiry must follow creation");
  }
  return freeze(row);
}

function normalizeSession(value: SecuritySession): SecuritySession {
  const source = value as unknown as Record<string, unknown>;
  const tokenHash = source.tokenHash;
  const principal = source.principal;
  const createdAt = source.createdAt;
  const expiresAt = source.expiresAt;
  const csrfTokenHash = source.csrfTokenHash;
  const revokedAt = source.revokedAt;
  const authMethod = source.authMethod;
  if (authMethod !== "oidc" && authMethod !== "service-account" && authMethod !== "break-glass") {
    throw new TypeError("invalid session auth method");
  }
  const row: SecuritySession = {
    tokenHash: requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash),
    principal: normalizePrincipalRef(principal as SecurityPrincipalRef),
    createdAt: finiteNumber(createdAt, "session created at"),
    expiresAt: finiteNumber(expiresAt, "session expires at"),
    ...(csrfTokenHash !== undefined && {
      csrfTokenHash: requiredString(csrfTokenHash, "csrf token hash", SECURITY_STORE_LIMITS.hash),
    }),
    ...(revokedAt !== undefined && { revokedAt: finiteNumber(revokedAt, "session revoked at") }),
    authMethod,
  };
  if (row.expiresAt <= row.createdAt) throw new TypeError("session expiry must follow creation");
  return freeze(row);
}

function normalizeAuditRecord(value: SecurityAuditRecord): SecurityAuditRecord {
  if (!value || typeof value !== "object") throw new TypeError("audit record must be an object");
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const namespaceId = source.namespaceId;
  const sequence = source.sequence;
  const occurredAt = source.occurredAt;
  const recordedAt = source.recordedAt;
  const event = source.event;
  const actor = source.actor;
  const effective = source.effective;
  const permission = source.permission;
  const resource = source.resource;
  const decision = source.decision;
  const correlation = source.correlation;
  const metadata = source.metadata;
  const previousHash = source.previousHash;
  const hash = source.hash;
  const keyId = source.keyId;
  const row: SecurityAuditRecord = {
    id: requiredString(id, "audit event id"),
    namespaceId: requiredString(namespaceId, "namespace id", SECURITY_STORE_LIMITS.namespace),
    sequence: nonNegativeInteger(sequence, "audit sequence"),
    occurredAt: finiteNumber(occurredAt, "audit occurrence"),
    recordedAt: finiteNumber(recordedAt, "audit recorded time"),
    event: requiredString(event, "audit event", SECURITY_STORE_LIMITS.event),
    ...(actor !== undefined && { actor: normalizeAuditPrincipal(actor, "audit actor") }),
    ...(effective !== undefined && {
      effective: normalizeAuditPrincipal(effective, "audit effective principal"),
    }),
    ...(permission !== undefined && { permission: normalizeAuditPermission(permission) }),
    ...(resource !== undefined && { resource: normalizeAuditResource(resource) }),
    ...(decision !== undefined && { decision: normalizeAuditDecision(decision) }),
    ...(correlation !== undefined && { correlation: normalizeAuditCorrelation(correlation) }),
    ...(metadata !== undefined && { metadata: normalizeAuditMetadata(metadata) }),
    ...(previousHash !== undefined && {
      previousHash: requiredString(previousHash, "previous audit hash", SECURITY_STORE_LIMITS.hash),
    }),
    hash: requiredString(hash, "audit hash", SECURITY_STORE_LIMITS.hash),
    keyId: requiredString(keyId, "audit key id"),
  };
  if ((row.actor === undefined) !== (row.effective === undefined)) {
    throw new TypeError("audit actor and effective principal must appear together");
  }
  if (row.recordedAt < row.occurredAt) {
    throw new TypeError("audit recorded time cannot precede occurrence");
  }
  if (
    row.permission !== undefined &&
    row.resource !== undefined &&
    row.permission.resource !== row.resource.kind
  ) {
    throw new TypeError("audit permission and resource kinds must match");
  }
  if (row.resource !== undefined && row.resource.namespaceId !== row.namespaceId) {
    throw new TypeError("audit resource namespace must match the audit namespace");
  }
  return freeze(row);
}

function normalizeBreakGlassGrant(value: BreakGlassGrant): BreakGlassGrant {
  if (!value || typeof value !== "object")
    throw new TypeError("break-glass grant must be an object");
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const principal = source.principal;
  const tokenHash = source.tokenHash;
  const reason = source.reason;
  const createdBy = source.createdBy;
  const createdAt = source.createdAt;
  const expiresAt = source.expiresAt;
  const revokedAt = source.revokedAt;
  const row: BreakGlassGrant = {
    id: requiredString(id, "break-glass grant id"),
    principal: normalizePrincipalRef(principal as SecurityPrincipalRef),
    tokenHash: requiredString(tokenHash, "break-glass token hash", SECURITY_STORE_LIMITS.hash),
    reason: requiredString(reason, "break-glass reason", SECURITY_STORE_LIMITS.reason),
    createdBy: requiredString(createdBy, "break-glass creator"),
    createdAt: finiteNumber(createdAt, "break-glass created at"),
    expiresAt: finiteNumber(expiresAt, "break-glass expires at"),
    ...(revokedAt !== undefined && {
      revokedAt: finiteNumber(revokedAt, "break-glass revoked at"),
    }),
  };
  if (row.expiresAt <= row.createdAt)
    throw new TypeError("break-glass expiry must follow creation");
  return freeze(row);
}

function normalizeAuditPrincipal(
  value: unknown,
  label: string,
): NonNullable<SecurityAuditRecord["actor"]> {
  if (!value || typeof value !== "object") throw new TypeError(`${label} must be an object`);
  const source = value as Record<string, unknown>;
  const kind = source.kind;
  const id = source.id;
  if (kind !== "operator" && kind !== "worker" && kind !== "user" && kind !== "service-account") {
    throw new TypeError(`${label} has an invalid kind`);
  }
  return freeze({ kind, id: requiredString(id, `${label} id`) });
}

function normalizeAuditPermission(value: unknown): SecurityAuditRecord["permission"] {
  if (!value || typeof value !== "object")
    throw new TypeError("audit permission must be an object");
  const source = value as Record<string, unknown>;
  const resource = source.resource;
  const action = source.action;
  if (typeof resource !== "string" || !(resource in SECURITY_ACTIONS)) {
    throw new TypeError("invalid audit permission resource");
  }
  const actions = SECURITY_ACTIONS[resource as keyof typeof SECURITY_ACTIONS];
  if (typeof action !== "string" || !(actions as ReadonlyArray<string>).includes(action)) {
    throw new TypeError("invalid audit permission action");
  }
  return freeze({
    resource: requiredString(resource, "audit permission resource"),
    action: requiredString(action, "audit permission action"),
  }) as SecurityAuditRecord["permission"];
}

function normalizeAuditResource(value: unknown): SecurityAuditRecord["resource"] {
  if (!value || typeof value !== "object") throw new TypeError("audit resource must be an object");
  const source = value as Record<string, unknown>;
  const kind = source.kind;
  const namespaceId = source.namespaceId;
  const id = source.id;
  const principal = source.principal;
  if (typeof kind !== "string" || !(kind in SECURITY_ACTIONS)) {
    throw new TypeError("invalid audit resource kind");
  }
  return freeze({
    kind: requiredString(kind, "audit resource kind"),
    namespaceId: requiredString(
      namespaceId,
      "audit resource namespace",
      SECURITY_STORE_LIMITS.namespace,
    ),
    ...(id !== undefined && { id: requiredString(id, "audit resource id") }),
    ...(principal !== undefined && {
      principal: normalizeAuditPrincipal(principal, "audit resource principal"),
    }),
  }) as SecurityAuditRecord["resource"];
}

function normalizeAuditDecision(value: unknown): SecurityAuditRecord["decision"] {
  if (!value || typeof value !== "object") throw new TypeError("audit decision must be an object");
  const source = value as Record<string, unknown>;
  const allowed = source.allowed;
  const reason = source.reason;
  if (typeof allowed !== "boolean") throw new TypeError("audit decision allowed must be boolean");
  if (
    typeof reason !== "string" ||
    !(AUTHORIZATION_REASONS as ReadonlyArray<string>).includes(reason)
  ) {
    throw new TypeError("invalid audit decision reason");
  }
  return freeze({
    allowed,
    reason: requiredString(reason, "audit decision reason"),
  }) as SecurityAuditRecord["decision"];
}

function normalizeAuditCorrelation(
  value: unknown,
): NonNullable<SecurityAuditRecord["correlation"]> {
  if (!value || typeof value !== "object")
    throw new TypeError("audit correlation must be an object");
  const source = value as Record<string, unknown>;
  const requestId = source.requestId;
  const runId = source.runId;
  const threadId = source.threadId;
  const toolCallId = source.toolCallId;
  return freeze({
    ...(requestId !== undefined && { requestId: requiredString(requestId, "request id") }),
    ...(runId !== undefined && { runId: requiredString(runId, "run id") }),
    ...(threadId !== undefined && { threadId: requiredString(threadId, "thread id") }),
    ...(toolCallId !== undefined && { toolCallId: requiredString(toolCallId, "tool call id") }),
  });
}

function normalizeAuditMetadata(
  value: unknown,
): Readonly<Record<string, string | number | boolean | null>> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("audit metadata must be an object");
  }
  const source = value as Record<string, unknown>;
  const keys = Object.keys(source).sort();
  if (keys.length > SECURITY_STORE_LIMITS.metadataEntries) {
    throw new TypeError("audit metadata has too many entries");
  }
  const metadata = Object.create(null) as Record<string, string | number | boolean | null>;
  for (const key of keys) {
    requiredString(key, "audit metadata key", SECURITY_STORE_LIMITS.metadataKey);
    const entry = source[key];
    if (typeof entry === "string") {
      assertStringWithinLimit(entry, "audit metadata string", SECURITY_STORE_LIMITS.metadataString);
      if (hasUnsafeStringCharacter(entry)) throw new TypeError("audit metadata string is unsafe");
      metadata[key] = entry;
    } else if (typeof entry === "number") {
      metadata[key] = finiteNumber(entry, "audit metadata number");
    } else if (typeof entry === "boolean" || entry === null) {
      metadata[key] = entry;
    } else {
      throw new TypeError("audit metadata values must be scalar");
    }
  }
  return freeze(metadata);
}

function pendingAuditOutbox(eventId: string, nextAttemptAt: unknown): SecurityAuditOutboxRecord {
  return freeze({
    eventId: requiredString(eventId, "event id"),
    status: "pending",
    attempts: 0,
    nextAttemptAt: finiteNumber(nextAttemptAt, "outbox next attempt"),
  });
}

function validateBreakGlassSession(grant: BreakGlassGrant, session: SecuritySession): void {
  if (grant.revokedAt !== undefined || session.revokedAt !== undefined) {
    throw new TypeError("new break-glass access cannot already be revoked");
  }
  if (session.authMethod !== "break-glass") {
    throw new TypeError("break-glass session must use the break-glass auth method");
  }
  if (!samePrincipal(grant.principal, session.principal)) {
    throw new TypeError("break-glass session principal must match the grant");
  }
  if (grant.tokenHash !== session.tokenHash) {
    throw new TypeError("break-glass session token must match the grant");
  }
  if (session.createdAt < grant.createdAt || session.expiresAt > grant.expiresAt) {
    throw new TypeError("break-glass session lifetime must fit within the grant");
  }
}

function validateBreakGlassAudit(
  audit: SecurityAuditRecord,
  grant: BreakGlassGrant,
  event: "break-glass.created" | "break-glass.revoked",
): void {
  if (
    audit.event !== event ||
    audit.namespaceId !== ORG_WIDE_NAMESPACE ||
    audit.actor?.id !== grant.createdBy ||
    audit.effective === undefined ||
    !samePrincipal(grant.principal, audit.effective as SecurityPrincipalRef) ||
    audit.decision?.allowed !== true ||
    audit.metadata?.breakGlassGrantId !== grant.id
  ) {
    throw new TypeError("break-glass audit does not identify the grant and principal");
  }
}

function providerIdentityKey(identity: Omit<ProviderIdentity, "userId">): string {
  return JSON.stringify([identity.providerId, identity.issuer, identity.subject]);
}

function membershipKey(row: GroupMembership): string {
  return JSON.stringify([row.groupId, row.subject.kind, row.subject.id]);
}

function subjectBindingKey(row: SubjectAccessBinding): string {
  return JSON.stringify([row.subject.kind, row.subject.id, row.namespaceId, row.accessRole]);
}

function knowledgeAudienceKey(row: KnowledgeAudienceGrant): string {
  return JSON.stringify([row.namespaceId, row.baseId, row.audience.kind, row.audience.id]);
}

function samePrincipal(left: SecurityPrincipalRef, right: SecurityPrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function sameAccessSubject(left: AccessSubjectRef, right: AccessSubjectRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function freeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const nested of Object.values(value as Record<string, unknown>)) freeze(nested);
    Object.freeze(value);
  }
  return value;
}

function clone<T>(value: T): T {
  return freeze(structuredClone(value));
}

function recordsEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
