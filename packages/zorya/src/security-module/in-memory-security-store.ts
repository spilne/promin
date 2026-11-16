import { SystemWallClock, type WallClock } from "@promin/workflow";
import { ORG_WIDE_NAMESPACE } from "../server/security/index.ts";
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
  private readonly auditRecords = new Map<string, SecurityAuditRecord[]>();
  private readonly auditIds = new Set<string>();
  private readonly auditOutbox = new Map<string, SecurityAuditOutboxRecord>();
  private readonly denialBuckets = new Map<string, SecurityDenialBucket>();
  private readonly breakGlassGrants = new Map<string, BreakGlassGrant>();

  constructor(config: InMemorySecurityStoreConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async findOrCreateUser(params: {
    readonly email: string;
    readonly bindings: ReadonlyArray<Omit<AccessBinding, "userId">>;
  }): Promise<{ user: SecurityUser; created: boolean }> {
    const email = normalizeEmail(requiredString(params.email, "email"));
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
    if (status === "disabled") {
      for (const [tokenHash, session] of this.sessions) {
        if (session.principal.kind === "user" && session.principal.id === id) {
          this.sessions.delete(tokenHash);
        }
      }
    }
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
    await this.putSession({
      tokenHash: params.tokenHash,
      principal: { kind: "user", id: params.userId },
      createdAt: now,
      expiresAt: now + positiveNumber(params.ttlMs, "session ttl"),
      authMethod: "oidc",
    });
  }

  async getSession(tokenHash: string): Promise<{ userId: string } | undefined> {
    const session = await this.getSessionRecord(tokenHash);
    if (session === undefined) return undefined;
    if (session.principal.kind !== "user") return undefined;
    return freeze({ userId: session.principal.id });
  }

  async deleteSession(tokenHash: string): Promise<void> {
    this.sessions.delete(requiredString(tokenHash, "session token hash"));
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
      const email = normalizeEmail(requiredString(params.email, "email"));
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
    const email = normalizeEmail(requiredString(params.email, "email"));
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
    this.memberships.set(membershipKey(row), row);
  }

  async removeGroupMembership(membership: GroupMembership): Promise<void> {
    this.memberships.delete(membershipKey(normalizeMembership(membership)));
  }

  async listGroupsForSubject(subject: SecurityPrincipalRef): Promise<AccessGroup[]> {
    const normalized = normalizePrincipalRef(subject);
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
      for (const [tokenHash, session] of this.sessions) {
        if (session.principal.kind === "service-account" && session.principal.id === row.id) {
          this.sessions.delete(tokenHash);
        }
      }
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
    this.sessions.set(row.tokenHash, row);
  }

  async getSessionRecord(tokenHash: string): Promise<SecuritySession | undefined> {
    const key = requiredString(tokenHash, "session token hash");
    const row = this.sessions.get(key);
    if (!row) return undefined;
    if (row.revokedAt !== undefined || row.expiresAt <= this.clock.currentTimeMs()) {
      this.sessions.delete(key);
      return undefined;
    }
    return clone(row);
  }

  async revokeSession(params: {
    readonly tokenHash: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const key = requiredString(params.tokenHash, "session token hash");
    const existing = this.sessions.get(key);
    if (!existing) return;
    this.sessions.set(
      key,
      freeze({ ...existing, revokedAt: finiteNumber(params.revokedAt, "revoked at") }),
    );
  }

  async appendAuditRecord(
    record: SecurityAuditRecord,
    outbox?: SecurityAuditOutboxRecord,
  ): Promise<void> {
    const row = normalizeAuditRecord(record);
    const outboxRow = outbox === undefined ? undefined : normalizeAuditOutbox(outbox);
    if (outboxRow !== undefined && outboxRow.eventId !== row.id) {
      throw new Error("Audit outbox event must match the audit record");
    }
    if (this.auditIds.has(row.id)) throw new Error(`Duplicate audit event: ${row.id}`);
    if (outboxRow !== undefined && this.auditOutbox.has(outboxRow.eventId)) {
      throw new Error(`Duplicate audit outbox event: ${outboxRow.eventId}`);
    }
    const chain = this.auditRecords.get(row.namespaceId) ?? [];
    const expectedSequence = chain.length + 1;
    const expectedPrevious = chain.at(-1)?.hash;
    if (row.sequence !== expectedSequence || row.previousHash !== expectedPrevious) {
      throw new Error("Audit chain boundary mismatch");
    }
    this.auditIds.add(row.id);
    this.auditRecords.set(row.namespaceId, [...chain, row]);
    if (outboxRow !== undefined) this.auditOutbox.set(outboxRow.eventId, outboxRow);
  }

  async listAuditRecords(namespaceId: string): Promise<SecurityAuditRecord[]> {
    return (this.auditRecords.get(requiredString(namespaceId, "namespace id")) ?? []).map(clone);
  }

  async putAuditOutbox(record: SecurityAuditOutboxRecord): Promise<void> {
    const row = normalizeAuditOutbox(record);
    if (!this.auditIds.has(row.eventId)) throw new Error(`Unknown audit event: ${row.eventId}`);
    this.auditOutbox.set(row.eventId, row);
  }

  async getAuditOutbox(eventId: string): Promise<SecurityAuditOutboxRecord | undefined> {
    const row = this.auditOutbox.get(requiredString(eventId, "event id"));
    return row && clone(row);
  }

  async putDenialBucket(bucket: SecurityDenialBucket): Promise<void> {
    const row = normalizeDenialBucket(bucket);
    this.denialBuckets.set(row.key, row);
  }

  async getDenialBucket(key: string): Promise<SecurityDenialBucket | undefined> {
    const row = this.denialBuckets.get(requiredString(key, "denial bucket key"));
    return row && clone(row);
  }

  async putBreakGlassGrant(grant: BreakGlassGrant): Promise<void> {
    const row = normalizeBreakGlassGrant(grant);
    this.breakGlassGrants.set(row.id, row);
  }

  async getBreakGlassGrant(id: string): Promise<BreakGlassGrant | undefined> {
    const row = this.breakGlassGrants.get(requiredString(id, "break-glass grant id"));
    return row && clone(row);
  }

  async revokeBreakGlassGrant(params: {
    readonly id: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const id = requiredString(params.id, "break-glass grant id");
    const existing = this.breakGlassGrants.get(id);
    if (!existing) return;
    this.breakGlassGrants.set(
      id,
      freeze({ ...existing, revokedAt: finiteNumber(params.revokedAt, "revoked at") }),
    );
  }

  private storeSubjectBinding(row: SubjectAccessBinding): void {
    this.bindings.set(subjectBindingKey(row), freeze(row));
  }
}

function normalizeEmail(email: string): string {
  const normalized = email.trim().toLowerCase();
  if (!normalized.includes("@")) throw new TypeError("email must be an address");
  return normalized;
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0 || hasUnsafeStringCharacter(value)) {
    throw new TypeError(`${label} must be a non-empty safe string`);
  }
  return value;
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

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${label} must be a non-negative safe integer`);
  }
  return value;
}

function boundedArray<T>(value: ReadonlyArray<T>, label: string): ReadonlyArray<T> {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const length = value.length;
  if (!Number.isSafeInteger(length) || length > 10_000) {
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
    issuer: requiredString(issuer, "issuer"),
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
    secretHash: requiredString(secretHash, "credential hash"),
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
    tokenHash: requiredString(tokenHash, "session token hash"),
    principal: normalizePrincipalRef(principal as SecurityPrincipalRef),
    createdAt: finiteNumber(createdAt, "session created at"),
    expiresAt: finiteNumber(expiresAt, "session expires at"),
    ...(csrfTokenHash !== undefined && {
      csrfTokenHash: requiredString(csrfTokenHash, "csrf token hash"),
    }),
    ...(revokedAt !== undefined && { revokedAt: finiteNumber(revokedAt, "session revoked at") }),
    authMethod,
  };
  if (row.expiresAt <= row.createdAt) throw new TypeError("session expiry must follow creation");
  return freeze(row);
}

function normalizeAuditRecord(value: SecurityAuditRecord): SecurityAuditRecord {
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const namespaceId = source.namespaceId;
  const sequence = source.sequence;
  const occurredAt = source.occurredAt;
  const eventJson = source.eventJson;
  const previousHash = source.previousHash;
  const hash = source.hash;
  const keyId = source.keyId;
  return freeze({
    id: requiredString(id, "audit event id"),
    namespaceId: requiredString(namespaceId, "namespace id"),
    sequence: nonNegativeInteger(sequence, "audit sequence"),
    occurredAt: finiteNumber(occurredAt, "audit occurrence"),
    eventJson: requiredString(eventJson, "audit event json"),
    ...(previousHash !== undefined && {
      previousHash: requiredString(previousHash, "previous audit hash"),
    }),
    hash: requiredString(hash, "audit hash"),
    keyId: requiredString(keyId, "audit key id"),
  });
}

function normalizeAuditOutbox(value: SecurityAuditOutboxRecord): SecurityAuditOutboxRecord {
  const source = value as unknown as Record<string, unknown>;
  const eventId = source.eventId;
  const status = source.status;
  const attempts = source.attempts;
  const nextAttemptAt = source.nextAttemptAt;
  if (status !== "pending" && status !== "delivered") throw new TypeError("invalid outbox status");
  return freeze({
    eventId: requiredString(eventId, "event id"),
    status,
    attempts: nonNegativeInteger(attempts, "outbox attempts"),
    nextAttemptAt: finiteNumber(nextAttemptAt, "next attempt at"),
  });
}

function normalizeDenialBucket(value: SecurityDenialBucket): SecurityDenialBucket {
  const source = value as unknown as Record<string, unknown>;
  const key = source.key;
  const windowStartedAt = source.windowStartedAt;
  const count = source.count;
  const suppressed = source.suppressed;
  return freeze({
    key: requiredString(key, "denial bucket key"),
    windowStartedAt: finiteNumber(windowStartedAt, "denial window"),
    count: nonNegativeInteger(count, "denial count"),
    suppressed: nonNegativeInteger(suppressed, "denial suppressed count"),
  });
}

function normalizeBreakGlassGrant(value: BreakGlassGrant): BreakGlassGrant {
  const source = value as unknown as Record<string, unknown>;
  const id = source.id;
  const tokenHash = source.tokenHash;
  const reason = source.reason;
  const createdBy = source.createdBy;
  const createdAt = source.createdAt;
  const expiresAt = source.expiresAt;
  const revokedAt = source.revokedAt;
  const row: BreakGlassGrant = {
    id: requiredString(id, "break-glass grant id"),
    tokenHash: requiredString(tokenHash, "break-glass token hash"),
    reason: requiredString(reason, "break-glass reason"),
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
