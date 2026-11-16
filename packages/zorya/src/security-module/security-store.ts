export interface SecurityUser {
  readonly id: string;
  /** Canonical lower-case email address. */
  readonly email: string;
  readonly status: "active" | "disabled";
}

export interface AccessBinding {
  readonly userId: string;
  readonly namespaceId: string;
  readonly accessRole: string;
}

export interface KnowledgeGrant {
  readonly namespaceId: string;
  readonly baseId: string;
  readonly accessRole: string;
}

/** Collision-safe identity used by durable access-control records. */
export interface SecurityPrincipalRef {
  readonly kind: "user" | "service-account";
  readonly id: string;
}

export type AccessSubjectRef =
  | SecurityPrincipalRef
  | { readonly kind: "group"; readonly id: string };

export interface ProviderIdentity {
  readonly providerId: string;
  readonly issuer: string;
  readonly subject: string;
  readonly userId: string;
}

export interface AccessGroup {
  readonly id: string;
  readonly status: "active" | "disabled";
}

export interface GroupMembership {
  readonly groupId: string;
  readonly subject: SecurityPrincipalRef;
}

export interface SubjectAccessBinding {
  readonly subject: AccessSubjectRef;
  readonly namespaceId: string;
  readonly accessRole: string;
}

export interface KnowledgeAudienceGrant {
  readonly namespaceId: string;
  readonly baseId: string;
  readonly audience:
    | { readonly kind: "access-role"; readonly id: string }
    | { readonly kind: "group"; readonly id: string };
}

export interface SecurityServiceAccount {
  readonly id: string;
  readonly status: "active" | "disabled";
  readonly canActFor: boolean;
}

/** Only a host-peppered digest is stored; plaintext credentials never enter this contract. */
export interface ServiceAccountCredential {
  readonly id: string;
  readonly serviceAccountId: string;
  readonly secretHash: string;
  readonly createdAt: number;
  readonly expiresAt?: number;
  readonly revokedAt?: number;
}

export interface SecuritySession {
  readonly tokenHash: string;
  readonly principal: SecurityPrincipalRef;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly csrfTokenHash?: string;
  readonly revokedAt?: number;
  readonly authMethod: "oidc" | "service-account" | "break-glass";
}

/** Canonical, already-hashed audit-chain row. PostgreSQL will verify/append atomically. */
export interface SecurityAuditRecord {
  readonly id: string;
  readonly namespaceId: string;
  readonly sequence: number;
  readonly occurredAt: number;
  readonly eventJson: string;
  readonly previousHash?: string;
  readonly hash: string;
  readonly keyId: string;
}

export interface SecurityAuditOutboxRecord {
  readonly eventId: string;
  readonly status: "pending" | "delivered";
  readonly attempts: number;
  readonly nextAttemptAt: number;
}

export interface SecurityDenialBucket {
  readonly key: string;
  readonly windowStartedAt: number;
  readonly count: number;
  readonly suppressed: number;
}

export interface BreakGlassGrant {
  readonly id: string;
  readonly tokenHash: string;
  readonly reason: string;
  readonly createdBy: string;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly revokedAt?: number;
}

export interface SecurityStore {
  /** Atomically find the email's user or create it with the initial bindings. */
  findOrCreateUser(params: {
    readonly email: string;
    readonly bindings: ReadonlyArray<Omit<AccessBinding, "userId">>;
  }): Promise<{ user: SecurityUser; created: boolean }>;
  getUser(id: string): Promise<SecurityUser | undefined>;
  listUsers(): Promise<SecurityUser[]>;
  /** Disabling a user also invalidates every session belonging to that user. */
  setUserStatus(params: { id: string; status: SecurityUser["status"] }): Promise<void>;

  listBindings(userId: string): Promise<AccessBinding[]>;
  putBinding(binding: AccessBinding): Promise<void>;
  removeBinding(binding: AccessBinding): Promise<void>;

  putKnowledgeGrant(grant: KnowledgeGrant): Promise<void>;
  removeKnowledgeGrant(grant: KnowledgeGrant): Promise<void>;
  /** Grants on this base in the requested namespace and the org-wide namespace. */
  listKnowledgeGrants(params: { namespaceId: string; baseId: string }): Promise<KnowledgeGrant[]>;

  createSession(params: { tokenHash: string; userId: string; ttlMs: number }): Promise<void>;
  /** The session owner, or undefined when the session is absent or expired. */
  getSession(tokenHash: string): Promise<{ userId: string } | undefined>;
  deleteSession(tokenHash: string): Promise<void>;
}

/**
 * Production-capable security persistence.  The legacy `SecurityStore`
 * methods remain while the module is migrated; new stores implement this
 * complete contract and may adapt the legacy user-only calls.
 */
export interface SecurityStoreV2 extends SecurityStore {
  findOrCreateProviderUser(params: {
    readonly identity: Omit<ProviderIdentity, "userId">;
    readonly email: string;
    readonly bindings?: ReadonlyArray<Omit<SubjectAccessBinding, "subject">>;
  }): Promise<{
    readonly user: SecurityUser;
    readonly identity: ProviderIdentity;
    readonly created: boolean;
  }>;
  getProviderIdentity(params: {
    readonly providerId: string;
    readonly issuer: string;
    readonly subject: string;
  }): Promise<ProviderIdentity | undefined>;

  putGroup(group: AccessGroup): Promise<void>;
  getGroup(id: string): Promise<AccessGroup | undefined>;
  putGroupMembership(membership: GroupMembership): Promise<void>;
  removeGroupMembership(membership: GroupMembership): Promise<void>;
  listGroupsForSubject(subject: SecurityPrincipalRef): Promise<AccessGroup[]>;

  putSubjectBinding(binding: SubjectAccessBinding): Promise<void>;
  removeSubjectBinding(binding: SubjectAccessBinding): Promise<void>;
  listSubjectBindings(subject: AccessSubjectRef): Promise<SubjectAccessBinding[]>;
  resolveAccess(params: {
    readonly subject: SecurityPrincipalRef;
    readonly namespaceId: string;
  }): Promise<{
    readonly accessRoles: ReadonlyArray<string>;
    readonly groups: ReadonlyArray<string>;
  }>;

  putKnowledgeAudienceGrant(grant: KnowledgeAudienceGrant): Promise<void>;
  removeKnowledgeAudienceGrant(grant: KnowledgeAudienceGrant): Promise<void>;
  listKnowledgeAudienceGrants(params: {
    readonly namespaceId: string;
    readonly baseId: string;
  }): Promise<KnowledgeAudienceGrant[]>;

  putServiceAccount(account: SecurityServiceAccount): Promise<void>;
  getServiceAccount(id: string): Promise<SecurityServiceAccount | undefined>;
  listServiceAccounts(): Promise<SecurityServiceAccount[]>;
  putServiceAccountCredential(credential: ServiceAccountCredential): Promise<void>;
  getServiceAccountCredential(id: string): Promise<ServiceAccountCredential | undefined>;
  listServiceAccountCredentials(serviceAccountId: string): Promise<ServiceAccountCredential[]>;
  revokeServiceAccountCredential(params: {
    readonly id: string;
    readonly revokedAt: number;
  }): Promise<void>;

  putSession(session: SecuritySession): Promise<void>;
  getSessionRecord(tokenHash: string): Promise<SecuritySession | undefined>;
  revokeSession(params: { readonly tokenHash: string; readonly revokedAt: number }): Promise<void>;

  /** Append the chain row and optional delivery row atomically. */
  appendAuditRecord(record: SecurityAuditRecord, outbox?: SecurityAuditOutboxRecord): Promise<void>;
  listAuditRecords(namespaceId: string): Promise<SecurityAuditRecord[]>;
  putAuditOutbox(record: SecurityAuditOutboxRecord): Promise<void>;
  getAuditOutbox(eventId: string): Promise<SecurityAuditOutboxRecord | undefined>;
  putDenialBucket(bucket: SecurityDenialBucket): Promise<void>;
  getDenialBucket(key: string): Promise<SecurityDenialBucket | undefined>;

  putBreakGlassGrant(grant: BreakGlassGrant): Promise<void>;
  getBreakGlassGrant(id: string): Promise<BreakGlassGrant | undefined>;
  revokeBreakGlassGrant(params: { readonly id: string; readonly revokedAt: number }): Promise<void>;
}
