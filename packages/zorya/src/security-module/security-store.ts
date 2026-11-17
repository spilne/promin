import type {
  AuthorizationDecision,
  PrincipalRef,
  SecurityPermission,
  SecurityResourceRef,
} from "../server/security/security.ts";

export interface SecurityStoreTextLimit {
  /** Unicode code points, not UTF-16 code units. */
  readonly maxCharacters: number;
  /** Bytes after UTF-8 encoding. */
  readonly maxBytes: number;
}

/**
 * Shared storage limits; PostgreSQL schemas and every store adapter must match these boundaries.
 * Stored text must also be well-formed Unicode so UTF-8 persistence is identity preserving.
 */
export const SECURITY_STORE_LIMITS = Object.freeze({
  identifier: Object.freeze({ maxCharacters: 256, maxBytes: 512 }),
  namespace: Object.freeze({ maxCharacters: 256, maxBytes: 512 }),
  issuer: Object.freeze({ maxCharacters: 2_048, maxBytes: 4_096 }),
  email: Object.freeze({ maxCharacters: 320, maxBytes: 640 }),
  hash: Object.freeze({ maxCharacters: 512, maxBytes: 1_024 }),
  reason: Object.freeze({ maxCharacters: 2_048, maxBytes: 4_096 }),
  event: Object.freeze({ maxCharacters: 256, maxBytes: 512 }),
  metadataKey: Object.freeze({ maxCharacters: 128, maxBytes: 256 }),
  metadataString: Object.freeze({ maxCharacters: 4_096, maxBytes: 8_192 }),
  metadataEntries: 64,
  initialBindings: 10_000,
  pageSize: 1_000,
  outboxBatch: 1_000,
} as const);

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

export type SecurityAuditScalar = string | number | boolean | null;

export interface SecurityAuditCorrelation {
  readonly requestId?: string;
  readonly runId?: string;
  readonly threadId?: string;
  readonly toolCallId?: string;
}

/** Canonical, structured and already-hashed audit-chain row. */
export interface SecurityAuditRecord {
  readonly id: string;
  readonly namespaceId: string;
  readonly sequence: number;
  readonly occurredAt: number;
  readonly recordedAt: number;
  readonly event: string;
  readonly actor?: PrincipalRef;
  readonly effective?: PrincipalRef;
  readonly permission?: SecurityPermission;
  readonly resource?: SecurityResourceRef;
  readonly decision?: AuthorizationDecision;
  readonly correlation?: SecurityAuditCorrelation;
  readonly metadata?: Readonly<Record<string, SecurityAuditScalar>>;
  readonly previousHash?: string;
  readonly hash: string;
  readonly keyId: string;
}

export interface SecurityAuditOutboxRecord {
  readonly eventId: string;
  readonly status: "pending" | "leased" | "delivered";
  readonly attempts: number;
  readonly nextAttemptAt: number;
  readonly leaseToken?: string;
  readonly leaseOwner?: string;
  readonly leaseExpiresAt?: number;
  readonly deliveredAt?: number;
  readonly lastError?: string;
}

export interface SecurityStorePage<T> {
  readonly items: ReadonlyArray<T>;
  /** Opaque stable cursor supplied unchanged to the next request. */
  readonly nextCursor?: string;
}

export interface SecurityDenialBucket {
  readonly key: string;
  readonly windowStartedAt: number;
  readonly windowEndsAt: number;
  readonly count: number;
  readonly suppressed: number;
}

export interface BreakGlassGrant {
  readonly id: string;
  readonly principal: SecurityPrincipalRef;
  readonly tokenHash: string;
  readonly reason: string;
  readonly createdBy: string;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly revokedAt?: number;
}

export interface SecurityStore {
  /** Authoritative clock for durable timestamps and expiries. */
  currentTimeMs(): Promise<number>;

  getUser(id: string): Promise<SecurityUser | undefined>;
  listUsers(): Promise<SecurityUser[]>;
  /** Disabling a user also invalidates every session belonging to that user. */
  setUserStatus(params: { id: string; status: SecurityUser["status"] }): Promise<void>;

  listBindings(userId: string): Promise<AccessBinding[]>;

  putKnowledgeGrant(grant: KnowledgeGrant): Promise<void>;
  removeKnowledgeGrant(grant: KnowledgeGrant): Promise<void>;

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
  /** Atomically creates an account and its first credential; returns false when the id exists. */
  createServiceAccountWithCredential(params: {
    readonly account: SecurityServiceAccount;
    readonly credential: ServiceAccountCredential;
  }): Promise<boolean>;
  getServiceAccount(id: string): Promise<SecurityServiceAccount | undefined>;
  listServiceAccounts(): Promise<SecurityServiceAccount[]>;
  putServiceAccountCredential(credential: ServiceAccountCredential): Promise<void>;
  getServiceAccountCredential(id: string): Promise<ServiceAccountCredential | undefined>;
  listServiceAccountCredentials(serviceAccountId: string): Promise<ServiceAccountCredential[]>;
  revokeServiceAccountCredential(params: {
    readonly id: string;
    readonly revokedAt: number;
  }): Promise<void>;

  /**
   * Insert-or-identical for an existing active principal; a token hash is never reassigned.
   * An exact retry of an already-revoked row is idempotent.
   */
  putSession(session: SecuritySession): Promise<void>;
  getSessionRecord(tokenHash: string): Promise<SecuritySession | undefined>;
  revokeSession(params: { readonly tokenHash: string; readonly revokedAt: number }): Promise<void>;

  /**
   * Atomically verify the chain head and append the row plus optional pending delivery.
   * An exact retry is idempotent; reuse of the event id with any conflicting field fails.
   */
  appendAuditWithOutbox(params: {
    readonly record: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"appended" | "already-exists">;
  /** Builds the next row after locking and reading the namespace head. */
  appendAuditFromHead(params: {
    readonly id: string;
    readonly namespaceId: string;
    readonly outbox?: { readonly nextAttemptAt: number };
    readonly build: (head: SecurityAuditRecord | undefined) => SecurityAuditRecord;
  }): Promise<{
    readonly result: "appended" | "already-exists";
    readonly record: SecurityAuditRecord;
  }>;
  getAuditRecord(eventId: string): Promise<SecurityAuditRecord | undefined>;
  /** Latest row in a namespace chain, obtained without paging its history. */
  getAuditHead(namespaceId: string): Promise<SecurityAuditRecord | undefined>;
  listAuditRecords(params: {
    readonly namespaceId: string;
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<SecurityStorePage<SecurityAuditRecord>>;
  getAuditOutbox(eventId: string): Promise<SecurityAuditOutboxRecord | undefined>;
  listAuditOutbox(params?: {
    readonly status?: SecurityAuditOutboxRecord["status"];
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<SecurityStorePage<SecurityAuditOutboxRecord>>;
  /** Atomically leases rows due at the store/DB clock and rows whose prior lease expired. */
  claimAuditOutbox(params: {
    readonly owner: string;
    readonly leaseMs: number;
    readonly limit: number;
  }): Promise<ReadonlyArray<SecurityAuditOutboxRecord>>;
  /** Acknowledges only a currently live lease, timestamped by the store/DB clock. */
  ackAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
  }): Promise<boolean>;
  /** Releases only a currently live lease; an expired token is always fenced out. */
  releaseAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
    readonly nextAttemptAt: number;
    readonly lastError?: string;
  }): Promise<boolean>;

  /** Atomically reset an elapsed window or increment its monotonic denial/suppression counts. */
  incrementDenialBucket(params: {
    readonly key: string;
    readonly now: number;
    readonly windowMs: number;
    readonly emitLimit: number;
  }): Promise<{
    readonly bucket: SecurityDenialBucket;
    readonly shouldEmit: boolean;
    /** The elapsed window replaced by this increment, for one deterministic summary emission. */
    readonly closedBucket?: SecurityDenialBucket;
  }>;
  getDenialBucket(key: string): Promise<SecurityDenialBucket | undefined>;

  /** Atomically creates one grant, its same-token session, audit row and optional outbox row. */
  createBreakGlassAccess(params: {
    readonly grant: BreakGlassGrant;
    readonly session: SecuritySession;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"created" | "already-exists">;
  getBreakGlassGrant(id: string): Promise<BreakGlassGrant | undefined>;
  /** Atomically revokes the grant/session pair and appends its audit/outbox record. */
  revokeBreakGlassAccess(params: {
    readonly grantId: string;
    readonly revokedAt: number;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"revoked" | "already-revoked">;
}
