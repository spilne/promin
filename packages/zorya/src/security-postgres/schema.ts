import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  check,
  foreignKey,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  text,
  uniqueIndex,
} from "drizzle-orm/pg-core";

const time = (name: string) => bigint(name, { mode: "number" });

/** Collision-safe user/service-account/group namespace. */
export const securitySubjects = pgTable(
  "zsec_subject",
  {
    kind: text("kind").notNull(),
    id: text("id").notNull(),
    status: text("status").notNull(),
    createdAt: time("created_at").notNull(),
    updatedAt: time("updated_at").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.kind, t.id] }),
    check("zsec_subject_kind_ck", sql`${t.kind} IN ('user', 'service-account', 'group')`),
    check("zsec_subject_status_ck", sql`${t.status} IN ('active', 'disabled')`),
    check(
      "zsec_subject_id_ck",
      sql`char_length(${t.id}) BETWEEN 1 AND 256 AND octet_length(${t.id}) <= 512`,
    ),
  ],
);

export const securityUsers = pgTable(
  "zsec_user",
  {
    subjectKind: text("subject_kind").notNull().default("user"),
    id: text("id").primaryKey(),
    email: text("email").notNull(),
  },
  (t) => [
    check("zsec_user_kind_ck", sql`${t.subjectKind} = 'user'`),
    check(
      "zsec_user_email_ck",
      sql`char_length(${t.email}) BETWEEN 1 AND 320 AND octet_length(${t.email}) <= 640`,
    ),
    foreignKey({
      columns: [t.subjectKind, t.id],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_user_subject_fk",
    }).onDelete("cascade"),
  ],
);

export const securityProviderIdentities = pgTable(
  "zsec_provider_identity",
  {
    providerId: text("provider_id").notNull(),
    issuer: text("issuer").notNull(),
    subject: text("subject").notNull(),
    userId: text("user_id")
      .notNull()
      .references(() => securityUsers.id, { onDelete: "cascade" }),
  },
  (t) => [
    primaryKey({ columns: [t.providerId, t.issuer, t.subject] }),
    check(
      "zsec_provider_identity_provider_ck",
      sql`char_length(${t.providerId}) BETWEEN 1 AND 256 AND octet_length(${t.providerId}) <= 512`,
    ),
    check(
      "zsec_provider_identity_issuer_ck",
      sql`char_length(${t.issuer}) BETWEEN 1 AND 2048 AND octet_length(${t.issuer}) <= 4096`,
    ),
    check(
      "zsec_provider_identity_subject_ck",
      sql`char_length(${t.subject}) BETWEEN 1 AND 256 AND octet_length(${t.subject}) <= 512`,
    ),
    index("zsec_provider_identity_user_idx").on(t.userId),
  ],
);

export const securityServiceAccounts = pgTable(
  "zsec_service_account",
  {
    subjectKind: text("subject_kind").notNull().default("service-account"),
    id: text("id").primaryKey(),
    canActFor: boolean("can_act_for").notNull().default(false),
  },
  (t) => [
    check("zsec_service_account_kind_ck", sql`${t.subjectKind} = 'service-account'`),
    foreignKey({
      columns: [t.subjectKind, t.id],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_service_account_subject_fk",
    }).onDelete("cascade"),
  ],
);

export const securityServiceAccountCredentials = pgTable(
  "zsec_service_account_credential",
  {
    id: text("id").primaryKey(),
    serviceAccountId: text("service_account_id")
      .notNull()
      .references(() => securityServiceAccounts.id, { onDelete: "cascade" }),
    secretHash: text("secret_hash").notNull(),
    createdAt: time("created_at").notNull(),
    expiresAt: time("expires_at"),
    revokedAt: time("revoked_at"),
  },
  (t) => [
    uniqueIndex("zsec_service_account_credential_hash_uq").on(t.secretHash),
    check(
      "zsec_service_account_credential_id_ck",
      sql`char_length(${t.id}) BETWEEN 1 AND 256 AND octet_length(${t.id}) <= 512`,
    ),
    check(
      "zsec_service_account_credential_hash_ck",
      sql`char_length(${t.secretHash}) BETWEEN 1 AND 512 AND octet_length(${t.secretHash}) <= 1024`,
    ),
    index("zsec_service_account_credential_account_idx").on(t.serviceAccountId),
  ],
);

export const securityGroups = pgTable(
  "zsec_access_group",
  {
    subjectKind: text("subject_kind").notNull().default("group"),
    id: text("id").primaryKey(),
  },
  (t) => [
    check("zsec_access_group_kind_ck", sql`${t.subjectKind} = 'group'`),
    foreignKey({
      columns: [t.subjectKind, t.id],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_access_group_subject_fk",
    }).onDelete("cascade"),
  ],
);

export const securityGroupMemberships = pgTable(
  "zsec_group_membership",
  {
    groupId: text("group_id")
      .notNull()
      .references(() => securityGroups.id, { onDelete: "cascade" }),
    subjectKind: text("subject_kind").notNull(),
    subjectId: text("subject_id").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.groupId, t.subjectKind, t.subjectId] }),
    check(
      "zsec_group_membership_subject_kind_ck",
      sql`${t.subjectKind} IN ('user', 'service-account')`,
    ),
    foreignKey({
      columns: [t.subjectKind, t.subjectId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_group_membership_subject_fk",
    }).onDelete("cascade"),
    index("zsec_group_membership_subject_idx").on(t.subjectKind, t.subjectId),
  ],
);

export const securityAccessBindings = pgTable(
  "zsec_access_binding",
  {
    subjectKind: text("subject_kind").notNull(),
    subjectId: text("subject_id").notNull(),
    namespaceId: text("namespace_id").notNull(),
    accessRole: text("access_role").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.subjectKind, t.subjectId, t.namespaceId, t.accessRole] }),
    foreignKey({
      columns: [t.subjectKind, t.subjectId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_access_binding_subject_fk",
    }).onDelete("cascade"),
    check(
      "zsec_access_binding_namespace_ck",
      sql`char_length(${t.namespaceId}) BETWEEN 1 AND 256 AND octet_length(${t.namespaceId}) <= 512`,
    ),
    check(
      "zsec_access_binding_role_ck",
      sql`char_length(${t.accessRole}) BETWEEN 1 AND 256 AND octet_length(${t.accessRole}) <= 512`,
    ),
    index("zsec_access_binding_scope_idx").on(t.namespaceId, t.accessRole),
  ],
);

export const securityKnowledgeAudienceGrants = pgTable(
  "zsec_knowledge_audience_grant",
  {
    namespaceId: text("namespace_id").notNull(),
    baseId: text("base_id").notNull(),
    audienceKind: text("audience_kind").notNull(),
    audienceId: text("audience_id").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.namespaceId, t.baseId, t.audienceKind, t.audienceId] }),
    check("zsec_knowledge_audience_kind_ck", sql`${t.audienceKind} IN ('access-role', 'group')`),
    check(
      "zsec_knowledge_audience_namespace_ck",
      sql`char_length(${t.namespaceId}) BETWEEN 1 AND 256 AND octet_length(${t.namespaceId}) <= 512`,
    ),
    index("zsec_knowledge_audience_base_idx").on(t.namespaceId, t.baseId),
  ],
);

export const securitySessions = pgTable(
  "zsec_session",
  {
    tokenHash: text("token_hash").primaryKey(),
    principalKind: text("principal_kind").notNull(),
    principalId: text("principal_id").notNull(),
    createdAt: time("created_at").notNull(),
    expiresAt: time("expires_at").notNull(),
    csrfTokenHash: text("csrf_token_hash"),
    revokedAt: time("revoked_at"),
    invalidatedAt: time("invalidated_at"),
    authMethod: text("auth_method").notNull(),
  },
  (t) => [
    foreignKey({
      columns: [t.principalKind, t.principalId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_session_principal_fk",
    }).onDelete("restrict"),
    check(
      "zsec_session_auth_method_ck",
      sql`${t.authMethod} IN ('oidc', 'service-account', 'break-glass')`,
    ),
    check(
      "zsec_session_hash_ck",
      sql`char_length(${t.tokenHash}) BETWEEN 1 AND 512 AND octet_length(${t.tokenHash}) <= 1024`,
    ),
    index("zsec_session_principal_idx").on(t.principalKind, t.principalId),
    index("zsec_session_expiry_idx").on(t.expiresAt),
  ],
);

export const securityAuditRecords = pgTable(
  "zsec_audit_record",
  {
    id: text("id").primaryKey(),
    namespaceId: text("namespace_id").notNull(),
    sequence: bigint("sequence", { mode: "number" }).notNull(),
    occurredAt: time("occurred_at").notNull(),
    recordedAt: time("recorded_at").notNull(),
    event: text("event").notNull(),
    actorKind: text("actor_kind"),
    actorId: text("actor_id"),
    effectiveKind: text("effective_kind"),
    effectiveId: text("effective_id"),
    permissionResource: text("permission_resource"),
    permissionAction: text("permission_action"),
    resourceKind: text("resource_kind"),
    resourceNamespaceId: text("resource_namespace_id"),
    resourceId: text("resource_id"),
    resourcePrincipalKind: text("resource_principal_kind"),
    resourcePrincipalId: text("resource_principal_id"),
    decisionAllowed: boolean("decision_allowed"),
    decisionReason: text("decision_reason"),
    requestId: text("request_id"),
    runId: text("run_id"),
    threadId: text("thread_id"),
    toolCallId: text("tool_call_id"),
    metadata: jsonb("metadata").$type<Readonly<Record<string, string | number | boolean | null>>>(),
    previousHash: text("previous_hash"),
    hash: text("hash").notNull(),
    keyId: text("key_id").notNull(),
  },
  (t) => [
    uniqueIndex("zsec_audit_record_chain_uq").on(t.namespaceId, t.sequence),
    check(
      "zsec_audit_record_namespace_ck",
      sql`char_length(${t.namespaceId}) BETWEEN 1 AND 256 AND octet_length(${t.namespaceId}) <= 512`,
    ),
    check(
      "zsec_audit_record_event_ck",
      sql`char_length(${t.event}) BETWEEN 1 AND 256 AND octet_length(${t.event}) <= 512`,
    ),
    check(
      "zsec_audit_record_hash_ck",
      sql`char_length(${t.hash}) BETWEEN 1 AND 512 AND octet_length(${t.hash}) <= 1024`,
    ),
    index("zsec_audit_record_recorded_idx").on(t.namespaceId, t.recordedAt, t.id),
    index("zsec_audit_record_actor_idx").on(t.actorKind, t.actorId, t.recordedAt),
    index("zsec_audit_record_permission_idx").on(
      t.permissionResource,
      t.permissionAction,
      t.recordedAt,
    ),
  ],
);

export const securityAuditOutbox = pgTable(
  "zsec_audit_outbox",
  {
    eventId: text("event_id")
      .primaryKey()
      .references(() => securityAuditRecords.id, { onDelete: "cascade" }),
    status: text("status").notNull(),
    attempts: integer("attempts").notNull().default(0),
    initialNextAttemptAt: time("initial_next_attempt_at").notNull(),
    nextAttemptAt: time("next_attempt_at").notNull(),
    leaseToken: text("lease_token"),
    leaseOwner: text("lease_owner"),
    leaseExpiresAt: time("lease_expires_at"),
    deliveredAt: time("delivered_at"),
    lastError: text("last_error"),
  },
  (t) => [
    check("zsec_audit_outbox_status_ck", sql`${t.status} IN ('pending', 'leased', 'delivered')`),
    index("zsec_audit_outbox_due_idx").on(t.status, t.nextAttemptAt, t.eventId),
    index("zsec_audit_outbox_lease_idx").on(t.status, t.leaseExpiresAt),
  ],
);

export const securityDenialBuckets = pgTable("zsec_denial_bucket", {
  key: text("key").primaryKey(),
  windowStartedAt: time("window_started_at").notNull(),
  windowEndsAt: time("window_ends_at").notNull(),
  count: bigint("count", { mode: "number" }).notNull(),
  suppressed: bigint("suppressed", { mode: "number" }).notNull(),
});

export const securityBreakGlassGrants = pgTable(
  "zsec_break_glass_grant",
  {
    id: text("id").primaryKey(),
    principalKind: text("principal_kind").notNull(),
    principalId: text("principal_id").notNull(),
    tokenHash: text("token_hash").notNull(),
    reason: text("reason").notNull(),
    createdBy: text("created_by").notNull(),
    createdAt: time("created_at").notNull(),
    expiresAt: time("expires_at").notNull(),
    revokedAt: time("revoked_at"),
  },
  (t) => [
    uniqueIndex("zsec_break_glass_grant_token_uq").on(t.tokenHash),
    foreignKey({
      columns: [t.principalKind, t.principalId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_break_glass_grant_principal_fk",
    }).onDelete("restrict"),
    check(
      "zsec_break_glass_reason_ck",
      sql`char_length(${t.reason}) BETWEEN 1 AND 2048 AND octet_length(${t.reason}) <= 4096`,
    ),
    index("zsec_break_glass_grant_expiry_idx").on(t.expiresAt),
  ],
);

export const SECURITY_POSTGRES_SCHEMA = Object.freeze({
  securitySubjects,
  securityUsers,
  securityProviderIdentities,
  securityServiceAccounts,
  securityServiceAccountCredentials,
  securityGroups,
  securityGroupMemberships,
  securityAccessBindings,
  securityKnowledgeAudienceGrants,
  securitySessions,
  securityAuditRecords,
  securityAuditOutbox,
  securityDenialBuckets,
  securityBreakGlassGrants,
});
