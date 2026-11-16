import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  check,
  foreignKey,
  index,
  integer,
  jsonb,
  pgSchema,
  primaryKey,
  text,
  uniqueIndex,
} from "drizzle-orm/pg-core";

const time = (name: string) => bigint(name, { mode: "number" });
export const SECURITY_DATA_SCHEMA = "zorya_security_data";
const securityData = pgSchema(SECURITY_DATA_SCHEMA);
const safeInteger = (column: unknown) =>
  sql`${column} BETWEEN -9007199254740991 AND 9007199254740991`;
const nonNegativeSafeInteger = (column: unknown) => sql`${column} BETWEEN 0 AND 9007199254740991`;
const textBoundary = (column: unknown, characters: number, bytes: number) =>
  sql`zorya_security_data.zsec_safe_text(${column}, ${characters}, ${bytes})`;

/** Collision-safe user/service-account/group namespace. */
export const securitySubjects = securityData.table(
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
    check("zsec_subject_id_ck", textBoundary(t.id, 256, 512)),
    check("zsec_subject_created_at_ck", safeInteger(t.createdAt)),
    check("zsec_subject_updated_at_ck", safeInteger(t.updatedAt)),
  ],
);

export const securityUsers = securityData.table(
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
      sql`${textBoundary(t.email, 320, 640)} AND ${t.email} = pg_catalog.lower(pg_catalog.btrim(${t.email})) AND pg_catalog.strpos(${t.email}, '@') > 0`,
    ),
    foreignKey({
      columns: [t.subjectKind, t.id],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_user_subject_fk",
    }).onDelete("cascade"),
  ],
);

export const securityProviderIdentities = securityData.table(
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
    check("zsec_provider_identity_provider_ck", textBoundary(t.providerId, 256, 512)),
    check("zsec_provider_identity_issuer_ck", textBoundary(t.issuer, 2048, 4096)),
    check("zsec_provider_identity_subject_ck", textBoundary(t.subject, 256, 512)),
    index("zsec_provider_identity_user_idx").on(t.userId),
  ],
);

export const securityServiceAccounts = securityData.table(
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

export const securityServiceAccountCredentials = securityData.table(
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
    check("zsec_service_account_credential_id_ck", textBoundary(t.id, 256, 512)),
    check("zsec_service_account_credential_hash_ck", textBoundary(t.secretHash, 512, 1024)),
    check("zsec_service_account_credential_created_at_ck", safeInteger(t.createdAt)),
    check("zsec_service_account_credential_expires_at_ck", safeInteger(t.expiresAt)),
    check("zsec_service_account_credential_revoked_at_ck", safeInteger(t.revokedAt)),
    check(
      "zsec_service_account_credential_expiry_ck",
      sql`${t.expiresAt} IS NULL OR ${t.expiresAt} > ${t.createdAt}`,
    ),
    index("zsec_service_account_credential_account_idx").on(t.serviceAccountId),
  ],
);

export const securityGroups = securityData.table(
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

export const securityGroupMemberships = securityData.table(
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

export const securityAccessBindings = securityData.table(
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
    check("zsec_access_binding_namespace_ck", textBoundary(t.namespaceId, 256, 512)),
    check("zsec_access_binding_role_ck", textBoundary(t.accessRole, 256, 512)),
    index("zsec_access_binding_scope_idx").on(t.namespaceId, t.accessRole),
  ],
);

export const securityKnowledgeAudienceGrants = securityData.table(
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
    check("zsec_knowledge_audience_namespace_ck", textBoundary(t.namespaceId, 256, 512)),
    check("zsec_knowledge_audience_base_ck", textBoundary(t.baseId, 256, 512)),
    check("zsec_knowledge_audience_id_ck", textBoundary(t.audienceId, 256, 512)),
    index("zsec_knowledge_audience_base_idx").on(t.namespaceId, t.baseId),
  ],
);

export const securitySessions = securityData.table(
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
    check("zsec_session_principal_kind_ck", sql`${t.principalKind} IN ('user', 'service-account')`),
    foreignKey({
      columns: [t.principalKind, t.principalId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_session_principal_fk",
    }).onDelete("restrict"),
    check(
      "zsec_session_auth_method_ck",
      sql`${t.authMethod} IN ('oidc', 'service-account', 'break-glass')`,
    ),
    check("zsec_session_hash_ck", textBoundary(t.tokenHash, 512, 1024)),
    check("zsec_session_csrf_hash_ck", textBoundary(t.csrfTokenHash, 512, 1024)),
    check("zsec_session_created_at_ck", safeInteger(t.createdAt)),
    check("zsec_session_expires_at_ck", safeInteger(t.expiresAt)),
    check("zsec_session_revoked_at_ck", safeInteger(t.revokedAt)),
    check("zsec_session_invalidated_at_ck", safeInteger(t.invalidatedAt)),
    check("zsec_session_expiry_ck", sql`${t.expiresAt} > ${t.createdAt}`),
    index("zsec_session_principal_idx").on(t.principalKind, t.principalId),
    index("zsec_session_expiry_idx").on(t.expiresAt),
  ],
);

export const securityAuditRecords = securityData.table(
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
    check("zsec_audit_record_namespace_ck", textBoundary(t.namespaceId, 256, 512)),
    check("zsec_audit_record_event_ck", textBoundary(t.event, 256, 512)),
    check("zsec_audit_record_hash_ck", textBoundary(t.hash, 512, 1024)),
    check("zsec_audit_record_id_ck", textBoundary(t.id, 256, 512)),
    check("zsec_audit_record_sequence_ck", nonNegativeSafeInteger(t.sequence)),
    check("zsec_audit_record_occurred_at_ck", safeInteger(t.occurredAt)),
    check("zsec_audit_record_recorded_at_ck", safeInteger(t.recordedAt)),
    check("zsec_audit_record_actor_id_ck", textBoundary(t.actorId, 256, 512)),
    check(
      "zsec_audit_record_actor_kind_ck",
      sql`${t.actorKind} IN ('operator', 'worker', 'user', 'service-account')`,
    ),
    check(
      "zsec_audit_record_actor_pair_ck",
      sql`(${t.actorKind} IS NULL) = (${t.actorId} IS NULL)`,
    ),
    check("zsec_audit_record_effective_id_ck", textBoundary(t.effectiveId, 256, 512)),
    check(
      "zsec_audit_record_effective_kind_ck",
      sql`${t.effectiveKind} IN ('operator', 'worker', 'user', 'service-account')`,
    ),
    check(
      "zsec_audit_record_effective_pair_ck",
      sql`(${t.effectiveKind} IS NULL) = (${t.effectiveId} IS NULL)`,
    ),
    check(
      "zsec_audit_record_principals_pair_ck",
      sql`(${t.actorKind} IS NULL) = (${t.effectiveKind} IS NULL)`,
    ),
    check("zsec_audit_record_permission_resource_ck", textBoundary(t.permissionResource, 256, 512)),
    check("zsec_audit_record_permission_action_ck", textBoundary(t.permissionAction, 256, 512)),
    check(
      "zsec_audit_record_permission_pair_ck",
      sql`(${t.permissionResource} IS NULL) = (${t.permissionAction} IS NULL)`,
    ),
    check(
      "zsec_audit_record_permission_value_ck",
      sql`zorya_security_data.zsec_valid_permission(${t.permissionResource}, ${t.permissionAction})`,
    ),
    check("zsec_audit_record_resource_kind_ck", textBoundary(t.resourceKind, 256, 512)),
    check(
      "zsec_audit_record_resource_value_ck",
      sql`zorya_security_data.zsec_valid_resource(${t.resourceKind})`,
    ),
    check("zsec_audit_record_resource_namespace_ck", textBoundary(t.resourceNamespaceId, 256, 512)),
    check(
      "zsec_audit_record_resource_pair_ck",
      sql`(${t.resourceKind} IS NULL) = (${t.resourceNamespaceId} IS NULL)`,
    ),
    check("zsec_audit_record_resource_id_ck", textBoundary(t.resourceId, 256, 512)),
    check(
      "zsec_audit_record_resource_principal_id_ck",
      textBoundary(t.resourcePrincipalId, 256, 512),
    ),
    check(
      "zsec_audit_record_resource_principal_kind_ck",
      sql`${t.resourcePrincipalKind} IN ('operator', 'worker', 'user', 'service-account')`,
    ),
    check(
      "zsec_audit_record_resource_principal_pair_ck",
      sql`(${t.resourcePrincipalKind} IS NULL) = (${t.resourcePrincipalId} IS NULL)`,
    ),
    check(
      "zsec_audit_record_resource_children_ck",
      sql`${t.resourceKind} IS NOT NULL OR (${t.resourceId} IS NULL AND ${t.resourcePrincipalKind} IS NULL)`,
    ),
    check("zsec_audit_record_decision_reason_ck", textBoundary(t.decisionReason, 256, 512)),
    check(
      "zsec_audit_record_decision_value_ck",
      sql`${t.decisionReason} IN ('operator_allow','access_role_allow','knowledge_grant_allow','self_identity_allow','permission_denied','namespace_denied','principal_disabled','identity_unknown','act_for_required','wrong_plane')`,
    ),
    check(
      "zsec_audit_record_decision_pair_ck",
      sql`(${t.decisionAllowed} IS NULL) = (${t.decisionReason} IS NULL)`,
    ),
    check("zsec_audit_record_request_id_ck", textBoundary(t.requestId, 256, 512)),
    check("zsec_audit_record_run_id_ck", textBoundary(t.runId, 256, 512)),
    check("zsec_audit_record_thread_id_ck", textBoundary(t.threadId, 256, 512)),
    check("zsec_audit_record_tool_call_id_ck", textBoundary(t.toolCallId, 256, 512)),
    check("zsec_audit_record_previous_hash_ck", textBoundary(t.previousHash, 512, 1024)),
    check("zsec_audit_record_key_id_ck", textBoundary(t.keyId, 256, 512)),
    check(
      "zsec_audit_record_metadata_ck",
      sql`zorya_security_data.zsec_valid_metadata(${t.metadata})`,
    ),
    check("zsec_audit_record_time_order_ck", sql`${t.recordedAt} >= ${t.occurredAt}`),
    index("zsec_audit_record_recorded_idx").on(t.namespaceId, t.recordedAt, t.id),
    index("zsec_audit_record_actor_idx").on(t.actorKind, t.actorId, t.recordedAt),
    index("zsec_audit_record_permission_idx").on(
      t.permissionResource,
      t.permissionAction,
      t.recordedAt,
    ),
  ],
);

export const securityAuditOutbox = securityData.table(
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
    check("zsec_audit_outbox_attempts_ck", sql`${t.attempts} >= 0`),
    check("zsec_audit_outbox_initial_next_attempt_at_ck", safeInteger(t.initialNextAttemptAt)),
    check("zsec_audit_outbox_next_attempt_at_ck", safeInteger(t.nextAttemptAt)),
    check("zsec_audit_outbox_lease_token_ck", textBoundary(t.leaseToken, 256, 512)),
    check("zsec_audit_outbox_lease_owner_ck", textBoundary(t.leaseOwner, 256, 512)),
    check("zsec_audit_outbox_lease_expires_at_ck", safeInteger(t.leaseExpiresAt)),
    check("zsec_audit_outbox_delivered_at_ck", safeInteger(t.deliveredAt)),
    check("zsec_audit_outbox_last_error_ck", textBoundary(t.lastError, 2048, 4096)),
    index("zsec_audit_outbox_due_idx").on(t.status, t.nextAttemptAt, t.eventId),
    index("zsec_audit_outbox_lease_idx").on(t.status, t.leaseExpiresAt),
  ],
);

export const securityDenialBuckets = securityData.table(
  "zsec_denial_bucket",
  {
    key: text("key").primaryKey(),
    windowStartedAt: time("window_started_at").notNull(),
    windowEndsAt: time("window_ends_at").notNull(),
    count: bigint("count", { mode: "number" }).notNull(),
    suppressed: bigint("suppressed", { mode: "number" }).notNull(),
  },
  (t) => [
    check("zsec_denial_bucket_key_ck", textBoundary(t.key, 256, 512)),
    check("zsec_denial_bucket_window_started_at_ck", safeInteger(t.windowStartedAt)),
    check("zsec_denial_bucket_window_ends_at_ck", safeInteger(t.windowEndsAt)),
    check("zsec_denial_bucket_count_ck", nonNegativeSafeInteger(t.count)),
    check("zsec_denial_bucket_suppressed_ck", nonNegativeSafeInteger(t.suppressed)),
    check("zsec_denial_bucket_window_order_ck", sql`${t.windowEndsAt} > ${t.windowStartedAt}`),
  ],
);

export const securityBreakGlassGrants = securityData.table(
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
    check(
      "zsec_break_glass_grant_principal_kind_ck",
      sql`${t.principalKind} IN ('user', 'service-account')`,
    ),
    foreignKey({
      columns: [t.principalKind, t.principalId],
      foreignColumns: [securitySubjects.kind, securitySubjects.id],
      name: "zsec_break_glass_grant_principal_fk",
    }).onDelete("restrict"),
    check("zsec_break_glass_reason_ck", textBoundary(t.reason, 2048, 4096)),
    check("zsec_break_glass_grant_id_ck", textBoundary(t.id, 256, 512)),
    check("zsec_break_glass_grant_token_ck", textBoundary(t.tokenHash, 512, 1024)),
    check("zsec_break_glass_grant_creator_ck", textBoundary(t.createdBy, 256, 512)),
    check("zsec_break_glass_grant_created_at_ck", safeInteger(t.createdAt)),
    check("zsec_break_glass_grant_expires_at_ck", safeInteger(t.expiresAt)),
    check("zsec_break_glass_grant_revoked_at_ck", safeInteger(t.revokedAt)),
    check("zsec_break_glass_grant_expiry_ck", sql`${t.expiresAt} > ${t.createdAt}`),
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
