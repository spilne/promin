-- Optional enterprise-security persistence. These tables intentionally use a
-- separate prefix and migration history from the workflow/agent stores.

CREATE TABLE zsec_subject (
  kind text NOT NULL,
  id text NOT NULL,
  status text NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  CONSTRAINT zsec_subject_pkey PRIMARY KEY (kind, id),
  CONSTRAINT zsec_subject_kind_ck CHECK (kind IN ('user', 'service-account', 'group')),
  CONSTRAINT zsec_subject_status_ck CHECK (status IN ('active', 'disabled')),
  CONSTRAINT zsec_subject_id_ck CHECK (char_length(id) BETWEEN 1 AND 256 AND octet_length(id) <= 512)
);

CREATE TABLE zsec_user (
  subject_kind text DEFAULT 'user' NOT NULL,
  id text PRIMARY KEY,
  email text NOT NULL,
  CONSTRAINT zsec_user_kind_ck CHECK (subject_kind = 'user'),
  CONSTRAINT zsec_user_email_ck CHECK (char_length(email) BETWEEN 1 AND 320 AND octet_length(email) <= 640),
  CONSTRAINT zsec_user_subject_fk FOREIGN KEY (subject_kind, id)
    REFERENCES zsec_subject(kind, id) ON DELETE CASCADE
);

CREATE TABLE zsec_provider_identity (
  provider_id text NOT NULL,
  issuer text NOT NULL,
  subject text NOT NULL,
  user_id text NOT NULL REFERENCES zsec_user(id) ON DELETE CASCADE,
  CONSTRAINT zsec_provider_identity_pkey PRIMARY KEY (provider_id, issuer, subject),
  CONSTRAINT zsec_provider_identity_provider_ck CHECK (char_length(provider_id) BETWEEN 1 AND 256 AND octet_length(provider_id) <= 512),
  CONSTRAINT zsec_provider_identity_issuer_ck CHECK (char_length(issuer) BETWEEN 1 AND 2048 AND octet_length(issuer) <= 4096),
  CONSTRAINT zsec_provider_identity_subject_ck CHECK (char_length(subject) BETWEEN 1 AND 256 AND octet_length(subject) <= 512)
);
CREATE INDEX zsec_provider_identity_user_idx ON zsec_provider_identity(user_id);

CREATE TABLE zsec_service_account (
  subject_kind text DEFAULT 'service-account' NOT NULL,
  id text PRIMARY KEY,
  can_act_for boolean DEFAULT false NOT NULL,
  CONSTRAINT zsec_service_account_kind_ck CHECK (subject_kind = 'service-account'),
  CONSTRAINT zsec_service_account_subject_fk FOREIGN KEY (subject_kind, id)
    REFERENCES zsec_subject(kind, id) ON DELETE CASCADE
);

CREATE TABLE zsec_service_account_credential (
  id text PRIMARY KEY,
  service_account_id text NOT NULL REFERENCES zsec_service_account(id) ON DELETE CASCADE,
  secret_hash text NOT NULL,
  created_at bigint NOT NULL,
  expires_at bigint,
  revoked_at bigint,
  CONSTRAINT zsec_service_account_credential_id_ck CHECK (char_length(id) BETWEEN 1 AND 256 AND octet_length(id) <= 512),
  CONSTRAINT zsec_service_account_credential_hash_ck CHECK (char_length(secret_hash) BETWEEN 1 AND 512 AND octet_length(secret_hash) <= 1024)
);
CREATE INDEX zsec_service_account_credential_account_idx
  ON zsec_service_account_credential(service_account_id);
CREATE UNIQUE INDEX zsec_service_account_credential_hash_uq
  ON zsec_service_account_credential(secret_hash);

CREATE TABLE zsec_access_group (
  subject_kind text DEFAULT 'group' NOT NULL,
  id text PRIMARY KEY,
  CONSTRAINT zsec_access_group_kind_ck CHECK (subject_kind = 'group'),
  CONSTRAINT zsec_access_group_subject_fk FOREIGN KEY (subject_kind, id)
    REFERENCES zsec_subject(kind, id) ON DELETE CASCADE
);

CREATE TABLE zsec_group_membership (
  group_id text NOT NULL REFERENCES zsec_access_group(id) ON DELETE CASCADE,
  subject_kind text NOT NULL,
  subject_id text NOT NULL,
  CONSTRAINT zsec_group_membership_pkey PRIMARY KEY (group_id, subject_kind, subject_id),
  CONSTRAINT zsec_group_membership_subject_kind_ck CHECK (subject_kind IN ('user', 'service-account')),
  CONSTRAINT zsec_group_membership_subject_fk FOREIGN KEY (subject_kind, subject_id)
    REFERENCES zsec_subject(kind, id) ON DELETE CASCADE
);
CREATE INDEX zsec_group_membership_subject_idx
  ON zsec_group_membership(subject_kind, subject_id);

CREATE TABLE zsec_access_binding (
  subject_kind text NOT NULL,
  subject_id text NOT NULL,
  namespace_id text NOT NULL,
  access_role text NOT NULL,
  CONSTRAINT zsec_access_binding_pkey PRIMARY KEY (subject_kind, subject_id, namespace_id, access_role),
  CONSTRAINT zsec_access_binding_subject_fk FOREIGN KEY (subject_kind, subject_id)
    REFERENCES zsec_subject(kind, id) ON DELETE CASCADE,
  CONSTRAINT zsec_access_binding_namespace_ck CHECK (char_length(namespace_id) BETWEEN 1 AND 256 AND octet_length(namespace_id) <= 512),
  CONSTRAINT zsec_access_binding_role_ck CHECK (char_length(access_role) BETWEEN 1 AND 256 AND octet_length(access_role) <= 512)
);
CREATE INDEX zsec_access_binding_scope_idx ON zsec_access_binding(namespace_id, access_role);

CREATE TABLE zsec_knowledge_audience_grant (
  namespace_id text NOT NULL,
  base_id text NOT NULL,
  audience_kind text NOT NULL,
  audience_id text NOT NULL,
  CONSTRAINT zsec_knowledge_audience_grant_pkey
    PRIMARY KEY (namespace_id, base_id, audience_kind, audience_id),
  CONSTRAINT zsec_knowledge_audience_kind_ck CHECK (audience_kind IN ('access-role', 'group')),
  CONSTRAINT zsec_knowledge_audience_namespace_ck CHECK (char_length(namespace_id) BETWEEN 1 AND 256 AND octet_length(namespace_id) <= 512)
);
CREATE INDEX zsec_knowledge_audience_base_idx
  ON zsec_knowledge_audience_grant(namespace_id, base_id);

CREATE TABLE zsec_session (
  token_hash text PRIMARY KEY,
  principal_kind text NOT NULL,
  principal_id text NOT NULL,
  created_at bigint NOT NULL,
  expires_at bigint NOT NULL,
  csrf_token_hash text,
  revoked_at bigint,
  invalidated_at bigint,
  auth_method text NOT NULL,
  CONSTRAINT zsec_session_principal_fk FOREIGN KEY (principal_kind, principal_id)
    REFERENCES zsec_subject(kind, id) ON DELETE RESTRICT,
  CONSTRAINT zsec_session_auth_method_ck CHECK (auth_method IN ('oidc', 'service-account', 'break-glass')),
  CONSTRAINT zsec_session_hash_ck CHECK (char_length(token_hash) BETWEEN 1 AND 512 AND octet_length(token_hash) <= 1024)
);
CREATE INDEX zsec_session_principal_idx ON zsec_session(principal_kind, principal_id);
CREATE INDEX zsec_session_expiry_idx ON zsec_session(expires_at);

CREATE TABLE zsec_audit_record (
  id text PRIMARY KEY,
  namespace_id text NOT NULL,
  sequence bigint NOT NULL,
  occurred_at bigint NOT NULL,
  recorded_at bigint NOT NULL,
  event text NOT NULL,
  actor_kind text,
  actor_id text,
  effective_kind text,
  effective_id text,
  permission_resource text,
  permission_action text,
  resource_kind text,
  resource_namespace_id text,
  resource_id text,
  resource_principal_kind text,
  resource_principal_id text,
  decision_allowed boolean,
  decision_reason text,
  request_id text,
  run_id text,
  thread_id text,
  tool_call_id text,
  metadata jsonb,
  previous_hash text,
  hash text NOT NULL,
  key_id text NOT NULL,
  CONSTRAINT zsec_audit_record_namespace_ck CHECK (char_length(namespace_id) BETWEEN 1 AND 256 AND octet_length(namespace_id) <= 512),
  CONSTRAINT zsec_audit_record_event_ck CHECK (char_length(event) BETWEEN 1 AND 256 AND octet_length(event) <= 512),
  CONSTRAINT zsec_audit_record_hash_ck CHECK (char_length(hash) BETWEEN 1 AND 512 AND octet_length(hash) <= 1024)
);
CREATE INDEX zsec_audit_record_recorded_idx
  ON zsec_audit_record(namespace_id, recorded_at, id);
CREATE UNIQUE INDEX zsec_audit_record_chain_uq
  ON zsec_audit_record(namespace_id, sequence);
CREATE INDEX zsec_audit_record_actor_idx
  ON zsec_audit_record(actor_kind, actor_id, recorded_at);
CREATE INDEX zsec_audit_record_permission_idx
  ON zsec_audit_record(permission_resource, permission_action, recorded_at);

CREATE TABLE zsec_audit_outbox (
  event_id text PRIMARY KEY REFERENCES zsec_audit_record(id) ON DELETE CASCADE,
  status text NOT NULL,
  attempts integer DEFAULT 0 NOT NULL,
  initial_next_attempt_at bigint NOT NULL,
  next_attempt_at bigint NOT NULL,
  lease_token text,
  lease_owner text,
  lease_expires_at bigint,
  delivered_at bigint,
  last_error text,
  CONSTRAINT zsec_audit_outbox_status_ck CHECK (status IN ('pending', 'leased', 'delivered'))
);
CREATE INDEX zsec_audit_outbox_due_idx
  ON zsec_audit_outbox(status, next_attempt_at, event_id);
CREATE INDEX zsec_audit_outbox_lease_idx
  ON zsec_audit_outbox(status, lease_expires_at);

CREATE TABLE zsec_denial_bucket (
  key text PRIMARY KEY,
  window_started_at bigint NOT NULL,
  window_ends_at bigint NOT NULL,
  count bigint NOT NULL,
  suppressed bigint NOT NULL
);

CREATE TABLE zsec_break_glass_grant (
  id text PRIMARY KEY,
  principal_kind text NOT NULL,
  principal_id text NOT NULL,
  token_hash text NOT NULL,
  reason text NOT NULL,
  created_by text NOT NULL,
  created_at bigint NOT NULL,
  expires_at bigint NOT NULL,
  revoked_at bigint,
  CONSTRAINT zsec_break_glass_grant_principal_fk FOREIGN KEY (principal_kind, principal_id)
    REFERENCES zsec_subject(kind, id) ON DELETE RESTRICT,
  CONSTRAINT zsec_break_glass_reason_ck CHECK (char_length(reason) BETWEEN 1 AND 2048 AND octet_length(reason) <= 4096)
);
CREATE INDEX zsec_break_glass_grant_expiry_idx ON zsec_break_glass_grant(expires_at);
CREATE UNIQUE INDEX zsec_break_glass_grant_token_uq ON zsec_break_glass_grant(token_hash);
