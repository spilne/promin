CREATE SCHEMA IF NOT EXISTS zorya_security_data;

CREATE FUNCTION zorya_security_data.zsec_safe_text(value text, max_characters integer, max_bytes integer)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, zorya_security_data
AS $$
  SELECT char_length(value) BETWEEN 1 AND max_characters
    AND octet_length(value) <= max_bytes
    AND strpos(value, chr(127)) = 0
    AND strpos(value, chr(65279)) = 0
    AND NOT EXISTS (
      SELECT 1
      FROM (
        SELECT generate_series(1, 31) AS codepoint
        UNION ALL SELECT generate_series(8203, 8207)
        UNION ALL SELECT generate_series(8234, 8238)
        UNION ALL SELECT generate_series(8288, 8303)
      ) unsafe
      WHERE strpos(value, chr(unsafe.codepoint)) > 0
    );
$$;

CREATE FUNCTION zorya_security_data.zsec_valid_metadata(metadata_value jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, zorya_security_data
AS $$
DECLARE
  entry record;
  entries integer := 0;
  kind text;
BEGIN
  IF jsonb_typeof(metadata_value) <> 'object' THEN
    RETURN false;
  END IF;
  FOR entry IN
    SELECT metadata_entry.key, metadata_entry.value
    FROM jsonb_each(metadata_value) AS metadata_entry(key, value)
  LOOP
    entries := entries + 1;
    IF entries > 64 OR NOT zorya_security_data.zsec_safe_text(entry.key, 128, 256) THEN
      RETURN false;
    END IF;
    kind := jsonb_typeof(entry.value);
    IF kind = 'string' THEN
      IF (entry.value #>> '{}') <> '' AND NOT zorya_security_data.zsec_safe_text(entry.value #>> '{}', 4096, 8192) THEN
        RETURN false;
      END IF;
    ELSIF kind = 'number' THEN
      IF abs((entry.value #>> '{}')::numeric) > 1.7976931348623157e308::numeric THEN
        RETURN false;
      END IF;
    ELSIF kind NOT IN ('boolean', 'null') THEN
      RETURN false;
    END IF;
  END LOOP;
  RETURN true;
END;
$$;

CREATE FUNCTION zorya_security_data.zsec_valid_resource(resource text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, zorya_security_data
AS $$
  SELECT resource IN (
    'namespace', 'user', 'service-account', 'agent', 'agent-role', 'skill', 'fragment',
    'toolset', 'thread', 'memory', 'approval', 'knowledge', 'connector', 'secret',
    'workflow', 'dag', 'run', 'schedule', 'eval', 'webhook', 'worker', 'audit', 'usage'
  );
$$;

CREATE FUNCTION zorya_security_data.zsec_valid_permission(resource text, action text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, zorya_security_data
AS $$
  SELECT CASE resource
    WHEN 'namespace' THEN action = ANY (ARRAY['read','create','update','archive'])
    WHEN 'user' THEN action = ANY (ARRAY['read','disable','bind'])
    WHEN 'service-account' THEN action = ANY (ARRAY['read','create','rotate','delete'])
    WHEN 'agent' THEN action = ANY (ARRAY['read','create','update','promote','rollback','disable','invoke'])
    WHEN 'agent-role' THEN action = ANY (ARRAY['read','create','update','disable'])
    WHEN 'skill' THEN action = ANY (ARRAY['read','create','update','disable'])
    WHEN 'fragment' THEN action = ANY (ARRAY['read','create','update','disable'])
    WHEN 'toolset' THEN action = ANY (ARRAY['read','create','promote','rollback'])
    WHEN 'thread' THEN action = ANY (ARRAY['read','send','rename','archive','view-trace'])
    WHEN 'memory' THEN action = ANY (ARRAY['read','write','erase'])
    WHEN 'approval' THEN action = ANY (ARRAY['decide','decide-elevated'])
    WHEN 'knowledge' THEN action = ANY (ARRAY['read','create','update','delete','grant','use','inspect-text'])
    WHEN 'connector' THEN action = ANY (ARRAY['read','register','approve-tools','disable','remove'])
    WHEN 'secret' THEN action = ANY (ARRAY['list','write','delete'])
    WHEN 'workflow' THEN action = ANY (ARRAY['read','create','update'])
    WHEN 'dag' THEN action = ANY (ARRAY['read','create','update'])
    WHEN 'run' THEN action = ANY (ARRAY['read','trigger','cancel','signal'])
    WHEN 'schedule' THEN action = ANY (ARRAY['read','create','update','delete'])
    WHEN 'eval' THEN action = ANY (ARRAY['read','create','run'])
    WHEN 'webhook' THEN action = ANY (ARRAY['read','configure'])
    WHEN 'worker' THEN action = ANY (ARRAY['read','drain'])
    WHEN 'audit' THEN action = ANY (ARRAY['search','export'])
    WHEN 'usage' THEN action = 'read'
    ELSE false
  END;
$$;

ALTER TABLE public.zsec_subject SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_user SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_provider_identity SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_service_account SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_service_account_credential SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_access_group SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_group_membership SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_access_binding SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_knowledge_audience_grant SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_session SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_audit_record SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_audit_outbox SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_denial_bucket SET SCHEMA zorya_security_data;
ALTER TABLE public.zsec_break_glass_grant SET SCHEMA zorya_security_data;

ALTER TABLE zorya_security_data.zsec_subject
  ADD CONSTRAINT zsec_subject_id_safe_ck CHECK (zorya_security_data.zsec_safe_text(id, 256, 512)),
  ADD CONSTRAINT zsec_subject_created_at_ck CHECK (created_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_subject_updated_at_ck CHECK (updated_at BETWEEN -9007199254740991 AND 9007199254740991);

ALTER TABLE zorya_security_data.zsec_user
  ADD CONSTRAINT zsec_user_email_safe_ck CHECK (zorya_security_data.zsec_safe_text(email, 320, 640) AND email = lower(btrim(email)) AND strpos(email, '@') > 0);

ALTER TABLE zorya_security_data.zsec_provider_identity
  ADD CONSTRAINT zsec_provider_identity_provider_safe_ck CHECK (zorya_security_data.zsec_safe_text(provider_id, 256, 512)),
  ADD CONSTRAINT zsec_provider_identity_issuer_safe_ck CHECK (zorya_security_data.zsec_safe_text(issuer, 2048, 4096)),
  ADD CONSTRAINT zsec_provider_identity_subject_safe_ck CHECK (zorya_security_data.zsec_safe_text(subject, 256, 512));

ALTER TABLE zorya_security_data.zsec_service_account_credential
  ADD CONSTRAINT zsec_service_account_credential_id_safe_ck CHECK (zorya_security_data.zsec_safe_text(id, 256, 512)),
  ADD CONSTRAINT zsec_service_account_credential_hash_safe_ck CHECK (zorya_security_data.zsec_safe_text(secret_hash, 512, 1024)),
  ADD CONSTRAINT zsec_service_account_credential_created_at_ck CHECK (created_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_service_account_credential_expires_at_ck CHECK (expires_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_service_account_credential_revoked_at_ck CHECK (revoked_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_service_account_credential_expiry_ck CHECK (expires_at IS NULL OR expires_at > created_at);

ALTER TABLE zorya_security_data.zsec_access_binding
  ADD CONSTRAINT zsec_access_binding_namespace_safe_ck CHECK (zorya_security_data.zsec_safe_text(namespace_id, 256, 512)),
  ADD CONSTRAINT zsec_access_binding_role_safe_ck CHECK (zorya_security_data.zsec_safe_text(access_role, 256, 512));

ALTER TABLE zorya_security_data.zsec_knowledge_audience_grant
  ADD CONSTRAINT zsec_knowledge_audience_namespace_safe_ck CHECK (zorya_security_data.zsec_safe_text(namespace_id, 256, 512)),
  ADD CONSTRAINT zsec_knowledge_audience_base_ck CHECK (zorya_security_data.zsec_safe_text(base_id, 256, 512)),
  ADD CONSTRAINT zsec_knowledge_audience_id_ck CHECK (zorya_security_data.zsec_safe_text(audience_id, 256, 512));

ALTER TABLE zorya_security_data.zsec_session
  ADD CONSTRAINT zsec_session_principal_kind_ck CHECK (principal_kind IN ('user', 'service-account')),
  ADD CONSTRAINT zsec_session_hash_safe_ck CHECK (zorya_security_data.zsec_safe_text(token_hash, 512, 1024)),
  ADD CONSTRAINT zsec_session_csrf_hash_ck CHECK (zorya_security_data.zsec_safe_text(csrf_token_hash, 512, 1024)),
  ADD CONSTRAINT zsec_session_created_at_ck CHECK (created_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_session_expires_at_ck CHECK (expires_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_session_revoked_at_ck CHECK (revoked_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_session_invalidated_at_ck CHECK (invalidated_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_session_expiry_ck CHECK (expires_at > created_at);

ALTER TABLE zorya_security_data.zsec_audit_record
  ADD CONSTRAINT zsec_audit_record_id_ck CHECK (zorya_security_data.zsec_safe_text(id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_namespace_safe_ck CHECK (zorya_security_data.zsec_safe_text(namespace_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_sequence_ck CHECK (sequence BETWEEN 0 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_record_occurred_at_ck CHECK (occurred_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_record_recorded_at_ck CHECK (recorded_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_record_event_safe_ck CHECK (zorya_security_data.zsec_safe_text(event, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_actor_kind_ck CHECK (actor_kind IN ('operator', 'worker', 'user', 'service-account')),
  ADD CONSTRAINT zsec_audit_record_actor_id_ck CHECK (zorya_security_data.zsec_safe_text(actor_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_actor_pair_ck CHECK ((actor_kind IS NULL) = (actor_id IS NULL)),
  ADD CONSTRAINT zsec_audit_record_effective_kind_ck CHECK (effective_kind IN ('operator', 'worker', 'user', 'service-account')),
  ADD CONSTRAINT zsec_audit_record_effective_id_ck CHECK (zorya_security_data.zsec_safe_text(effective_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_effective_pair_ck CHECK ((effective_kind IS NULL) = (effective_id IS NULL)),
  ADD CONSTRAINT zsec_audit_record_principals_pair_ck CHECK ((actor_kind IS NULL) = (effective_kind IS NULL)),
  ADD CONSTRAINT zsec_audit_record_permission_resource_ck CHECK (zorya_security_data.zsec_safe_text(permission_resource, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_permission_action_ck CHECK (zorya_security_data.zsec_safe_text(permission_action, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_permission_pair_ck CHECK ((permission_resource IS NULL) = (permission_action IS NULL)),
  ADD CONSTRAINT zsec_audit_record_permission_value_ck CHECK (zorya_security_data.zsec_valid_permission(permission_resource, permission_action)),
  ADD CONSTRAINT zsec_audit_record_resource_kind_ck CHECK (zorya_security_data.zsec_safe_text(resource_kind, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_resource_value_ck CHECK (zorya_security_data.zsec_valid_resource(resource_kind)),
  ADD CONSTRAINT zsec_audit_record_resource_namespace_ck CHECK (zorya_security_data.zsec_safe_text(resource_namespace_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_resource_pair_ck CHECK ((resource_kind IS NULL) = (resource_namespace_id IS NULL)),
  ADD CONSTRAINT zsec_audit_record_resource_id_ck CHECK (zorya_security_data.zsec_safe_text(resource_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_resource_principal_kind_ck CHECK (resource_principal_kind IN ('operator', 'worker', 'user', 'service-account')),
  ADD CONSTRAINT zsec_audit_record_resource_principal_id_ck CHECK (zorya_security_data.zsec_safe_text(resource_principal_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_resource_principal_pair_ck CHECK ((resource_principal_kind IS NULL) = (resource_principal_id IS NULL)),
  ADD CONSTRAINT zsec_audit_record_resource_children_ck CHECK (resource_kind IS NOT NULL OR (resource_id IS NULL AND resource_principal_kind IS NULL)),
  ADD CONSTRAINT zsec_audit_record_decision_reason_ck CHECK (zorya_security_data.zsec_safe_text(decision_reason, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_decision_value_ck CHECK (decision_reason IN ('operator_allow','access_role_allow','knowledge_grant_allow','self_identity_allow','permission_denied','namespace_denied','principal_disabled','identity_unknown','act_for_required','wrong_plane')),
  ADD CONSTRAINT zsec_audit_record_decision_pair_ck CHECK ((decision_allowed IS NULL) = (decision_reason IS NULL)),
  ADD CONSTRAINT zsec_audit_record_request_id_ck CHECK (zorya_security_data.zsec_safe_text(request_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_run_id_ck CHECK (zorya_security_data.zsec_safe_text(run_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_thread_id_ck CHECK (zorya_security_data.zsec_safe_text(thread_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_tool_call_id_ck CHECK (zorya_security_data.zsec_safe_text(tool_call_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_metadata_ck CHECK (zorya_security_data.zsec_valid_metadata(metadata)),
  ADD CONSTRAINT zsec_audit_record_previous_hash_ck CHECK (zorya_security_data.zsec_safe_text(previous_hash, 512, 1024)),
  ADD CONSTRAINT zsec_audit_record_hash_safe_ck CHECK (zorya_security_data.zsec_safe_text(hash, 512, 1024)),
  ADD CONSTRAINT zsec_audit_record_key_id_ck CHECK (zorya_security_data.zsec_safe_text(key_id, 256, 512)),
  ADD CONSTRAINT zsec_audit_record_time_order_ck CHECK (recorded_at >= occurred_at);

ALTER TABLE zorya_security_data.zsec_audit_outbox
  ADD CONSTRAINT zsec_audit_outbox_attempts_ck CHECK (attempts >= 0),
  ADD CONSTRAINT zsec_audit_outbox_initial_next_attempt_at_ck CHECK (initial_next_attempt_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_outbox_next_attempt_at_ck CHECK (next_attempt_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_outbox_lease_token_ck CHECK (zorya_security_data.zsec_safe_text(lease_token, 256, 512)),
  ADD CONSTRAINT zsec_audit_outbox_lease_owner_ck CHECK (zorya_security_data.zsec_safe_text(lease_owner, 256, 512)),
  ADD CONSTRAINT zsec_audit_outbox_lease_expires_at_ck CHECK (lease_expires_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_outbox_delivered_at_ck CHECK (delivered_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_audit_outbox_last_error_ck CHECK (zorya_security_data.zsec_safe_text(last_error, 2048, 4096));

ALTER TABLE zorya_security_data.zsec_denial_bucket
  ADD CONSTRAINT zsec_denial_bucket_key_ck CHECK (zorya_security_data.zsec_safe_text(key, 256, 512)),
  ADD CONSTRAINT zsec_denial_bucket_window_started_at_ck CHECK (window_started_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_window_ends_at_ck CHECK (window_ends_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_count_ck CHECK (count BETWEEN 0 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_suppressed_ck CHECK (suppressed BETWEEN 0 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_window_order_ck CHECK (window_ends_at > window_started_at),
  ADD CONSTRAINT zsec_denial_bucket_closed_started_ck CHECK (closed_window_started_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_closed_ends_ck CHECK (closed_window_ends_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_closed_count_ck CHECK (closed_count BETWEEN 0 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_closed_suppressed_ck CHECK (closed_suppressed BETWEEN 0 AND 9007199254740991),
  ADD CONSTRAINT zsec_denial_bucket_closed_shape_ck CHECK (
    (closed_window_started_at IS NULL AND closed_window_ends_at IS NULL AND closed_count IS NULL AND closed_suppressed IS NULL)
    OR (closed_window_started_at IS NOT NULL AND closed_window_ends_at IS NOT NULL AND closed_count IS NOT NULL AND closed_suppressed IS NOT NULL AND closed_window_ends_at > closed_window_started_at)
  );

ALTER TABLE zorya_security_data.zsec_break_glass_grant
  ADD CONSTRAINT zsec_break_glass_grant_principal_kind_ck CHECK (principal_kind IN ('user', 'service-account')),
  ADD CONSTRAINT zsec_break_glass_grant_id_ck CHECK (zorya_security_data.zsec_safe_text(id, 256, 512)),
  ADD CONSTRAINT zsec_break_glass_grant_token_ck CHECK (zorya_security_data.zsec_safe_text(token_hash, 512, 1024)),
  ADD CONSTRAINT zsec_break_glass_reason_safe_ck CHECK (zorya_security_data.zsec_safe_text(reason, 2048, 4096)),
  ADD CONSTRAINT zsec_break_glass_grant_creator_ck CHECK (zorya_security_data.zsec_safe_text(created_by, 256, 512)),
  ADD CONSTRAINT zsec_break_glass_grant_created_at_ck CHECK (created_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_break_glass_grant_expires_at_ck CHECK (expires_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_break_glass_grant_revoked_at_ck CHECK (revoked_at BETWEEN -9007199254740991 AND 9007199254740991),
  ADD CONSTRAINT zsec_break_glass_grant_expiry_ck CHECK (expires_at > created_at);
