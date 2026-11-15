import type { WallClock } from "@promin/workflow";
import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import {
  AUTHORIZATION_REASONS,
  ORG_WIDE_NAMESPACE,
  SECURITY_ACTIONS,
} from "../server/security/index.ts";
import { SECURITY_STORE_LIMITS } from "../security-module/security-store.ts";
import type {
  AccessBinding,
  AccessGroup,
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
  SecurityStorePage,
  SecurityStoreTextLimit,
  SecurityStoreV2,
  SecurityUser,
  ServiceAccountCredential,
  SubjectAccessBinding,
} from "../security-module/security-store.ts";

export interface PostgresSecurityStoreOptions {
  readonly db: DrizzleDb;
  /** Test-only clock override. Production defaults to PostgreSQL's clock. */
  readonly clock?: WallClock;
}

type DbRow = Record<string, unknown>;

/** PostgreSQL-backed durable implementation of the complete security-store contract. */
export class PostgresSecurityStore implements SecurityStoreV2 {
  private readonly db: DrizzleDb;
  private readonly clock?: WallClock;

  constructor(config: PostgresSecurityStoreOptions) {
    this.db = config.db;
    this.clock = config.clock;
  }

  async findOrCreateUser(params: {
    readonly email: string;
    readonly bindings: ReadonlyArray<Omit<AccessBinding, "userId">>;
  }): Promise<{ user: SecurityUser; created: boolean }> {
    const email = normalizeEmail(params.email);
    const bindings = normalizeInitialBindings(params.bindings);
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `legacy-user:${email}`);
      const [existing] = await execRaw(
        tx,
        sql`SELECT u.id, u.email, s.status FROM zorya_security_data.zsec_user u JOIN zorya_security_data.zsec_subject s ON s.kind = 'user' AND s.id = u.id WHERE u.email = ${email} ORDER BY u.id LIMIT 1`,
      );
      if (existing) return { user: userFromRow(existing), created: false };
      const id = crypto.randomUUID();
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at) VALUES ('user',${id},'active',${now},${now})`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_user(subject_kind,id,email) VALUES ('user',${id},${email})`,
      );
      for (const binding of bindings) {
        await execRaw(
          tx,
          sql`INSERT INTO zorya_security_data.zsec_access_binding(subject_kind,subject_id,namespace_id,access_role) VALUES ('user',${id},${binding.namespaceId},${binding.accessRole}) ON CONFLICT DO NOTHING`,
        );
      }
      return { user: freeze({ id, email, status: "active" }), created: true };
    });
  }

  async getUser(id: string): Promise<SecurityUser | undefined> {
    const value = requiredString(id, "user id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT u.id,u.email,s.status FROM zorya_security_data.zsec_user u JOIN zorya_security_data.zsec_subject s ON s.kind='user' AND s.id=u.id WHERE u.id=${value}`,
    );
    return row && userFromRow(row);
  }

  async listUsers(): Promise<SecurityUser[]> {
    const rows = await execRaw(
      this.db,
      sql`SELECT u.id,u.email,s.status FROM zorya_security_data.zsec_user u JOIN zorya_security_data.zsec_subject s ON s.kind='user' AND s.id=u.id ORDER BY u.id`,
    );
    return rows.map(userFromRow);
  }

  async setUserStatus(params: { id: string; status: SecurityUser["status"] }): Promise<void> {
    const id = requiredString(params.id, "user id");
    const status = statusValue(params.status);
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`UPDATE zorya_security_data.zsec_subject SET status=${status},updated_at=${now} WHERE kind='user' AND id=${id}`,
      );
      if (status === "disabled") {
        await execRaw(
          tx,
          sql`UPDATE zorya_security_data.zsec_session SET invalidated_at=COALESCE(invalidated_at,${now}) WHERE principal_kind='user' AND principal_id=${id}`,
        );
      }
    });
  }

  async listBindings(userId: string): Promise<AccessBinding[]> {
    const rows = await this.listSubjectBindings({ kind: "user", id: userId });
    return rows.map((row) =>
      freeze({
        userId: row.subject.id,
        namespaceId: row.namespaceId,
        accessRole: row.accessRole,
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
    return (await this.listKnowledgeAudienceGrants(params))
      .filter((row) => row.audience.kind === "access-role")
      .map((row) =>
        freeze({
          namespaceId: row.namespaceId,
          baseId: row.baseId,
          accessRole: row.audience.id,
        }),
      );
  }

  async createSession(params: { tokenHash: string; userId: string; ttlMs: number }): Promise<void> {
    const tokenHash = requiredString(
      params.tokenHash,
      "session token hash",
      SECURITY_STORE_LIMITS.hash,
    );
    const userId = requiredString(params.userId, "user id");
    const ttl = positiveIntegerMs(params.ttlMs, "session ttl");
    const now = await this.now(this.db);
    await this.putSession({
      tokenHash,
      principal: { kind: "user", id: userId },
      createdAt: now,
      expiresAt: finiteSum(now, ttl, "session expiry"),
      authMethod: "oidc",
    });
  }

  async getSession(tokenHash: string): Promise<{ userId: string } | undefined> {
    const row = await this.getSessionRecord(tokenHash);
    return row?.principal.kind === "user" ? freeze({ userId: row.principal.id }) : undefined;
  }

  async deleteSession(tokenHash: string): Promise<void> {
    const hash = requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `session:${hash}`);
      const [row] = await execRaw(
        tx,
        sql`SELECT auth_method FROM zorya_security_data.zsec_session WHERE token_hash=${hash}`,
      );
      if (!row) return;
      if (row.auth_method === "break-glass") {
        throw new Error("Break-glass sessions must be revoked transactionally");
      }
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`UPDATE zorya_security_data.zsec_session SET revoked_at=COALESCE(revoked_at,${now}) WHERE token_hash=${hash}`,
      );
    });
  }

  async findOrCreateProviderUser(params: {
    readonly identity: Omit<ProviderIdentity, "userId">;
    readonly email: string;
    readonly bindings?: ReadonlyArray<Omit<SubjectAccessBinding, "subject">>;
  }): Promise<{
    readonly user: SecurityUser;
    readonly identity: ProviderIdentity;
    readonly created: boolean;
  }> {
    const identity = normalizeProviderIdentity(params.identity);
    const email = normalizeEmail(params.email);
    const bindings = normalizeInitialBindings(params.bindings ?? []);
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `provider:${identity.providerId}:${identity.issuer}:${identity.subject}`);
      const [existing] = await execRaw(
        tx,
        sql`SELECT p.provider_id,p.issuer,p.subject,p.user_id,u.email,s.status FROM zorya_security_data.zsec_provider_identity p JOIN zorya_security_data.zsec_user u ON u.id=p.user_id JOIN zorya_security_data.zsec_subject s ON s.kind='user' AND s.id=u.id WHERE p.provider_id=${identity.providerId} AND p.issuer=${identity.issuer} AND p.subject=${identity.subject}`,
      );
      if (existing) {
        await execRaw(
          tx,
          sql`UPDATE zorya_security_data.zsec_user SET email=${email} WHERE id=${existing.user_id as string}`,
        );
        const provider = freeze({
          ...identity,
          userId: existing.user_id as string,
        });
        return {
          user: freeze({
            id: provider.userId,
            email,
            status: existing.status as SecurityUser["status"],
          }),
          identity: provider,
          created: false,
        };
      }
      const userId = crypto.randomUUID();
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at) VALUES ('user',${userId},'active',${now},${now})`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_user(subject_kind,id,email) VALUES ('user',${userId},${email})`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_provider_identity(provider_id,issuer,subject,user_id) VALUES (${identity.providerId},${identity.issuer},${identity.subject},${userId})`,
      );
      for (const binding of bindings) {
        await execRaw(
          tx,
          sql`INSERT INTO zorya_security_data.zsec_access_binding(subject_kind,subject_id,namespace_id,access_role) VALUES ('user',${userId},${binding.namespaceId},${binding.accessRole}) ON CONFLICT DO NOTHING`,
        );
      }
      return {
        user: freeze({ id: userId, email, status: "active" }),
        identity: freeze({ ...identity, userId }),
        created: true,
      };
    });
  }

  async getProviderIdentity(params: {
    providerId: string;
    issuer: string;
    subject: string;
  }): Promise<ProviderIdentity | undefined> {
    const identity = normalizeProviderIdentity(params);
    const [row] = await execRaw(
      this.db,
      sql`SELECT provider_id,issuer,subject,user_id FROM zorya_security_data.zsec_provider_identity WHERE provider_id=${identity.providerId} AND issuer=${identity.issuer} AND subject=${identity.subject}`,
    );
    return (
      row &&
      freeze({
        providerId: row.provider_id as string,
        issuer: row.issuer as string,
        subject: row.subject as string,
        userId: row.user_id as string,
      })
    );
  }

  async putGroup(value: AccessGroup): Promise<void> {
    const group = normalizeGroup(value);
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at) VALUES ('group',${group.id},${group.status},${now},${now}) ON CONFLICT (kind,id) DO UPDATE SET status=EXCLUDED.status,updated_at=EXCLUDED.updated_at`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_access_group(subject_kind,id) VALUES ('group',${group.id}) ON CONFLICT DO NOTHING`,
      );
    });
  }

  async getGroup(id: string): Promise<AccessGroup | undefined> {
    const value = requiredString(id, "group id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT g.id,s.status FROM zorya_security_data.zsec_access_group g JOIN zorya_security_data.zsec_subject s ON s.kind='group' AND s.id=g.id WHERE g.id=${value}`,
    );
    return (
      row &&
      freeze({
        id: row.id as string,
        status: row.status as AccessGroup["status"],
      })
    );
  }

  async putGroupMembership(value: GroupMembership): Promise<void> {
    const row = normalizeMembership(value);
    await this.assertSubjectExists(this.db, { kind: "group", id: row.groupId }, "Unknown group");
    await this.assertSubjectExists(this.db, row.subject, `Unknown ${row.subject.kind}`);
    await execRaw(
      this.db,
      sql`INSERT INTO zorya_security_data.zsec_group_membership(group_id,subject_kind,subject_id) VALUES (${row.groupId},${row.subject.kind},${row.subject.id}) ON CONFLICT DO NOTHING`,
    );
  }

  async removeGroupMembership(value: GroupMembership): Promise<void> {
    const row = normalizeMembership(value);
    await execRaw(
      this.db,
      sql`DELETE FROM zorya_security_data.zsec_group_membership WHERE group_id=${row.groupId} AND subject_kind=${row.subject.kind} AND subject_id=${row.subject.id}`,
    );
  }

  async listGroupsForSubject(value: SecurityPrincipalRef): Promise<AccessGroup[]> {
    const subject = normalizePrincipalRef(value);
    const rows = await execRaw(
      this.db,
      sql`SELECT g.id,s.status FROM zorya_security_data.zsec_group_membership m JOIN zorya_security_data.zsec_access_group g ON g.id=m.group_id JOIN zorya_security_data.zsec_subject s ON s.kind='group' AND s.id=g.id WHERE m.subject_kind=${subject.kind} AND m.subject_id=${subject.id} ORDER BY g.id`,
    );
    return rows.map((row) =>
      freeze({
        id: row.id as string,
        status: row.status as AccessGroup["status"],
      }),
    );
  }

  async putSubjectBinding(value: SubjectAccessBinding): Promise<void> {
    const row = normalizeSubjectBinding(value);
    await this.assertSubjectExists(this.db, row.subject, `Unknown ${row.subject.kind}`);
    await execRaw(
      this.db,
      sql`INSERT INTO zorya_security_data.zsec_access_binding(subject_kind,subject_id,namespace_id,access_role) VALUES (${row.subject.kind},${row.subject.id},${row.namespaceId},${row.accessRole}) ON CONFLICT DO NOTHING`,
    );
  }

  async removeSubjectBinding(value: SubjectAccessBinding): Promise<void> {
    const row = normalizeSubjectBinding(value);
    await execRaw(
      this.db,
      sql`DELETE FROM zorya_security_data.zsec_access_binding WHERE subject_kind=${row.subject.kind} AND subject_id=${row.subject.id} AND namespace_id=${row.namespaceId} AND access_role=${row.accessRole}`,
    );
  }

  async listSubjectBindings(value: AccessSubjectRef): Promise<SubjectAccessBinding[]> {
    const subject = normalizeAccessSubject(value);
    const rows = await execRaw(
      this.db,
      sql`SELECT namespace_id,access_role FROM zorya_security_data.zsec_access_binding WHERE subject_kind=${subject.kind} AND subject_id=${subject.id} ORDER BY namespace_id,access_role`,
    );
    return rows.map((row) =>
      freeze({
        subject: clone(subject),
        namespaceId: row.namespace_id as string,
        accessRole: row.access_role as string,
      }),
    );
  }

  async resolveAccess(params: {
    readonly subject: SecurityPrincipalRef;
    readonly namespaceId: string;
  }): Promise<{
    readonly accessRoles: ReadonlyArray<string>;
    readonly groups: ReadonlyArray<string>;
  }> {
    const subject = normalizePrincipalRef(params.subject);
    const namespaceId = requiredString(
      params.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    );
    const [row] = await execRaw(
      this.db,
      sql`
        WITH active_subject AS (
          SELECT id
          FROM zorya_security_data.zsec_subject
          WHERE kind=${subject.kind} AND id=${subject.id} AND status='active'
        ), active_groups AS (
          SELECT m.group_id
          FROM active_subject
          JOIN zorya_security_data.zsec_group_membership m
            ON m.subject_kind=${subject.kind} AND m.subject_id=active_subject.id
          JOIN zorya_security_data.zsec_subject g
            ON g.kind='group' AND g.id=m.group_id AND g.status='active'
        )
        SELECT
          COALESCE(
            (SELECT pg_catalog.array_agg(group_id ORDER BY group_id) FROM active_groups),
            ARRAY[]::pg_catalog.text[]
          ) AS groups,
          COALESCE(
            (
              SELECT pg_catalog.array_agg(DISTINCT b.access_role ORDER BY b.access_role)
              FROM zorya_security_data.zsec_access_binding b
              WHERE EXISTS (SELECT 1 FROM active_subject)
                AND b.namespace_id IN (${namespaceId},${ORG_WIDE_NAMESPACE})
                AND (
                  (b.subject_kind=${subject.kind} AND b.subject_id=${subject.id})
                  OR (b.subject_kind='group' AND b.subject_id IN (SELECT group_id FROM active_groups))
                )
            ),
            ARRAY[]::pg_catalog.text[]
          ) AS access_roles
      `,
    );
    return freeze({
      accessRoles: freeze((row?.access_roles as string[] | undefined) ?? []),
      groups: freeze((row?.groups as string[] | undefined) ?? []),
    });
  }

  async putKnowledgeAudienceGrant(value: KnowledgeAudienceGrant): Promise<void> {
    const row = normalizeKnowledgeAudienceGrant(value);
    if (row.audience.kind === "group") {
      await this.assertSubjectExists(this.db, row.audience, "Unknown group");
    }
    await execRaw(
      this.db,
      sql`INSERT INTO zorya_security_data.zsec_knowledge_audience_grant(namespace_id,base_id,audience_kind,audience_id) VALUES (${row.namespaceId},${row.baseId},${row.audience.kind},${row.audience.id}) ON CONFLICT DO NOTHING`,
    );
  }

  async removeKnowledgeAudienceGrant(value: KnowledgeAudienceGrant): Promise<void> {
    const row = normalizeKnowledgeAudienceGrant(value);
    await execRaw(
      this.db,
      sql`DELETE FROM zorya_security_data.zsec_knowledge_audience_grant WHERE namespace_id=${row.namespaceId} AND base_id=${row.baseId} AND audience_kind=${row.audience.kind} AND audience_id=${row.audience.id}`,
    );
  }

  async listKnowledgeAudienceGrants(params: {
    readonly namespaceId: string;
    readonly baseId: string;
  }): Promise<KnowledgeAudienceGrant[]> {
    const namespaceId = requiredString(
      params.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    );
    const baseId = requiredString(params.baseId, "knowledge base id");
    const rows = await execRaw(
      this.db,
      sql`SELECT namespace_id,base_id,audience_kind,audience_id FROM zorya_security_data.zsec_knowledge_audience_grant WHERE namespace_id IN (${namespaceId},${ORG_WIDE_NAMESPACE}) AND base_id=${baseId} ORDER BY namespace_id,audience_kind,audience_id`,
    );
    return rows.map((row) =>
      freeze({
        namespaceId: row.namespace_id as string,
        baseId: row.base_id as string,
        audience: freeze({
          kind: row.audience_kind as "access-role" | "group",
          id: row.audience_id as string,
        }),
      }),
    );
  }

  async putServiceAccount(value: SecurityServiceAccount): Promise<void> {
    const account = normalizeServiceAccount(value);
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at) VALUES ('service-account',${account.id},${account.status},${now},${now}) ON CONFLICT (kind,id) DO UPDATE SET status=EXCLUDED.status,updated_at=EXCLUDED.updated_at`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_service_account(subject_kind,id,can_act_for) VALUES ('service-account',${account.id},${account.canActFor}) ON CONFLICT (id) DO UPDATE SET can_act_for=EXCLUDED.can_act_for`,
      );
      if (account.status === "disabled") {
        await execRaw(
          tx,
          sql`UPDATE zorya_security_data.zsec_session SET invalidated_at=COALESCE(invalidated_at,${now}) WHERE principal_kind='service-account' AND principal_id=${account.id}`,
        );
      }
    });
  }

  async createServiceAccountWithCredential(params: {
    readonly account: SecurityServiceAccount;
    readonly credential: ServiceAccountCredential;
  }): Promise<boolean> {
    const account = normalizeServiceAccount(params.account);
    const credential = normalizeCredential(params.credential);
    if (credential.serviceAccountId !== account.id) {
      throw new Error("Credential service account does not match account");
    }
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `service-account:${account.id}`);
      const [existing] = await execRaw(
        tx,
        sql`SELECT id FROM zorya_security_data.zsec_service_account WHERE id=${account.id}`,
      );
      if (existing) return false;
      const now = await this.now(tx);
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at) VALUES ('service-account',${account.id},${account.status},${now},${now})`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_service_account(subject_kind,id,can_act_for) VALUES ('service-account',${account.id},${account.canActFor})`,
      );
      try {
        await execRaw(
          tx,
          sql`INSERT INTO zorya_security_data.zsec_service_account_credential(id,service_account_id,secret_hash,created_at,expires_at,revoked_at) VALUES (${credential.id},${credential.serviceAccountId},${credential.secretHash},${credential.createdAt},${credential.expiresAt ?? null},${credential.revokedAt ?? null})`,
        );
      } catch (error) {
        if (isUniqueViolation(error)) throw new Error("Credential hash is already used");
        throw error;
      }
      return true;
    });
  }

  async getServiceAccount(id: string): Promise<SecurityServiceAccount | undefined> {
    const value = requiredString(id, "service account id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT a.id,a.can_act_for,s.status FROM zorya_security_data.zsec_service_account a JOIN zorya_security_data.zsec_subject s ON s.kind='service-account' AND s.id=a.id WHERE a.id=${value}`,
    );
    return row && serviceAccountFromRow(row);
  }

  async listServiceAccounts(): Promise<SecurityServiceAccount[]> {
    const rows = await execRaw(
      this.db,
      sql`SELECT a.id,a.can_act_for,s.status FROM zorya_security_data.zsec_service_account a JOIN zorya_security_data.zsec_subject s ON s.kind='service-account' AND s.id=a.id ORDER BY a.id`,
    );
    return rows.map(serviceAccountFromRow);
  }

  async putServiceAccountCredential(value: ServiceAccountCredential): Promise<void> {
    const credential = normalizeCredential(value);
    await this.assertSubjectExists(
      this.db,
      { kind: "service-account", id: credential.serviceAccountId },
      "Unknown service account",
    );
    const [existing] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_service_account_credential WHERE id=${credential.id}`,
    );
    if (existing) {
      if (!recordsEqual(credentialFromRow(existing), credential))
        throw new Error("Conflicting service-account credential id");
      return;
    }
    try {
      await execRaw(
        this.db,
        sql`INSERT INTO zorya_security_data.zsec_service_account_credential(id,service_account_id,secret_hash,created_at,expires_at,revoked_at) VALUES (${credential.id},${credential.serviceAccountId},${credential.secretHash},${credential.createdAt},${credential.expiresAt ?? null},${credential.revokedAt ?? null})`,
      );
    } catch (error) {
      if (isUniqueViolation(error)) throw new Error("Credential hash is already used");
      throw error;
    }
  }

  async getServiceAccountCredential(id: string): Promise<ServiceAccountCredential | undefined> {
    const value = requiredString(id, "credential id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_service_account_credential WHERE id=${value}`,
    );
    return row && credentialFromRow(row);
  }

  async listServiceAccountCredentials(
    serviceAccountId: string,
  ): Promise<ServiceAccountCredential[]> {
    const id = requiredString(serviceAccountId, "service account id");
    const rows = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_service_account_credential WHERE service_account_id=${id} ORDER BY created_at,id`,
    );
    return rows.map(credentialFromRow);
  }

  async revokeServiceAccountCredential(params: {
    readonly id: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const id = requiredString(params.id, "credential id");
    const revokedAt = integerMs(params.revokedAt, "credential revoked at");
    await execRaw(
      this.db,
      sql`UPDATE zorya_security_data.zsec_service_account_credential SET revoked_at=COALESCE(revoked_at,${revokedAt}) WHERE id=${id}`,
    );
  }

  async putSession(value: SecuritySession): Promise<void> {
    const session = normalizeSession(value);
    if (session.authMethod === "break-glass") {
      throw new Error("Break-glass sessions must be created transactionally");
    }
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `session:${session.tokenHash}`);
      await this.assertActiveSubject(
        tx,
        session.principal,
        `Unknown or disabled ${session.principal.kind}`,
      );
      const [breakGlass] = await execRaw(
        tx,
        sql`SELECT 1 FROM zorya_security_data.zsec_break_glass_grant WHERE token_hash=${session.tokenHash}`,
      );
      if (breakGlass) throw new Error("Break-glass sessions must be created transactionally");
      const [existing] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_session WHERE token_hash=${session.tokenHash}`,
      );
      if (existing) {
        const stored = sessionFromRow(existing);
        if (!recordsEqual(stored, session)) {
          throw new Error("Conflicting session token hash");
        }
        return;
      }
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_session(token_hash,principal_kind,principal_id,created_at,expires_at,csrf_token_hash,revoked_at,auth_method) VALUES (${session.tokenHash},${session.principal.kind},${session.principal.id},${session.createdAt},${session.expiresAt},${session.csrfTokenHash ?? null},${session.revokedAt ?? null},${session.authMethod})`,
      );
    });
  }

  async getSessionRecord(tokenHash: string): Promise<SecuritySession | undefined> {
    const hash = requiredString(tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash);
    const now = await this.now(this.db);
    const [row] = await execRaw(
      this.db,
      sql`SELECT x.* FROM zorya_security_data.zsec_session x JOIN zorya_security_data.zsec_subject s ON s.kind=x.principal_kind AND s.id=x.principal_id AND s.status='active' WHERE x.token_hash=${hash} AND x.revoked_at IS NULL AND x.invalidated_at IS NULL AND x.expires_at>${now}`,
    );
    return row && sessionFromRow(row);
  }

  async revokeSession(params: {
    readonly tokenHash: string;
    readonly revokedAt: number;
  }): Promise<void> {
    const tokenHash = requiredString(
      params.tokenHash,
      "session token hash",
      SECURITY_STORE_LIMITS.hash,
    );
    const revokedAt = integerMs(params.revokedAt, "session revoked at");
    await this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `session:${tokenHash}`);
      const [row] = await execRaw(
        tx,
        sql`SELECT auth_method,revoked_at FROM zorya_security_data.zsec_session WHERE token_hash=${tokenHash}`,
      );
      if (!row) return;
      if (row.auth_method === "break-glass") {
        throw new Error("Break-glass sessions must be revoked transactionally");
      }
      if (row.revoked_at !== null && row.revoked_at !== undefined) {
        if (Number(row.revoked_at) === revokedAt) return;
        throw new Error(`Session is already revoked: ${tokenHash}`);
      }
      await execRaw(
        tx,
        sql`UPDATE zorya_security_data.zsec_session SET revoked_at=${revokedAt} WHERE token_hash=${tokenHash}`,
      );
    });
  }

  async appendAuditWithOutbox(params: {
    readonly record: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"appended" | "already-exists"> {
    const record = normalizeAuditRecord(params.record);
    const outbox = params.outbox && pendingAuditOutbox(record.id, params.outbox.nextAttemptAt);
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      return this.appendAuditInTx(tx, record, outbox);
    });
  }

  async getAuditRecord(eventId: string): Promise<SecurityAuditRecord | undefined> {
    const id = requiredString(eventId, "audit event id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_audit_record WHERE id=${id}`,
    );
    return row && auditFromRow(row);
  }

  async listAuditRecords(params: {
    readonly namespaceId: string;
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<SecurityStorePage<SecurityAuditRecord>> {
    const namespaceId = requiredString(
      params.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    );
    const cursor =
      params.cursor === undefined ? undefined : requiredString(params.cursor, "audit cursor");
    const limit = pageLimit(params.limit, "audit page limit");
    let afterSequence = -1;
    if (cursor !== undefined) {
      const [row] = await execRaw(
        this.db,
        sql`SELECT sequence,namespace_id FROM zorya_security_data.zsec_audit_record WHERE id=${cursor}`,
      );
      if (!row || row.namespace_id !== namespaceId)
        throw new Error(`Unknown audit cursor: ${cursor}`);
      afterSequence = Number(row.sequence);
    }
    const rows = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_audit_record WHERE namespace_id=${namespaceId} AND sequence>${afterSequence} ORDER BY sequence,id LIMIT ${limit + 1}`,
    );
    return makePage(rows.map(auditFromRow), limit, (row) => row.id);
  }

  async getAuditOutbox(eventId: string): Promise<SecurityAuditOutboxRecord | undefined> {
    const id = requiredString(eventId, "event id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${id}`,
    );
    return row && outboxFromRow(row);
  }

  async listAuditOutbox(
    params: {
      readonly status?: SecurityAuditOutboxRecord["status"];
      readonly cursor?: string;
      readonly limit?: number;
    } = {},
  ): Promise<SecurityStorePage<SecurityAuditOutboxRecord>> {
    const status = params.status;
    if (
      status !== undefined &&
      status !== "pending" &&
      status !== "leased" &&
      status !== "delivered"
    )
      throw new TypeError("invalid outbox status");
    const cursor =
      params.cursor === undefined ? undefined : requiredString(params.cursor, "outbox cursor");
    const limit = pageLimit(params.limit, "outbox page limit");
    if (cursor !== undefined) {
      const [exists] = await execRaw(
        this.db,
        sql`SELECT 1 FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${cursor}`,
      );
      if (!exists) throw new Error(`Unknown outbox cursor: ${cursor}`);
    }
    const rows =
      status === undefined
        ? await execRaw(
            this.db,
            sql`SELECT * FROM zorya_security_data.zsec_audit_outbox WHERE (${cursor ?? null}::pg_catalog.text IS NULL OR event_id>${cursor ?? ""}) ORDER BY event_id LIMIT ${limit + 1}`,
          )
        : await execRaw(
            this.db,
            sql`SELECT * FROM zorya_security_data.zsec_audit_outbox WHERE status=${status} AND (${cursor ?? null}::pg_catalog.text IS NULL OR event_id>${cursor ?? ""}) ORDER BY event_id LIMIT ${limit + 1}`,
          );
    return makePage(rows.map(outboxFromRow), limit, (row) => row.eventId);
  }

  async claimAuditOutbox(params: {
    readonly owner: string;
    readonly leaseMs: number;
    readonly limit: number;
  }): Promise<ReadonlyArray<SecurityAuditOutboxRecord>> {
    const owner = requiredString(params.owner, "outbox lease owner");
    const leaseMs = positiveIntegerMs(params.leaseMs, "outbox lease duration");
    const limit = positiveInteger(params.limit, "outbox claim limit");
    if (limit > SECURITY_STORE_LIMITS.outboxBatch)
      throw new TypeError("outbox claim limit is too large");
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      const now = this.nowExpression();
      const rows = await execRaw(
        tx,
        sql`
        WITH store_clock AS (
          SELECT ${now}::pg_catalog.int8 AS now
        ), candidates AS (
          SELECT event_id
          FROM zorya_security_data.zsec_audit_outbox, store_clock
          WHERE (status='pending' AND next_attempt_at<=store_clock.now)
             OR (status='leased' AND lease_expires_at<=store_clock.now)
          ORDER BY next_attempt_at,event_id
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        UPDATE zorya_security_data.zsec_audit_outbox o
        SET status='leased', attempts=o.attempts+1,
            lease_token=pg_catalog.md5(
              pg_catalog.random()::pg_catalog.text
              || pg_catalog.clock_timestamp()::pg_catalog.text
              || o.event_id
            ),
            lease_owner=${owner}, lease_expires_at=store_clock.now+${leaseMs}, delivered_at=NULL
        FROM candidates c, store_clock WHERE o.event_id=c.event_id
        RETURNING o.*`,
      );
      return freeze(rows.map(outboxFromRow));
    });
  }

  async ackAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
  }): Promise<boolean> {
    const eventId = requiredString(params.eventId, "event id");
    const leaseToken = requiredString(params.leaseToken, "outbox lease token");
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await execRaw(
        tx,
        sql`SELECT event_id FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${eventId} FOR UPDATE`,
      );
      const now = this.nowExpression();
      const rows = await execRaw(
        tx,
        sql`WITH store_clock AS (SELECT ${now}::pg_catalog.int8 AS now)
          UPDATE zorya_security_data.zsec_audit_outbox o
          SET status='delivered',delivered_at=COALESCE(o.delivered_at,store_clock.now)
          FROM store_clock
          WHERE o.event_id=${eventId} AND o.lease_token=${leaseToken}
            AND ((o.status='leased' AND o.lease_expires_at>store_clock.now) OR o.status='delivered')
          RETURNING o.event_id`,
      );
      return rows.length > 0;
    });
  }

  async releaseAuditOutbox(params: {
    readonly eventId: string;
    readonly leaseToken: string;
    readonly nextAttemptAt: number;
    readonly lastError?: string;
  }): Promise<boolean> {
    const eventId = requiredString(params.eventId, "event id");
    const leaseToken = requiredString(params.leaseToken, "outbox lease token");
    const nextAttemptAt = integerMs(params.nextAttemptAt, "outbox next attempt");
    const lastError = optionalBoundedString(
      params.lastError,
      "outbox last error",
      SECURITY_STORE_LIMITS.reason,
    );
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await execRaw(
        tx,
        sql`SELECT event_id FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${eventId} FOR UPDATE`,
      );
      const now = this.nowExpression();
      const rows = await execRaw(
        tx,
        sql`WITH store_clock AS (SELECT ${now}::pg_catalog.int8 AS now)
          UPDATE zorya_security_data.zsec_audit_outbox o
          SET status='pending',next_attempt_at=${nextAttemptAt},lease_owner=NULL,
              lease_expires_at=NULL,last_error=${lastError ?? null}
          FROM store_clock
          WHERE o.event_id=${eventId} AND o.lease_token=${leaseToken}
            AND (
              (o.status='leased' AND o.lease_expires_at>store_clock.now)
              OR (o.status='pending' AND o.next_attempt_at=${nextAttemptAt}
                  AND o.last_error IS NOT DISTINCT FROM ${lastError ?? null})
            )
          RETURNING o.event_id`,
      );
      return rows.length > 0;
    });
  }

  async incrementDenialBucket(params: {
    readonly key: string;
    readonly now: number;
    readonly windowMs: number;
    readonly emitLimit: number;
  }): Promise<{
    readonly bucket: SecurityDenialBucket;
    readonly shouldEmit: boolean;
  }> {
    const key = requiredString(params.key, "denial bucket key");
    const now = integerMs(params.now, "denial time");
    const windowMs = positiveIntegerMs(params.windowMs, "denial window");
    const emitLimit = nonNegativeInteger(params.emitLimit, "denial emit limit");
    const windowEnd = finiteSum(now, windowMs, "denial window end");
    try {
      const [row] = await execRaw(
        this.db,
        sql`
        INSERT INTO zorya_security_data.zsec_denial_bucket(key,window_started_at,window_ends_at,count,suppressed)
        VALUES (${key},${now},${windowEnd},1,${emitLimit < 1 ? 1 : 0})
        ON CONFLICT (key) DO UPDATE SET
          window_started_at=CASE WHEN ${now}>=zorya_security_data.zsec_denial_bucket.window_ends_at THEN ${now} ELSE zorya_security_data.zsec_denial_bucket.window_started_at END,
          window_ends_at=CASE WHEN ${now}>=zorya_security_data.zsec_denial_bucket.window_ends_at THEN ${windowEnd} ELSE zorya_security_data.zsec_denial_bucket.window_ends_at END,
          count=CASE WHEN ${now}>=zorya_security_data.zsec_denial_bucket.window_ends_at THEN 1 ELSE zorya_security_data.zsec_denial_bucket.count+1 END,
          suppressed=CASE WHEN ${now}>=zorya_security_data.zsec_denial_bucket.window_ends_at THEN ${emitLimit < 1 ? 1 : 0} WHEN zorya_security_data.zsec_denial_bucket.count+1>${emitLimit} THEN zorya_security_data.zsec_denial_bucket.suppressed+1 ELSE zorya_security_data.zsec_denial_bucket.suppressed END
        WHERE ${now}>=zorya_security_data.zsec_denial_bucket.window_started_at
        RETURNING *`,
      );
      if (!row) throw new Error("Denial time precedes the current window");
      const bucket = denialFromRow(row);
      return freeze({ bucket, shouldEmit: bucket.count <= emitLimit });
    } catch (error) {
      if (isNumericOverflow(error)) throw new Error("denial count overflow");
      throw error;
    }
  }

  async getDenialBucket(key: string): Promise<SecurityDenialBucket | undefined> {
    const value = requiredString(key, "denial bucket key");
    const [row] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_denial_bucket WHERE key=${value}`,
    );
    return row && denialFromRow(row);
  }

  async createBreakGlassAccess(params: {
    readonly grant: BreakGlassGrant;
    readonly session: SecuritySession;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"created" | "already-exists"> {
    const grant = normalizeBreakGlassGrant(params.grant);
    const session = normalizeSession(params.session);
    validateBreakGlassSession(grant, session);
    const audit = normalizeAuditRecord(params.audit);
    validateBreakGlassAudit(audit, grant, "break-glass.created");
    const outbox = params.outbox && pendingAuditOutbox(audit.id, params.outbox.nextAttemptAt);
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `break-glass:${grant.id}`);
      await lock(tx, `break-glass-token:${grant.tokenHash}`);
      await lock(tx, `session:${grant.tokenHash}`);
      await this.assertActiveSubject(
        tx,
        grant.principal,
        `Unknown or disabled ${grant.principal.kind}`,
      );
      const [tokenGrant] = await execRaw(
        tx,
        sql`SELECT id FROM zorya_security_data.zsec_break_glass_grant WHERE token_hash=${grant.tokenHash}`,
      );
      if (tokenGrant && tokenGrant.id !== grant.id)
        throw new Error(
          `Break-glass token hash is already used by grant: ${tokenGrant.id as string}`,
        );
      const [existingGrant] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_break_glass_grant WHERE id=${grant.id}`,
      );
      const [existingSession] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_session WHERE token_hash=${session.tokenHash}`,
      );
      const [existingAudit] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_audit_record WHERE id=${audit.id}`,
      );
      if (existingGrant || existingSession || existingAudit) {
        if (
          existingGrant &&
          existingSession &&
          existingAudit &&
          recordsEqual(grantFromRow(existingGrant), grant) &&
          recordsEqual(sessionFromRow(existingSession), session) &&
          recordsEqual(auditFromRow(existingAudit), audit) &&
          (await this.existingOutboxMatches(tx, audit.id, outbox))
        )
          return "already-exists";
        throw new Error("Conflicting or incomplete break-glass transaction");
      }
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_break_glass_grant(id,principal_kind,principal_id,token_hash,reason,created_by,created_at,expires_at,revoked_at) VALUES (${grant.id},${grant.principal.kind},${grant.principal.id},${grant.tokenHash},${grant.reason},${grant.createdBy},${grant.createdAt},${grant.expiresAt},NULL)`,
      );
      await execRaw(
        tx,
        sql`INSERT INTO zorya_security_data.zsec_session(token_hash,principal_kind,principal_id,created_at,expires_at,csrf_token_hash,revoked_at,auth_method) VALUES (${session.tokenHash},${session.principal.kind},${session.principal.id},${session.createdAt},${session.expiresAt},${session.csrfTokenHash ?? null},NULL,'break-glass')`,
      );
      await this.appendAuditInTx(tx, audit, outbox);
      return "created";
    });
  }

  async getBreakGlassGrant(id: string): Promise<BreakGlassGrant | undefined> {
    const value = requiredString(id, "break-glass grant id");
    const [row] = await execRaw(
      this.db,
      sql`SELECT * FROM zorya_security_data.zsec_break_glass_grant WHERE id=${value}`,
    );
    return row && grantFromRow(row);
  }

  async revokeBreakGlassAccess(params: {
    readonly grantId: string;
    readonly revokedAt: number;
    readonly audit: SecurityAuditRecord;
    readonly outbox?: { readonly nextAttemptAt: number };
  }): Promise<"revoked" | "already-revoked"> {
    const grantId = requiredString(params.grantId, "break-glass grant id");
    const revokedAt = integerMs(params.revokedAt, "break-glass revoked at");
    const audit = normalizeAuditRecord(params.audit);
    const outbox = params.outbox && pendingAuditOutbox(audit.id, params.outbox.nextAttemptAt);
    return this.db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      await lock(tx, `break-glass:${grantId}`);
      const [grantRow] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_break_glass_grant WHERE id=${grantId}`,
      );
      if (!grantRow) throw new Error(`Unknown break-glass grant: ${grantId}`);
      const grant = grantFromRow(grantRow);
      validateBreakGlassAudit(audit, grant, "break-glass.revoked");
      const [sessionRow] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_session WHERE token_hash=${grant.tokenHash}`,
      );
      if (!sessionRow || sessionRow.auth_method !== "break-glass")
        throw new Error("Break-glass grant has no matching session");
      const session = sessionFromRow(sessionRow);
      const [existingAudit] = await execRaw(
        tx,
        sql`SELECT * FROM zorya_security_data.zsec_audit_record WHERE id=${audit.id}`,
      );
      if (grant.revokedAt !== undefined || session.revokedAt !== undefined) {
        if (
          grant.revokedAt === revokedAt &&
          session.revokedAt === revokedAt &&
          existingAudit &&
          recordsEqual(auditFromRow(existingAudit), audit) &&
          (await this.existingOutboxMatches(tx, audit.id, outbox))
        )
          return "already-revoked";
        throw new Error("Conflicting or incomplete break-glass revocation");
      }
      if (existingAudit)
        throw new Error("Audit event already exists for an unrevoked break-glass grant");
      await execRaw(
        tx,
        sql`UPDATE zorya_security_data.zsec_break_glass_grant SET revoked_at=${revokedAt} WHERE id=${grantId}`,
      );
      await execRaw(
        tx,
        sql`UPDATE zorya_security_data.zsec_session SET revoked_at=${revokedAt} WHERE token_hash=${grant.tokenHash}`,
      );
      await this.appendAuditInTx(tx, audit, outbox);
      return "revoked";
    });
  }

  private async now(db: DrizzleDb): Promise<number> {
    if (this.clock !== undefined) return integerMs(this.clock.currentTimeMs(), "store clock");
    const [row] = await execRaw(
      db,
      sql`SELECT pg_catalog.floor(
        pg_catalog.date_part('epoch', pg_catalog.clock_timestamp()) * 1000
      )::pg_catalog.float8 AS now`,
    );
    return integerMs(Number(row!.now), "database clock");
  }

  private nowExpression(): ReturnType<typeof sql> {
    return this.clock === undefined
      ? sql`pg_catalog.floor(
          pg_catalog.date_part('epoch', pg_catalog.clock_timestamp()) * 1000
        )::pg_catalog.float8`
      : sql`${integerMs(this.clock.currentTimeMs(), "store clock")}`;
  }

  private async assertActiveSubject(
    db: DrizzleDb,
    subject: AccessSubjectRef,
    message: string,
  ): Promise<void> {
    const [row] = await execRaw(
      db,
      sql`SELECT 1 FROM zorya_security_data.zsec_subject WHERE kind=${subject.kind} AND id=${subject.id} AND status='active' FOR SHARE`,
    );
    if (!row) throw new Error(`${message}: ${subject.id}`);
  }

  private async assertSubjectExists(
    db: DrizzleDb,
    subject: AccessSubjectRef,
    message: string,
  ): Promise<void> {
    const [row] = await execRaw(
      db,
      sql`SELECT 1 FROM zorya_security_data.zsec_subject WHERE kind=${subject.kind} AND id=${subject.id}`,
    );
    if (!row) throw new Error(`${message}: ${subject.id}`);
  }

  private async appendAuditInTx(
    db: DrizzleDb,
    record: SecurityAuditRecord,
    outbox?: SecurityAuditOutboxRecord,
  ): Promise<"appended" | "already-exists"> {
    await lock(db, `audit:${record.namespaceId}`);
    const [existing] = await execRaw(
      db,
      sql`SELECT * FROM zorya_security_data.zsec_audit_record WHERE id=${record.id}`,
    );
    if (existing) {
      if (
        recordsEqual(auditFromRow(existing), record) &&
        (await this.existingOutboxMatches(db, record.id, outbox))
      )
        return "already-exists";
      throw new Error(`Conflicting audit event: ${record.id}`);
    }
    const [head] = await execRaw(
      db,
      sql`SELECT sequence,hash FROM zorya_security_data.zsec_audit_record WHERE namespace_id=${record.namespaceId} ORDER BY sequence DESC LIMIT 1`,
    );
    const expectedSequence = head ? Number(head.sequence) + 1 : 1;
    const expectedPrevious = head?.hash as string | undefined;
    if (record.sequence !== expectedSequence || record.previousHash !== expectedPrevious)
      throw new Error("Audit chain boundary mismatch");
    await execRaw(
      db,
      sql`INSERT INTO zorya_security_data.zsec_audit_record(id,namespace_id,sequence,occurred_at,recorded_at,event,actor_kind,actor_id,effective_kind,effective_id,permission_resource,permission_action,resource_kind,resource_namespace_id,resource_id,resource_principal_kind,resource_principal_id,decision_allowed,decision_reason,request_id,run_id,thread_id,tool_call_id,metadata,previous_hash,hash,key_id) VALUES (${record.id},${record.namespaceId},${record.sequence},${record.occurredAt},${record.recordedAt},${record.event},${record.actor?.kind ?? null},${record.actor?.id ?? null},${record.effective?.kind ?? null},${record.effective?.id ?? null},${record.permission?.resource ?? null},${record.permission?.action ?? null},${record.resource?.kind ?? null},${record.resource?.namespaceId ?? null},${record.resource?.id ?? null},${record.resource?.principal?.kind ?? null},${record.resource?.principal?.id ?? null},${record.decision?.allowed ?? null},${record.decision?.reason ?? null},${record.correlation?.requestId ?? null},${record.correlation?.runId ?? null},${record.correlation?.threadId ?? null},${record.correlation?.toolCallId ?? null},${record.metadata ? JSON.stringify(record.metadata) : null}::pg_catalog.jsonb,${record.previousHash ?? null},${record.hash},${record.keyId})`,
    );
    if (outbox)
      await execRaw(
        db,
        sql`INSERT INTO zorya_security_data.zsec_audit_outbox(event_id,status,attempts,next_attempt_at,initial_next_attempt_at) VALUES (${record.id},'pending',0,${outbox.nextAttemptAt},${outbox.nextAttemptAt})`,
      );
    return "appended";
  }

  private async existingOutboxMatches(
    db: DrizzleDb,
    eventId: string,
    outbox?: SecurityAuditOutboxRecord,
  ): Promise<boolean> {
    const [existing] = await execRaw(
      db,
      sql`SELECT initial_next_attempt_at FROM zorya_security_data.zsec_audit_outbox WHERE event_id=${eventId}`,
    );
    return outbox === undefined
      ? existing === undefined
      : existing !== undefined && Number(existing.initial_next_attempt_at) === outbox.nextAttemptAt;
  }
}

async function lock(db: DrizzleDb, key: string): Promise<void> {
  await execRaw(
    db,
    sql`SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(${key},0))`,
  );
}

function userFromRow(row: DbRow): SecurityUser {
  return freeze({
    id: row.id as string,
    email: row.email as string,
    status: row.status as SecurityUser["status"],
  });
}

function serviceAccountFromRow(row: DbRow): SecurityServiceAccount {
  return freeze({
    id: row.id as string,
    status: row.status as SecurityServiceAccount["status"],
    canActFor: row.can_act_for as boolean,
  });
}

function credentialFromRow(row: DbRow): ServiceAccountCredential {
  return freeze({
    id: row.id as string,
    serviceAccountId: row.service_account_id as string,
    secretHash: row.secret_hash as string,
    createdAt: Number(row.created_at),
    ...(row.expires_at !== null &&
      row.expires_at !== undefined && { expiresAt: Number(row.expires_at) }),
    ...(row.revoked_at !== null &&
      row.revoked_at !== undefined && { revokedAt: Number(row.revoked_at) }),
  });
}

function sessionFromRow(row: DbRow): SecuritySession {
  return freeze({
    tokenHash: row.token_hash as string,
    principal: freeze({
      kind: row.principal_kind as SecurityPrincipalRef["kind"],
      id: row.principal_id as string,
    }),
    createdAt: Number(row.created_at),
    expiresAt: Number(row.expires_at),
    ...(row.csrf_token_hash !== null &&
      row.csrf_token_hash !== undefined && {
        csrfTokenHash: row.csrf_token_hash as string,
      }),
    ...(row.revoked_at !== null &&
      row.revoked_at !== undefined && { revokedAt: Number(row.revoked_at) }),
    authMethod: row.auth_method as SecuritySession["authMethod"],
  });
}

function auditFromRow(row: DbRow): SecurityAuditRecord {
  const metadata = row.metadata as
    | Record<string, string | number | boolean | null>
    | null
    | undefined;
  return freeze({
    id: row.id as string,
    namespaceId: row.namespace_id as string,
    sequence: Number(row.sequence),
    occurredAt: Number(row.occurred_at),
    recordedAt: Number(row.recorded_at),
    event: row.event as string,
    ...(row.actor_kind !== null &&
      row.actor_kind !== undefined && {
        actor: freeze({
          kind: row.actor_kind as NonNullable<SecurityAuditRecord["actor"]>["kind"],
          id: row.actor_id as string,
        }),
      }),
    ...(row.effective_kind !== null &&
      row.effective_kind !== undefined && {
        effective: freeze({
          kind: row.effective_kind as NonNullable<SecurityAuditRecord["effective"]>["kind"],
          id: row.effective_id as string,
        }),
      }),
    ...(row.permission_resource !== null &&
      row.permission_resource !== undefined && {
        permission: freeze({
          resource: row.permission_resource as string,
          action: row.permission_action as string,
        }) as SecurityAuditRecord["permission"],
      }),
    ...(row.resource_kind !== null &&
      row.resource_kind !== undefined && {
        resource: freeze({
          kind: row.resource_kind as string,
          namespaceId: row.resource_namespace_id as string,
          ...(row.resource_id !== null &&
            row.resource_id !== undefined && { id: row.resource_id as string }),
          ...(row.resource_principal_kind !== null &&
            row.resource_principal_kind !== undefined && {
              principal: freeze({
                kind: row.resource_principal_kind as NonNullable<
                  SecurityAuditRecord["actor"]
                >["kind"],
                id: row.resource_principal_id as string,
              }),
            }),
        }) as SecurityAuditRecord["resource"],
      }),
    ...(row.decision_allowed !== null &&
      row.decision_allowed !== undefined && {
        decision: freeze({
          allowed: row.decision_allowed as boolean,
          reason: row.decision_reason as string,
        }) as SecurityAuditRecord["decision"],
      }),
    ...((row.request_id ?? row.run_id ?? row.thread_id ?? row.tool_call_id) !== null &&
      (row.request_id ?? row.run_id ?? row.thread_id ?? row.tool_call_id) !== undefined && {
        correlation: freeze({
          ...(row.request_id !== null &&
            row.request_id !== undefined && {
              requestId: row.request_id as string,
            }),
          ...(row.run_id !== null && row.run_id !== undefined && { runId: row.run_id as string }),
          ...(row.thread_id !== null &&
            row.thread_id !== undefined && {
              threadId: row.thread_id as string,
            }),
          ...(row.tool_call_id !== null &&
            row.tool_call_id !== undefined && {
              toolCallId: row.tool_call_id as string,
            }),
        }),
      }),
    ...(metadata !== null &&
      metadata !== undefined && {
        metadata: freeze(structuredClone(metadata)),
      }),
    ...(row.previous_hash !== null &&
      row.previous_hash !== undefined && {
        previousHash: row.previous_hash as string,
      }),
    hash: row.hash as string,
    keyId: row.key_id as string,
  });
}

function outboxFromRow(row: DbRow): SecurityAuditOutboxRecord {
  return freeze({
    eventId: row.event_id as string,
    status: row.status as SecurityAuditOutboxRecord["status"],
    attempts: Number(row.attempts),
    nextAttemptAt: Number(row.next_attempt_at),
    ...(row.lease_token !== null &&
      row.lease_token !== undefined && {
        leaseToken: row.lease_token as string,
      }),
    ...(row.lease_owner !== null &&
      row.lease_owner !== undefined && {
        leaseOwner: row.lease_owner as string,
      }),
    ...(row.lease_expires_at !== null &&
      row.lease_expires_at !== undefined && {
        leaseExpiresAt: Number(row.lease_expires_at),
      }),
    ...(row.delivered_at !== null &&
      row.delivered_at !== undefined && {
        deliveredAt: Number(row.delivered_at),
      }),
    ...(row.last_error !== null &&
      row.last_error !== undefined && { lastError: row.last_error as string }),
  });
}

function denialFromRow(row: DbRow): SecurityDenialBucket {
  return freeze({
    key: row.key as string,
    windowStartedAt: Number(row.window_started_at),
    windowEndsAt: Number(row.window_ends_at),
    count: Number(row.count),
    suppressed: Number(row.suppressed),
  });
}

function grantFromRow(row: DbRow): BreakGlassGrant {
  return freeze({
    id: row.id as string,
    principal: freeze({
      kind: row.principal_kind as SecurityPrincipalRef["kind"],
      id: row.principal_id as string,
    }),
    tokenHash: row.token_hash as string,
    reason: row.reason as string,
    createdBy: row.created_by as string,
    createdAt: Number(row.created_at),
    expiresAt: Number(row.expires_at),
    ...(row.revoked_at !== null &&
      row.revoked_at !== undefined && { revokedAt: Number(row.revoked_at) }),
  });
}

function makePage<T>(rows: T[], limit: number, keyOf: (row: T) => string): SecurityStorePage<T> {
  const hasMore = rows.length > limit;
  const items = freeze(rows.slice(0, limit).map(clone));
  return freeze({
    items,
    ...(hasMore && items.length > 0 && { nextCursor: keyOf(items[items.length - 1]!) }),
  });
}

function normalizeEmail(value: unknown): string {
  const email = requiredString(value, "email", SECURITY_STORE_LIMITS.email).trim().toLowerCase();
  if (!email.includes("@")) throw new TypeError("email must be an address");
  return requiredString(email, "email", SECURITY_STORE_LIMITS.email);
}

function requiredString(
  value: unknown,
  label: string,
  limit: SecurityStoreTextLimit = SECURITY_STORE_LIMITS.identifier,
): string {
  if (typeof value !== "string" || value.trim().length === 0 || hasUnsafeStringCharacter(value))
    throw new TypeError(`${label} must be a non-empty safe string`);
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
  if (value.length > limit.maxCharacters * 2)
    throw new TypeError(`${label} exceeds the character limit`);
  let characters = 0;
  for (const _ of value)
    if (++characters > limit.maxCharacters)
      throw new TypeError(`${label} exceeds the character limit`);
  if (new TextEncoder().encode(value).byteLength > limit.maxBytes)
    throw new TypeError(`${label} exceeds the byte limit`);
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
      (point >= 0xd800 && point <= 0xdfff) ||
      point === 0xfeff
    )
      return true;
  }
  return false;
}

function finiteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new TypeError(`${label} must be finite`);
  return value;
}
function integerMs(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new TypeError(`${label} must be a safe integer number of milliseconds`);
  return value;
}
function positiveIntegerMs(value: unknown, label: string): number {
  const row = integerMs(value, label);
  if (row <= 0) throw new TypeError(`${label} must be positive`);
  return row;
}
function finiteSum(a: number, b: number, label: string): number {
  const row = a + b;
  if (!Number.isSafeInteger(row)) throw new TypeError(`${label} must be a safe integer`);
  return row;
}
function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new TypeError(`${label} must be a non-negative safe integer`);
  return value;
}
function positiveInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0)
    throw new TypeError(`${label} must be a positive safe integer`);
  return value;
}
function pageLimit(value: unknown, label: string): number {
  const limit =
    value === undefined ? SECURITY_STORE_LIMITS.pageSize : positiveInteger(value, label);
  if (limit > SECURITY_STORE_LIMITS.pageSize) throw new TypeError(`${label} is too large`);
  return limit;
}

function normalizeInitialBindings(
  value: ReadonlyArray<{
    readonly namespaceId: string;
    readonly accessRole: string;
  }>,
): ReadonlyArray<{
  readonly namespaceId: string;
  readonly accessRole: string;
}> {
  if (!Array.isArray(value) || value.length > SECURITY_STORE_LIMITS.initialBindings)
    throw new TypeError("initial bindings is too large");
  return Array.from(value, (binding) => {
    if (!binding || typeof binding !== "object")
      throw new TypeError("initial binding must be an object");
    const source = binding as unknown as Record<string, unknown>;
    return freeze({
      namespaceId: requiredString(
        source.namespaceId,
        "namespace id",
        SECURITY_STORE_LIMITS.namespace,
      ),
      accessRole: requiredString(source.accessRole, "access role"),
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
  if (kind !== "user" && kind !== "service-account" && kind !== "group")
    throw new TypeError("invalid access subject kind");
  return freeze({
    kind,
    id: requiredString(id, "access subject id"),
  }) as AccessSubjectRef;
}

function normalizeProviderIdentity(
  value: Omit<ProviderIdentity, "userId">,
): Omit<ProviderIdentity, "userId"> {
  if (!value || typeof value !== "object")
    throw new TypeError("provider identity must be an object");
  const source = value as unknown as Record<string, unknown>;
  return freeze({
    providerId: requiredString(source.providerId, "provider id"),
    issuer: requiredString(source.issuer, "issuer", SECURITY_STORE_LIMITS.issuer),
    subject: requiredString(source.subject, "provider subject"),
  });
}

function normalizeGroup(value: AccessGroup): AccessGroup {
  const source = value as unknown as Record<string, unknown>;
  return freeze({
    id: requiredString(source.id, "group id"),
    status: statusValue(source.status),
  });
}
function normalizeMembership(value: GroupMembership): GroupMembership {
  const source = value as unknown as Record<string, unknown>;
  return freeze({
    groupId: requiredString(source.groupId, "group id"),
    subject: normalizePrincipalRef(source.subject as SecurityPrincipalRef),
  });
}
function normalizeSubjectBinding(value: SubjectAccessBinding): SubjectAccessBinding {
  const source = value as unknown as Record<string, unknown>;
  return freeze({
    subject: normalizeAccessSubject(source.subject as AccessSubjectRef),
    namespaceId: requiredString(
      source.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    ),
    accessRole: requiredString(source.accessRole, "access role"),
  });
}

function normalizeKnowledgeAudienceGrant(value: KnowledgeAudienceGrant): KnowledgeAudienceGrant {
  const source = value as unknown as Record<string, unknown>;
  const audience = source.audience as Record<string, unknown> | undefined;
  if (!audience || (audience.kind !== "access-role" && audience.kind !== "group"))
    throw new TypeError("invalid audience kind");
  return freeze({
    namespaceId: requiredString(
      source.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    ),
    baseId: requiredString(source.baseId, "knowledge base id"),
    audience: freeze({
      kind: audience.kind,
      id: requiredString(audience.id, "audience id"),
    }),
  });
}

function normalizeServiceAccount(value: SecurityServiceAccount): SecurityServiceAccount {
  const source = value as unknown as Record<string, unknown>;
  if (typeof source.canActFor !== "boolean") throw new TypeError("canActFor must be boolean");
  return freeze({
    id: requiredString(source.id, "service account id"),
    status: statusValue(source.status),
    canActFor: source.canActFor,
  });
}

function normalizeCredential(value: ServiceAccountCredential): ServiceAccountCredential {
  const source = value as unknown as Record<string, unknown>;
  const expiresAt = source.expiresAt;
  const revokedAt = source.revokedAt;
  const row = freeze({
    id: requiredString(source.id, "credential id"),
    serviceAccountId: requiredString(source.serviceAccountId, "service account id"),
    secretHash: requiredString(source.secretHash, "credential hash", SECURITY_STORE_LIMITS.hash),
    createdAt: integerMs(source.createdAt, "credential created at"),
    ...(expiresAt !== undefined && {
      expiresAt: integerMs(expiresAt, "credential expires at"),
    }),
    ...(revokedAt !== undefined && {
      revokedAt: integerMs(revokedAt, "credential revoked at"),
    }),
  });
  if (row.expiresAt !== undefined && row.expiresAt <= row.createdAt)
    throw new TypeError("credential expiry must follow creation");
  return row;
}

function normalizeSession(value: SecuritySession): SecuritySession {
  const source = value as unknown as Record<string, unknown>;
  const authMethod = source.authMethod;
  const csrf = source.csrfTokenHash;
  const revoked = source.revokedAt;
  if (authMethod !== "oidc" && authMethod !== "service-account" && authMethod !== "break-glass")
    throw new TypeError("invalid session auth method");
  const row: SecuritySession = freeze({
    tokenHash: requiredString(source.tokenHash, "session token hash", SECURITY_STORE_LIMITS.hash),
    principal: normalizePrincipalRef(source.principal as SecurityPrincipalRef),
    createdAt: integerMs(source.createdAt, "session created at"),
    expiresAt: integerMs(source.expiresAt, "session expires at"),
    ...(csrf !== undefined && {
      csrfTokenHash: requiredString(csrf, "csrf token hash", SECURITY_STORE_LIMITS.hash),
    }),
    ...(revoked !== undefined && {
      revokedAt: integerMs(revoked, "session revoked at"),
    }),
    authMethod,
  });
  if (row.expiresAt <= row.createdAt) throw new TypeError("session expiry must follow creation");
  return row;
}

function normalizeAuditRecord(value: SecurityAuditRecord): SecurityAuditRecord {
  if (!value || typeof value !== "object") throw new TypeError("audit record must be an object");
  const source = value as unknown as Record<string, unknown>;
  const actor = source.actor;
  const effective = source.effective;
  const permission = source.permission;
  const resource = source.resource;
  const decision = source.decision;
  const correlation = source.correlation;
  const metadata = source.metadata;
  const previousHash = source.previousHash;
  const row: SecurityAuditRecord = freeze({
    id: requiredString(source.id, "audit event id"),
    namespaceId: requiredString(
      source.namespaceId,
      "namespace id",
      SECURITY_STORE_LIMITS.namespace,
    ),
    sequence: nonNegativeInteger(source.sequence, "audit sequence"),
    occurredAt: integerMs(source.occurredAt, "audit occurrence"),
    recordedAt: integerMs(source.recordedAt, "audit recorded time"),
    event: requiredString(source.event, "audit event", SECURITY_STORE_LIMITS.event),
    ...(actor !== undefined && {
      actor: normalizeAuditPrincipal(actor, "audit actor"),
    }),
    ...(effective !== undefined && {
      effective: normalizeAuditPrincipal(effective, "audit effective principal"),
    }),
    ...(permission !== undefined && {
      permission: normalizeAuditPermission(permission),
    }),
    ...(resource !== undefined && {
      resource: normalizeAuditResource(resource),
    }),
    ...(decision !== undefined && {
      decision: normalizeAuditDecision(decision),
    }),
    ...(correlation !== undefined && {
      correlation: normalizeAuditCorrelation(correlation),
    }),
    ...(metadata !== undefined && {
      metadata: normalizeAuditMetadata(metadata),
    }),
    ...(previousHash !== undefined && {
      previousHash: requiredString(previousHash, "previous audit hash", SECURITY_STORE_LIMITS.hash),
    }),
    hash: requiredString(source.hash, "audit hash", SECURITY_STORE_LIMITS.hash),
    keyId: requiredString(source.keyId, "audit key id"),
  });
  if ((row.actor === undefined) !== (row.effective === undefined))
    throw new TypeError("audit actor and effective principal must appear together");
  if (row.recordedAt < row.occurredAt)
    throw new TypeError("audit recorded time cannot precede occurrence");
  if (row.permission && row.resource && row.permission.resource !== row.resource.kind)
    throw new TypeError("audit permission and resource kinds must match");
  if (row.resource && row.resource.namespaceId !== row.namespaceId)
    throw new TypeError("audit resource namespace must match the audit namespace");
  return row;
}

function normalizeAuditPrincipal(
  value: unknown,
  label: string,
): NonNullable<SecurityAuditRecord["actor"]> {
  if (!value || typeof value !== "object") throw new TypeError(`${label} must be an object`);
  const source = value as Record<string, unknown>;
  const kind = source.kind;
  if (kind !== "operator" && kind !== "worker" && kind !== "user" && kind !== "service-account")
    throw new TypeError(`${label} has an invalid kind`);
  return freeze({ kind, id: requiredString(source.id, `${label} id`) });
}

function normalizeAuditPermission(value: unknown): SecurityAuditRecord["permission"] {
  if (!value || typeof value !== "object")
    throw new TypeError("audit permission must be an object");
  const source = value as Record<string, unknown>;
  const resource = source.resource;
  const action = source.action;
  if (typeof resource !== "string" || !(resource in SECURITY_ACTIONS))
    throw new TypeError("invalid audit permission resource");
  if (
    typeof action !== "string" ||
    !(SECURITY_ACTIONS[resource as keyof typeof SECURITY_ACTIONS] as readonly string[]).includes(
      action,
    )
  )
    throw new TypeError("invalid audit permission action");
  return freeze({ resource, action }) as SecurityAuditRecord["permission"];
}

function normalizeAuditResource(value: unknown): SecurityAuditRecord["resource"] {
  if (!value || typeof value !== "object") throw new TypeError("audit resource must be an object");
  const source = value as Record<string, unknown>;
  const kind = source.kind;
  if (typeof kind !== "string" || !(kind in SECURITY_ACTIONS))
    throw new TypeError("invalid audit resource kind");
  return freeze({
    kind,
    namespaceId: requiredString(
      source.namespaceId,
      "audit resource namespace",
      SECURITY_STORE_LIMITS.namespace,
    ),
    ...(source.id !== undefined && {
      id: requiredString(source.id, "audit resource id"),
    }),
    ...(source.principal !== undefined && {
      principal: normalizeAuditPrincipal(source.principal, "audit resource principal"),
    }),
  }) as SecurityAuditRecord["resource"];
}

function normalizeAuditDecision(value: unknown): SecurityAuditRecord["decision"] {
  if (!value || typeof value !== "object") throw new TypeError("audit decision must be an object");
  const source = value as Record<string, unknown>;
  const allowed = source.allowed;
  const reason = source.reason;
  if (typeof allowed !== "boolean") throw new TypeError("audit decision allowed must be boolean");
  if (typeof reason !== "string" || !(AUTHORIZATION_REASONS as readonly string[]).includes(reason))
    throw new TypeError("invalid audit decision reason");
  return freeze({ allowed, reason }) as SecurityAuditRecord["decision"];
}

function normalizeAuditCorrelation(
  value: unknown,
): NonNullable<SecurityAuditRecord["correlation"]> {
  if (!value || typeof value !== "object")
    throw new TypeError("audit correlation must be an object");
  const source = value as Record<string, unknown>;
  return freeze({
    ...(source.requestId !== undefined && {
      requestId: requiredString(source.requestId, "request id"),
    }),
    ...(source.runId !== undefined && {
      runId: requiredString(source.runId, "run id"),
    }),
    ...(source.threadId !== undefined && {
      threadId: requiredString(source.threadId, "thread id"),
    }),
    ...(source.toolCallId !== undefined && {
      toolCallId: requiredString(source.toolCallId, "tool call id"),
    }),
  });
}

function normalizeAuditMetadata(
  value: unknown,
): Readonly<Record<string, string | number | boolean | null>> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("audit metadata must be an object");
  const source = value as Record<string, unknown>;
  const keys = Object.keys(source).sort();
  if (keys.length > SECURITY_STORE_LIMITS.metadataEntries)
    throw new TypeError("audit metadata has too many entries");
  const result = Object.create(null) as Record<string, string | number | boolean | null>;
  for (const key of keys) {
    requiredString(key, "audit metadata key", SECURITY_STORE_LIMITS.metadataKey);
    const entry = source[key];
    if (typeof entry === "string") {
      assertStringWithinLimit(entry, "audit metadata string", SECURITY_STORE_LIMITS.metadataString);
      if (hasUnsafeStringCharacter(entry)) throw new TypeError("audit metadata string is unsafe");
      result[key] = entry;
    } else if (typeof entry === "number")
      result[key] = finiteNumber(entry, "audit metadata number");
    else if (typeof entry === "boolean" || entry === null) result[key] = entry;
    else throw new TypeError("audit metadata values must be scalar");
  }
  return freeze(result);
}

function normalizeBreakGlassGrant(value: BreakGlassGrant): BreakGlassGrant {
  if (!value || typeof value !== "object")
    throw new TypeError("break-glass grant must be an object");
  const source = value as unknown as Record<string, unknown>;
  const row = freeze({
    id: requiredString(source.id, "break-glass grant id"),
    principal: normalizePrincipalRef(source.principal as SecurityPrincipalRef),
    tokenHash: requiredString(
      source.tokenHash,
      "break-glass token hash",
      SECURITY_STORE_LIMITS.hash,
    ),
    reason: requiredString(source.reason, "break-glass reason", SECURITY_STORE_LIMITS.reason),
    createdBy: requiredString(source.createdBy, "break-glass creator"),
    createdAt: integerMs(source.createdAt, "break-glass created at"),
    expiresAt: integerMs(source.expiresAt, "break-glass expires at"),
    ...(source.revokedAt !== undefined && {
      revokedAt: integerMs(source.revokedAt, "break-glass revoked at"),
    }),
  });
  if (row.expiresAt <= row.createdAt)
    throw new TypeError("break-glass expiry must follow creation");
  return row;
}

function pendingAuditOutbox(eventId: string, value: unknown): SecurityAuditOutboxRecord {
  return freeze({
    eventId: requiredString(eventId, "event id"),
    status: "pending",
    attempts: 0,
    nextAttemptAt: integerMs(value, "outbox next attempt"),
  });
}

function validateBreakGlassSession(grant: BreakGlassGrant, session: SecuritySession): void {
  if (grant.revokedAt !== undefined || session.revokedAt !== undefined)
    throw new TypeError("new break-glass access cannot already be revoked");
  if (session.authMethod !== "break-glass")
    throw new TypeError("break-glass session must use the break-glass auth method");
  if (!samePrincipal(grant.principal, session.principal))
    throw new TypeError("break-glass session principal must match the grant");
  if (grant.tokenHash !== session.tokenHash)
    throw new TypeError("break-glass session token must match the grant");
  if (session.createdAt < grant.createdAt || session.expiresAt > grant.expiresAt)
    throw new TypeError("break-glass session lifetime must fit within the grant");
}

function validateBreakGlassAudit(
  audit: SecurityAuditRecord,
  grant: BreakGlassGrant,
  event: "break-glass.created" | "break-glass.revoked",
): void {
  if (
    audit.event !== event ||
    audit.namespaceId !== ORG_WIDE_NAMESPACE ||
    audit.actor?.kind !== "operator" ||
    (event === "break-glass.created" && audit.actor.id !== grant.createdBy) ||
    !audit.effective ||
    !samePrincipal(grant.principal, audit.effective as SecurityPrincipalRef) ||
    audit.decision?.allowed !== true ||
    audit.metadata?.breakGlassGrantId !== grant.id ||
    (event === "break-glass.revoked" && audit.metadata.revokedBy !== audit.actor.id)
  )
    throw new TypeError("break-glass audit does not identify the grant and principal");
}

function samePrincipal(a: SecurityPrincipalRef, b: SecurityPrincipalRef): boolean {
  return a.kind === b.kind && a.id === b.id;
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
function recordsEqual(a: unknown, b: unknown): boolean {
  return stableJson(a) === stableJson(b);
}
function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const source = value as Record<string, unknown>;
    return `{${Object.keys(source)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(source[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}
function isUniqueViolation(error: unknown): boolean {
  return (
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}
function isNumericOverflow(error: unknown): boolean {
  return (
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    ((error as { code?: unknown }).code === "22003" ||
      (error as { code?: unknown }).code === "23514")
  );
}
