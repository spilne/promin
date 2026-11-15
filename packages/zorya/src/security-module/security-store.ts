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
