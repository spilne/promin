// ---------------------------------------------------------------------------
// @promin/zorya/security — optional enterprise identity and access storage.
// Kept off the root entry so hosts that do not configure enterprise security
// do not load its implementation or future provider dependencies.
// ---------------------------------------------------------------------------

export {
  InMemorySecurityStore,
  type InMemorySecurityStoreConfig,
} from "./security-module/in-memory-security-store.ts";
export type { LoginIdentity, LoginProvider } from "./security-module/login-provider.ts";
export {
  oidcLoginProvider,
  type OidcLoginProviderConfig,
} from "./security-module/oidc-login-provider.ts";
export type {
  AccessGroup,
  AccessBinding,
  AccessSubjectRef,
  BreakGlassGrant,
  GroupMembership,
  KnowledgeAudienceGrant,
  KnowledgeGrant,
  ProviderIdentity,
  SecurityAuditOutboxRecord,
  SecurityAuditCorrelation,
  SecurityAuditRecord,
  SecurityAuditScalar,
  SecurityDenialBucket,
  SecurityPrincipalRef,
  SecurityServiceAccount,
  SecuritySession,
  SecurityStore,
  SecurityStoreTextLimit,
  SecurityStoreV2,
  SecurityUser,
  ServiceAccountCredential,
  SubjectAccessBinding,
} from "./security-module/security-store.ts";
export { SECURITY_STORE_LIMITS } from "./security-module/security-store.ts";
export {
  createZoryaSecurity,
  type ServiceAccountConfig,
  type ZoryaSecurityConfig,
} from "./security-module/zorya-security.ts";
