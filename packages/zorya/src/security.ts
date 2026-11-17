// ---------------------------------------------------------------------------
// @promin/zorya/security — optional enterprise identity and access storage.
// Kept off the root entry so hosts that do not configure enterprise security
// do not load its implementation or future provider dependencies.
// ---------------------------------------------------------------------------

export {
  InMemorySecurityStore,
  type InMemorySecurityStoreConfig,
} from "./security-module/in-memory-security-store.ts";
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
  SecurityStorePage,
  SecurityStoreTextLimit,
  SecurityUser,
  ServiceAccountCredential,
  SubjectAccessBinding,
} from "./security-module/security-store.ts";
export { SECURITY_STORE_LIMITS } from "./security-module/security-store.ts";
export {
  BoundedDenialAudit,
  computeSecurityAuditHash,
  DurableSecurityAudit,
  SecurityAuditAccessDeniedError,
  SecurityAuditReader,
  SecurityToolAuditBridge,
  verifySecurityAuditRecords,
  type DurableSecurityAuditConfig,
  type BoundedDenialAuditConfig,
  type SecurityAuditCheckpoint,
  type SecurityAuditEnvelope,
  type SecurityAuditHmacKey,
  type SecurityAuditSearchFilter,
  type SecurityAuditVerification,
} from "./security-module/security-audit.ts";
export {
  SecurityAuditOutboxWorker,
  type SecurityAuditDestination,
  type SecurityAuditOutboxMetrics,
  type SecurityAuditOutboxReport,
  type SecurityAuditOutboxWorkerConfig,
} from "./security-module/security-audit-outbox.ts";
export {
  breakGlassGrantIdFromToken,
  BreakGlassAccessManager,
  type BreakGlassAccessManagerConfig,
  type IssueBreakGlassAccessParams,
  type IssuedBreakGlassAccess,
} from "./security-module/break-glass.ts";
export {
  createZoryaSecurity,
  type ServiceAccountConfig,
  type ZoryaSecurityConfig,
} from "./security-module/zorya-security.ts";
