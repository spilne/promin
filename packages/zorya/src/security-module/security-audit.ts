import { createHmac, timingSafeEqual } from "node:crypto";
import type { ToolAuditEntry, ToolAuditLogger } from "@promin/agent";
import type { RunSecurityContext } from "@promin/workflow";
import type {
  Authorizer,
  PrincipalRef,
  SecurityAuditEvent,
  SecurityAuditSink,
  SecurityContext,
  SecurityPermission,
  SecurityResourceRef,
} from "../server/security/security.ts";
import {
  SECURITY_STORE_LIMITS,
  type SecurityAuditCorrelation,
  type SecurityAuditRecord,
  type SecurityAuditScalar,
  type SecurityStorePage,
  type SecurityStoreV2,
} from "./security-store.ts";

export type SecurityAuditHmacKey = string | Uint8Array;

export interface SecurityAuditCheckpoint {
  readonly namespaceId: string;
  readonly sequence: number;
  readonly hash: string;
  readonly keyId: string;
}

export interface SecurityAuditEnvelope {
  readonly id?: string;
  readonly namespaceId: string;
  readonly occurredAt: number;
  readonly event: string;
  readonly actor?: PrincipalRef;
  readonly effective?: PrincipalRef;
  readonly permission?: SecurityPermission;
  readonly resource?: SecurityResourceRef;
  readonly decision?: SecurityAuditRecord["decision"];
  readonly correlation?: SecurityAuditCorrelation;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface DurableSecurityAuditConfig {
  readonly store: SecurityStoreV2;
  readonly activeKeyId: string;
  readonly keys: Readonly<Record<string, SecurityAuditHmacKey>>;
  readonly clock?: { readonly currentTimeMs: () => number };
  readonly idFactory?: () => string;
  readonly maxAppendAttempts?: number;
  /** When set, the audit row and a pending external-delivery row are committed atomically. */
  readonly outbox?: { readonly nextAttemptAt?: (recordedAt: number) => number };
}

export type SecurityAuditVerification =
  | { readonly valid: true; readonly checkpoint?: SecurityAuditCheckpoint }
  | {
      readonly valid: false;
      readonly reason:
        | "namespace_mismatch"
        | "sequence_mismatch"
        | "previous_hash_mismatch"
        | "unknown_key"
        | "hash_mismatch"
        | "suffix_truncated"
        | "checkpoint_mismatch";
      readonly sequence?: number;
    };

/**
 * Durable canonical audit sink. Each namespace is an independent HMAC chain;
 * the store serializes the head check and this sink retries a losing replica.
 */
export class DurableSecurityAudit implements SecurityAuditSink {
  private readonly store: SecurityStoreV2;
  private readonly activeKeyId: string;
  private readonly keys: Readonly<Record<string, SecurityAuditHmacKey>>;
  private readonly clock: { readonly currentTimeMs: () => number };
  private readonly idFactory: () => string;
  private readonly maxAppendAttempts: number;
  private readonly outbox: DurableSecurityAuditConfig["outbox"];

  constructor(config: DurableSecurityAuditConfig) {
    this.store = config.store;
    this.activeKeyId = boundedRequired(
      config.activeKeyId,
      "audit key id",
      SECURITY_STORE_LIMITS.identifier,
    );
    const copiedKeys: Record<string, SecurityAuditHmacKey> = {};
    for (const [id, key] of Object.entries(config.keys)) {
      boundedRequired(id, "audit key id", SECURITY_STORE_LIMITS.identifier);
      const bytes = keyBytes(key);
      if (bytes.byteLength < 32)
        throw new TypeError(`audit HMAC key '${id}' must be at least 32 bytes`);
      copiedKeys[id] = bytes.slice();
    }
    this.keys = Object.freeze(copiedKeys);
    if (this.keys[this.activeKeyId] === undefined)
      throw new TypeError("active audit HMAC key is missing");
    this.clock = config.clock ?? { currentTimeMs: () => Date.now() };
    this.idFactory = config.idFactory ?? (() => crypto.randomUUID());
    this.maxAppendAttempts = config.maxAppendAttempts ?? 128;
    if (!Number.isSafeInteger(this.maxAppendAttempts) || this.maxAppendAttempts < 1) {
      throw new TypeError("max audit append attempts must be a positive safe integer");
    }
    this.outbox = config.outbox;
  }

  async recordAudit(event: SecurityAuditEvent): Promise<void> {
    const metadata = event.metadata ?? {};
    const correlation = correlationFromMetadata(metadata);
    await this.append({
      namespaceId: event.namespaceId,
      occurredAt: parseOccurredAt(event.occurredAt),
      event: event.event,
      ...(event.context !== undefined && {
        actor: principalRef(event.context.actor),
        effective: principalRef(event.context.effective),
      }),
      ...(event.permission !== undefined && { permission: event.permission }),
      ...(event.resource !== undefined && { resource: event.resource }),
      ...(event.decision !== undefined && { decision: event.decision }),
      ...(correlation !== undefined && { correlation }),
      metadata,
    });
  }

  async append(envelope: SecurityAuditEnvelope): Promise<SecurityAuditRecord> {
    const id = boundedRequired(
      envelope.id ?? this.idFactory(),
      "audit event id",
      SECURITY_STORE_LIMITS.identifier,
    );
    const namespaceId = boundedRequired(
      envelope.namespaceId,
      "audit namespace",
      SECURITY_STORE_LIMITS.namespace,
    );
    const event = boundedRequired(envelope.event, "audit event", SECURITY_STORE_LIMITS.event);
    const occurredAt = safeTime(envelope.occurredAt, "audit occurrence");
    const recordedAt = Math.max(
      occurredAt,
      safeTime(this.clock.currentTimeMs(), "audit recorded time"),
    );
    const actor = envelope.actor === undefined ? undefined : principalRef(envelope.actor);
    const effective =
      envelope.effective === undefined ? undefined : principalRef(envelope.effective);
    const permission =
      envelope.permission === undefined
        ? undefined
        : ({
            resource: envelope.permission.resource,
            action: envelope.permission.action,
          } as SecurityPermission);
    const resource =
      envelope.resource === undefined
        ? undefined
        : ({
            kind: envelope.resource.kind,
            namespaceId: envelope.resource.namespaceId,
            ...(envelope.resource.id !== undefined && { id: envelope.resource.id }),
            ...(envelope.resource.principal !== undefined && {
              principal: principalRef(envelope.resource.principal),
            }),
          } as SecurityResourceRef);
    const decision =
      envelope.decision === undefined
        ? undefined
        : ({
            allowed: envelope.decision.allowed,
            reason: envelope.decision.reason,
          } as SecurityAuditRecord["decision"]);
    const correlation = snapshotCorrelation(envelope.correlation);
    const metadata = sanitizeAuditMetadata(envelope.metadata);
    const activeKey = this.keys[this.activeKeyId]!;

    for (let attempt = 0; attempt < this.maxAppendAttempts; attempt += 1) {
      const existing = await this.store.getAuditRecord(id);
      if (existing !== undefined) {
        if (
          existingMatchesEnvelope(existing, {
            id,
            namespaceId,
            occurredAt,
            recordedAt,
            event,
            actor,
            effective,
            permission,
            resource,
            decision,
            correlation,
            metadata,
            keyId: this.activeKeyId,
          })
        ) {
          return existing;
        }
        throw new Error(`Conflicting audit event: ${id}`);
      }
      const head = await auditHead(this.store, namespaceId);
      const unsigned = {
        id,
        namespaceId,
        sequence: (head?.sequence ?? 0) + 1,
        occurredAt,
        recordedAt,
        event,
        ...(actor !== undefined && { actor }),
        ...(effective !== undefined && { effective }),
        ...(permission !== undefined && { permission }),
        ...(resource !== undefined && { resource }),
        ...(decision !== undefined && { decision }),
        ...(correlation !== undefined && { correlation }),
        ...(metadata !== undefined && { metadata }),
        ...(head !== undefined && { previousHash: head.hash }),
        keyId: this.activeKeyId,
      } satisfies Omit<SecurityAuditRecord, "hash">;
      const record: SecurityAuditRecord = Object.freeze({
        ...unsigned,
        hash: computeSecurityAuditHash(unsigned, activeKey),
      });
      try {
        await this.store.appendAuditWithOutbox({
          record,
          ...(this.outbox !== undefined && {
            outbox: { nextAttemptAt: this.outbox.nextAttemptAt?.(recordedAt) ?? recordedAt },
          }),
        });
        return record;
      } catch (error) {
        if (!isChainBoundaryConflict(error)) throw error;
      }
    }
    throw new Error(`Audit append contention exceeded ${this.maxAppendAttempts} attempts`);
  }

  async list(params: {
    readonly namespaceId: string;
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<SecurityStorePage<SecurityAuditRecord>> {
    return this.store.listAuditRecords(params);
  }

  async verify(
    namespaceId: string,
    checkpoint?: SecurityAuditCheckpoint,
  ): Promise<SecurityAuditVerification> {
    const rows = await allAuditRows(this.store, namespaceId);
    return verifySecurityAuditRecords(rows, this.keys, checkpoint);
  }
}

/** Stable JSON plus HMAC-SHA256; no locale, insertion order or whitespace participates. */
export function computeSecurityAuditHash(
  record: Omit<SecurityAuditRecord, "hash">,
  key: SecurityAuditHmacKey,
): string {
  return createHmac("sha256", keyBytes(key)).update(canonicalJson(record)).digest("base64url");
}

export function verifySecurityAuditRecords(
  records: ReadonlyArray<SecurityAuditRecord>,
  keys: Readonly<Record<string, SecurityAuditHmacKey>>,
  checkpoint?: SecurityAuditCheckpoint,
): SecurityAuditVerification {
  const namespaceId = checkpoint?.namespaceId ?? records[0]?.namespaceId;
  let previous: SecurityAuditRecord | undefined;
  for (const record of records) {
    if (namespaceId !== undefined && record.namespaceId !== namespaceId) {
      return { valid: false, reason: "namespace_mismatch", sequence: record.sequence };
    }
    if (record.sequence !== (previous?.sequence ?? 0) + 1) {
      return { valid: false, reason: "sequence_mismatch", sequence: record.sequence };
    }
    if (record.previousHash !== previous?.hash) {
      return { valid: false, reason: "previous_hash_mismatch", sequence: record.sequence };
    }
    const key = keys[record.keyId];
    if (key === undefined)
      return { valid: false, reason: "unknown_key", sequence: record.sequence };
    const { hash: _hash, ...unsigned } = record;
    const expected = computeSecurityAuditHash(unsigned, key);
    if (!sameHash(record.hash, expected)) {
      return { valid: false, reason: "hash_mismatch", sequence: record.sequence };
    }
    previous = record;
  }
  if (checkpoint !== undefined) {
    const row = records[checkpoint.sequence - 1];
    if (row === undefined)
      return { valid: false, reason: "suffix_truncated", sequence: checkpoint.sequence };
    if (
      row.namespaceId !== checkpoint.namespaceId ||
      row.hash !== checkpoint.hash ||
      row.keyId !== checkpoint.keyId
    ) {
      return { valid: false, reason: "checkpoint_mismatch", sequence: checkpoint.sequence };
    }
  }
  return {
    valid: true,
    ...(previous !== undefined && {
      checkpoint: {
        namespaceId: previous.namespaceId,
        sequence: previous.sequence,
        hash: previous.hash,
        keyId: previous.keyId,
      },
    }),
  };
}

export interface SecurityAuditSearchFilter {
  readonly event?: string;
  readonly actor?: PrincipalRef;
  readonly effective?: PrincipalRef;
  readonly decisionAllowed?: boolean;
  readonly toolCallId?: string;
  readonly occurredFrom?: number;
  readonly occurredTo?: number;
}

/** Namespace-authorized, cursor-paged audit reads which audit their own sensitive access. */
export class SecurityAuditReader {
  constructor(
    private readonly store: SecurityStoreV2,
    private readonly authorizer: Authorizer,
    private readonly audit: DurableSecurityAudit,
    private readonly clock: { readonly currentTimeMs: () => number } = {
      currentTimeMs: () => Date.now(),
    },
  ) {}

  async search(params: {
    readonly context: SecurityContext;
    readonly namespaceId: string;
    readonly filter?: SecurityAuditSearchFilter;
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<SecurityStorePage<SecurityAuditRecord>> {
    await this.authorizeRead(params.context, params.namespaceId, "search", params.filter);
    const limit = pageLimit(params.limit);
    const items: SecurityAuditRecord[] = [];
    let cursor = params.cursor;
    while (items.length < limit + 1) {
      const page = await this.store.listAuditRecords({
        namespaceId: params.namespaceId,
        ...(cursor !== undefined && { cursor }),
        limit: SECURITY_STORE_LIMITS.pageSize,
      });
      for (const row of page.items) if (matches(row, params.filter)) items.push(row);
      if (items.length >= limit + 1 || page.nextCursor === undefined) break;
      cursor = page.nextCursor;
    }
    const visible = items.slice(0, limit);
    return {
      items: visible,
      ...(items.length > limit && visible.length > 0 && { nextCursor: visible.at(-1)!.id }),
    };
  }

  async exportPage(params: {
    readonly context: SecurityContext;
    readonly namespaceId: string;
    readonly cursor?: string;
    readonly limit?: number;
  }): Promise<{ readonly jsonl: string; readonly nextCursor?: string }> {
    await this.authorizeRead(params.context, params.namespaceId, "export");
    const page = await this.store.listAuditRecords({
      namespaceId: params.namespaceId,
      ...(params.cursor !== undefined && { cursor: params.cursor }),
      limit: pageLimit(params.limit),
    });
    return {
      jsonl: page.items.map((row) => canonicalJson(row)).join("\n"),
      ...(page.nextCursor !== undefined && { nextCursor: page.nextCursor }),
    };
  }

  private async authorizeRead(
    context: SecurityContext,
    namespaceId: string,
    action: "search" | "export",
    filter?: SecurityAuditSearchFilter,
  ): Promise<void> {
    const permission = { resource: "audit", action } as const;
    const resource = { kind: "audit", namespaceId, id: namespaceId } as const;
    const decision = await this.authorizer.authorize({ context, permission, resource });
    const event: SecurityAuditEvent = {
      event: `audit.${action}.authorization`,
      occurredAt: new Date(this.clock.currentTimeMs()).toISOString(),
      context,
      namespaceId,
      permission,
      resource,
      decision,
      metadata: action === "search" ? searchShape(filter) : { format: "jsonl" },
    };
    try {
      await this.audit.recordAudit(event);
    } catch (error) {
      if (decision.allowed) throw error;
    }
    if (!decision.allowed) throw new SecurityAuditAccessDeniedError(action, namespaceId);
  }
}

export class SecurityAuditAccessDeniedError extends Error {
  constructor(action: "search" | "export", namespaceId: string) {
    super(`Audit ${action} denied for namespace '${namespaceId}'`);
    this.name = "SecurityAuditAccessDeniedError";
  }
}

/** Maps elevated-tool intent/outcome records into the same durable actor/effective chain. */
export class SecurityToolAuditBridge implements ToolAuditLogger {
  constructor(
    private readonly audit: DurableSecurityAudit,
    private readonly clock: { readonly currentTimeMs: () => number } = {
      currentTimeMs: () => Date.now(),
    },
  ) {}

  async record(entry: ToolAuditEntry): Promise<void> {
    // The intersection keeps project-reference builds compatible while the
    // agent package's generated declaration is refreshed in the same build.
    const securityContext = (
      entry as ToolAuditEntry & {
        readonly securityContext?: RunSecurityContext;
      }
    ).securityContext;
    await this.audit.append({
      namespaceId: entry.namespaceId,
      occurredAt: this.clock.currentTimeMs(),
      event: `tool.elevated.${entry.phase}`,
      ...(securityContext !== undefined && {
        actor: principalRef(securityContext.actor),
        effective: principalRef(securityContext.effective ?? securityContext.actor),
      }),
      resource: { kind: "toolset", namespaceId: entry.namespaceId, id: entry.toolName },
      correlation: {
        ...(entry.toolCallId !== undefined && { toolCallId: entry.toolCallId }),
      },
      metadata: {
        toolName: entry.toolName,
        action: entry.action,
        phase: entry.phase,
        callerResourceId: entry.resourceId,
        ...(entry.agentId !== undefined && { agentId: entry.agentId }),
        ...(entry.target !== undefined && { target: entry.target }),
        ...(entry.phase === "failed" && { failed: true }),
        ...flattenToolMetadata(entry.meta),
      },
    });
  }
}

async function auditHead(
  store: SecurityStoreV2,
  namespaceId: string,
): Promise<SecurityAuditRecord | undefined> {
  let cursor: string | undefined;
  let head: SecurityAuditRecord | undefined;
  do {
    const page = await store.listAuditRecords({
      namespaceId,
      ...(cursor !== undefined && { cursor }),
      limit: SECURITY_STORE_LIMITS.pageSize,
    });
    head = page.items.at(-1) ?? head;
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  return head;
}

async function allAuditRows(
  store: SecurityStoreV2,
  namespaceId: string,
): Promise<SecurityAuditRecord[]> {
  const rows: SecurityAuditRecord[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.listAuditRecords({
      namespaceId,
      ...(cursor !== undefined && { cursor }),
      limit: SECURITY_STORE_LIMITS.pageSize,
    });
    rows.push(...page.items);
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  return rows;
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const object = value as Readonly<Record<string, unknown>>;
  return `{${Object.keys(object)
    .filter((key) => object[key] !== undefined)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`)
    .join(",")}}`;
}

function sanitizeAuditMetadata(
  source: Readonly<Record<string, unknown>> | undefined,
): Readonly<Record<string, SecurityAuditScalar>> | undefined {
  if (source === undefined) return undefined;
  const result = Object.create(null) as Record<string, SecurityAuditScalar>;
  const keys = Object.keys(source).sort().slice(0, SECURITY_STORE_LIMITS.metadataEntries);
  for (const key of keys) {
    if (!safeBoundedString(key, SECURITY_STORE_LIMITS.metadataKey)) continue;
    const value = source[key];
    if (SENSITIVE_KEY.test(key)) {
      result[key] = "[REDACTED]";
    } else if (typeof value === "string") {
      result[key] = safeMetadataString(value);
    } else if (typeof value === "number" && Number.isFinite(value)) {
      result[key] = value;
    } else if (typeof value === "boolean" || value === null) {
      result[key] = value;
    }
  }
  return Object.keys(result).length === 0 ? undefined : Object.freeze(result);
}

function flattenToolMetadata(
  source: Readonly<Record<string, unknown>> | undefined,
): Readonly<Record<string, unknown>> {
  if (source === undefined) return {};
  const flattened: Record<string, unknown> = {};
  for (const key of Object.keys(source).sort()) {
    const value = source[key];
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean" ||
      value === null
    ) {
      flattened[`toolMeta.${key}`] = value;
    }
  }
  return flattened;
}

function correlationFromMetadata(
  metadata: Readonly<Record<string, SecurityAuditScalar>>,
): SecurityAuditCorrelation | undefined {
  const result: Record<string, string> = {};
  for (const key of ["requestId", "runId", "threadId", "toolCallId"] as const) {
    const value = metadata[key];
    if (typeof value === "string" && value.length > 0) result[key] = value;
  }
  return Object.keys(result).length === 0 ? undefined : result;
}

function snapshotCorrelation(
  correlation: SecurityAuditCorrelation | undefined,
): SecurityAuditCorrelation | undefined {
  if (correlation === undefined) return undefined;
  return {
    ...(correlation.requestId !== undefined && { requestId: correlation.requestId }),
    ...(correlation.runId !== undefined && { runId: correlation.runId }),
    ...(correlation.threadId !== undefined && { threadId: correlation.threadId }),
    ...(correlation.toolCallId !== undefined && { toolCallId: correlation.toolCallId }),
  };
}

function matches(row: SecurityAuditRecord, filter: SecurityAuditSearchFilter | undefined): boolean {
  if (filter === undefined) return true;
  return (
    (filter.event === undefined || row.event === filter.event) &&
    (filter.actor === undefined || samePrincipal(row.actor, filter.actor)) &&
    (filter.effective === undefined || samePrincipal(row.effective, filter.effective)) &&
    (filter.decisionAllowed === undefined || row.decision?.allowed === filter.decisionAllowed) &&
    (filter.toolCallId === undefined || row.correlation?.toolCallId === filter.toolCallId) &&
    (filter.occurredFrom === undefined || row.occurredAt >= filter.occurredFrom) &&
    (filter.occurredTo === undefined || row.occurredAt <= filter.occurredTo)
  );
}

function existingMatchesEnvelope(
  row: SecurityAuditRecord,
  expected: {
    readonly id: string;
    readonly namespaceId: string;
    readonly occurredAt: number;
    readonly recordedAt: number;
    readonly event: string;
    readonly actor?: PrincipalRef;
    readonly effective?: PrincipalRef;
    readonly permission?: SecurityPermission;
    readonly resource?: SecurityResourceRef;
    readonly decision?: SecurityAuditRecord["decision"];
    readonly correlation?: SecurityAuditCorrelation;
    readonly metadata?: Readonly<Record<string, SecurityAuditScalar>>;
    readonly keyId: string;
  },
): boolean {
  return (
    canonicalJson({
      id: row.id,
      namespaceId: row.namespaceId,
      occurredAt: row.occurredAt,
      recordedAt: row.recordedAt,
      event: row.event,
      actor: row.actor,
      effective: row.effective,
      permission: row.permission,
      resource: row.resource,
      decision: row.decision,
      correlation: row.correlation,
      metadata: row.metadata,
      keyId: row.keyId,
    }) === canonicalJson(expected)
  );
}

function searchShape(
  filter: SecurityAuditSearchFilter | undefined,
): Readonly<Record<string, SecurityAuditScalar>> {
  return {
    filtered: filter !== undefined,
    byEvent: filter?.event !== undefined,
    byActor: filter?.actor !== undefined,
    byEffective: filter?.effective !== undefined,
    byDecision: filter?.decisionAllowed !== undefined,
    byToolCall: filter?.toolCallId !== undefined,
    byTime: filter?.occurredFrom !== undefined || filter?.occurredTo !== undefined,
  };
}

function principalRef(value: { readonly kind: string; readonly id: string }): PrincipalRef {
  return { kind: value.kind as PrincipalRef["kind"], id: value.id };
}

function samePrincipal(left: PrincipalRef | undefined, right: PrincipalRef): boolean {
  return left?.kind === right.kind && left.id === right.id;
}

function parseOccurredAt(value: string): number {
  const parsed = Date.parse(value);
  if (!Number.isSafeInteger(parsed))
    throw new TypeError("audit occurrence must be a valid ISO timestamp");
  return parsed;
}

function safeTime(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new TypeError(`${label} must be a non-negative safe integer`);
  return value;
}

function pageLimit(value: number | undefined): number {
  const limit = value ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > SECURITY_STORE_LIMITS.pageSize) {
    throw new TypeError(`audit page limit must be between 1 and ${SECURITY_STORE_LIMITS.pageSize}`);
  }
  return limit;
}

function boundedRequired(
  value: string,
  label: string,
  limit: { readonly maxCharacters: number; readonly maxBytes: number },
): string {
  if (typeof value !== "string" || value.trim().length === 0 || !safeBoundedString(value, limit)) {
    throw new TypeError(`${label} must be a non-empty bounded safe string`);
  }
  return value;
}

function safeBoundedString(
  value: string,
  limit: { readonly maxCharacters: number; readonly maxBytes: number },
): boolean {
  return (
    !UNSAFE_TEXT.test(value) &&
    [...value].length <= limit.maxCharacters &&
    new TextEncoder().encode(value).byteLength <= limit.maxBytes
  );
}

function safeMetadataString(value: string): string {
  if (UNSAFE_TEXT.test(value)) return "[REDACTED]";
  const chars = [...value];
  let end = Math.min(chars.length, SECURITY_STORE_LIMITS.metadataString.maxCharacters);
  while (
    end > 0 &&
    new TextEncoder().encode(chars.slice(0, end).join("")).byteLength >
      SECURITY_STORE_LIMITS.metadataString.maxBytes
  )
    end -= 1;
  return chars.slice(0, end).join("");
}

function keyBytes(key: SecurityAuditHmacKey): Uint8Array {
  return typeof key === "string" ? new TextEncoder().encode(key) : key;
}

function sameHash(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.byteLength === b.byteLength && timingSafeEqual(a, b);
}

function isChainBoundaryConflict(error: unknown): boolean {
  return error instanceof Error && error.message === "Audit chain boundary mismatch";
}

const SENSITIVE_KEY =
  /(?:authorization|cookie|token|secret|password|credential|api.?key|query|document|result|body|content|error)/i;
const UNSAFE_TEXT = /[\p{Cc}\p{Cf}]/u;
