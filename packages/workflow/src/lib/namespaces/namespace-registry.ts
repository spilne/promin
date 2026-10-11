// ---------------------------------------------------------------------------
// NamespaceRegistry — the authoritative list of namespaces.
//
// Runs, schedules, agent memory and secrets carry a namespace as an opaque
// partition key; the engine never checks that it exists. A host that wants
// namespaces to be explicit entities (created before use, archived when a
// tenant leaves, carrying a capability policy) keeps them here and checks
// requests against it at its boundary (Zorya does, when configured with
// one). Backends: in-memory (below), SQLite and Postgres.
// ---------------------------------------------------------------------------

import { TaggedError } from "@spilne/perfect-core";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

/** `active` namespaces accept new work; `archived` ones keep their data but refuse new work. */
export const NAMESPACE_STATUSES = ["active", "archived"] as const;
export type NamespaceStatus = (typeof NAMESPACE_STATUSES)[number];

/**
 * A namespace id: 1–128 characters, letters, digits and `_ . : -`, starting
 * with a letter or digit. The same rule applies wherever a namespace name
 * is accepted (Zorya validates run, schedule and registry namespaces with it).
 */
export const NAMESPACE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
/** Longest `displayName`, after trimming. */
export const MAX_NAMESPACE_DISPLAY_NAME_LENGTH = 128;
/** Longest `description`. */
export const MAX_NAMESPACE_DESCRIPTION_LENGTH = 1024;

/** Whether `id` is a well-formed namespace id (`NAMESPACE_ID_PATTERN`). */
export function isValidNamespaceId(id: string): boolean {
  return NAMESPACE_ID_PATTERN.test(id);
}

/**
 * Per-namespace capability policy. Stored and returned as given (after
 * validation); the registry does not enforce it. Hosts read it where they
 * make the decision (e.g. an agent resolver capping `maxTokensPerTurn`).
 */
export interface NamespaceCapabilities {
  readonly ai?: {
    readonly enabled?: boolean;
    readonly providers?: readonly string[];
    readonly models?: readonly string[];
    readonly maxTokensPerTurn?: number;
    readonly maxTurnsPerRun?: number;
  };
  readonly agents?: {
    readonly enabled?: boolean;
    readonly maxConcurrentThreads?: number;
    readonly maxInvocationsPerMinute?: number;
  };
  readonly workflows?: {
    readonly enabled?: boolean;
    readonly maxConcurrentRuns?: number;
    readonly maxRunMs?: number;
  };
  readonly tools?: {
    readonly network?: boolean;
    readonly filesystem?: boolean;
    readonly shell?: boolean;
  };
}

export interface NamespaceRecord {
  readonly id: string;
  /** Human label. Defaults to the id. */
  readonly displayName: string;
  readonly description: string | null;
  readonly status: NamespaceStatus;
  readonly capabilities: NamespaceCapabilities;
  /** Free-form JSON. */
  readonly metadata: Record<string, unknown>;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

export interface CreateNamespaceInput {
  readonly id: string;
  readonly displayName?: string;
  readonly description?: string | null;
  readonly capabilities?: NamespaceCapabilities;
  readonly metadata?: Record<string, unknown>;
}

/** Fields `update` changes; omitted fields keep their value. `description: null` clears it. */
export interface NamespacePatch {
  readonly displayName?: string;
  readonly description?: string | null;
  readonly status?: NamespaceStatus;
  /** Replaces the whole policy. */
  readonly capabilities?: NamespaceCapabilities;
  /** Replaces the whole metadata object. */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Durable namespace store. Every method is async; every backend validates
 * input the same way (`InvalidNamespaceError`) and returns records ordered
 * by id.
 */
export interface NamespaceRegistry {
  /**
   * Create an `active` namespace. Throws `NamespaceExistsError` when the id
   * is taken (of two concurrent creates of one id, exactly one succeeds) and
   * `InvalidNamespaceError` for malformed input.
   */
  create(input: CreateNamespaceInput): Promise<NamespaceRecord>;
  /** The namespace, or null (also for an id that is not well-formed). */
  get(id: string): Promise<NamespaceRecord | null>;
  /** All namespaces, or those with `status`, ordered by id. */
  list(params?: { readonly status?: NamespaceStatus }): Promise<NamespaceRecord[]>;
  /**
   * Apply `patch` atomically: concurrent updates of different fields both
   * land. Throws `NamespaceNotFoundError` for an unknown id and
   * `InvalidNamespaceError` for a malformed patch.
   */
  update(params: { readonly id: string; readonly patch: NamespacePatch }): Promise<NamespaceRecord>;
  /** Remove the namespace. False when there was none. */
  delete(id: string): Promise<boolean>;
}

export class NamespaceNotFoundError extends TaggedError("NamespaceNotFoundError")<{
  readonly namespaceId: string;
  readonly message: string;
}>() {
  static of(namespaceId: string): NamespaceNotFoundError {
    return new NamespaceNotFoundError({
      namespaceId,
      message: `Unknown namespace "${namespaceId}"`,
    });
  }
}

export class NamespaceExistsError extends TaggedError("NamespaceExistsError")<{
  readonly namespaceId: string;
  readonly message: string;
}>() {
  static of(namespaceId: string): NamespaceExistsError {
    return new NamespaceExistsError({
      namespaceId,
      message: `Namespace "${namespaceId}" already exists`,
    });
  }
}

/** Which input field failed validation. */
export type NamespaceField =
  | "id"
  | "displayName"
  | "description"
  | "status"
  | "capabilities"
  | "metadata";

export class InvalidNamespaceError extends TaggedError("InvalidNamespaceError")<{
  readonly field: NamespaceField;
  readonly message: string;
}>() {}

function invalid(field: NamespaceField, message: string): InvalidNamespaceError {
  return new InvalidNamespaceError({ field, message: `${field}: ${message}` });
}

// ---------------------------------------------------------------------------
// Validation shared by every backend (exported through
// `@promin/workflow/storage-kit`).
// ---------------------------------------------------------------------------

/** A create input with every field validated and defaulted. */
export interface NormalizedNamespaceCreate {
  readonly id: string;
  readonly displayName: string;
  readonly description: string | null;
  readonly capabilities: NamespaceCapabilities;
  readonly metadata: Record<string, unknown>;
}

/** Validate a create input; throws `InvalidNamespaceError`. */
export function normalizeNamespaceCreate(input: CreateNamespaceInput): NormalizedNamespaceCreate {
  if (typeof input.id !== "string" || !isValidNamespaceId(input.id)) {
    throw invalid("id", `must match ${NAMESPACE_ID_PATTERN}`);
  }
  return {
    id: input.id,
    displayName: normalizeDisplayName({ value: input.displayName, fallback: input.id }),
    description: normalizeDescription(input.description ?? null),
    capabilities: normalizeNamespaceCapabilities(input.capabilities ?? {}),
    metadata: normalizeMetadata(input.metadata ?? {}),
  };
}

/** Validate a patch; throws `InvalidNamespaceError`. A blank `displayName` resets it to the id. */
export function normalizeNamespacePatch(params: {
  readonly id: string;
  readonly patch: NamespacePatch;
}): NamespacePatch {
  const { id, patch } = params;
  return {
    ...(patch.displayName !== undefined && {
      displayName: normalizeDisplayName({ value: patch.displayName, fallback: id }),
    }),
    ...(patch.description !== undefined && {
      description: normalizeDescription(patch.description),
    }),
    ...(patch.status !== undefined && { status: parseNamespaceStatus(patch.status) }),
    ...(patch.capabilities !== undefined && {
      capabilities: normalizeNamespaceCapabilities(patch.capabilities),
    }),
    ...(patch.metadata !== undefined && { metadata: normalizeMetadata(patch.metadata) }),
  };
}

/** Decode a stored or requested status; throws `InvalidNamespaceError` for anything else. */
export function parseNamespaceStatus(value: unknown): NamespaceStatus {
  if (value === "active" || value === "archived") return value;
  throw invalid("status", `must be one of ${NAMESPACE_STATUSES.join(", ")}`);
}

function normalizeDisplayName(params: { value: unknown; fallback: string }): string {
  if (params.value === undefined) return params.fallback;
  if (typeof params.value !== "string") throw invalid("displayName", "must be a string");
  const label = params.value.trim();
  if (label.length > MAX_NAMESPACE_DISPLAY_NAME_LENGTH) {
    throw invalid("displayName", `must be at most ${MAX_NAMESPACE_DISPLAY_NAME_LENGTH} characters`);
  }
  return label.length > 0 ? label : params.fallback;
}

function normalizeDescription(value: unknown): string | null {
  if (value === null) return null;
  if (typeof value !== "string") throw invalid("description", "must be a string or null");
  if (value.length > MAX_NAMESPACE_DESCRIPTION_LENGTH) {
    throw invalid("description", `must be at most ${MAX_NAMESPACE_DESCRIPTION_LENGTH} characters`);
  }
  return value;
}

type CapabilityFieldKind = "boolean" | "positiveInteger" | "stringArray";

const CAPABILITY_SPEC: Record<keyof NamespaceCapabilities, Record<string, CapabilityFieldKind>> = {
  ai: {
    enabled: "boolean",
    providers: "stringArray",
    models: "stringArray",
    maxTokensPerTurn: "positiveInteger",
    maxTurnsPerRun: "positiveInteger",
  },
  agents: {
    enabled: "boolean",
    maxConcurrentThreads: "positiveInteger",
    maxInvocationsPerMinute: "positiveInteger",
  },
  workflows: {
    enabled: "boolean",
    maxConcurrentRuns: "positiveInteger",
    maxRunMs: "positiveInteger",
  },
  tools: {
    network: "boolean",
    filesystem: "boolean",
    shell: "boolean",
  },
};

/**
 * Validate a capability policy: only the known sections and fields, each of
 * its declared type. Unknown keys are rejected rather than dropped, so a
 * typo never silently loses a limit.
 */
export function normalizeNamespaceCapabilities(value: unknown): NamespaceCapabilities {
  if (!isPlainObject(value)) throw invalid("capabilities", "must be an object");
  const out: Record<string, Record<string, unknown>> = {};
  for (const [sectionName, section] of Object.entries(value)) {
    if (section === undefined) continue;
    const spec = Object.hasOwn(CAPABILITY_SPEC, sectionName)
      ? CAPABILITY_SPEC[sectionName as keyof NamespaceCapabilities]
      : undefined;
    if (!spec) throw invalid("capabilities", `unknown section "${sectionName}"`);
    if (!isPlainObject(section)) {
      throw invalid("capabilities", `${sectionName} must be an object`);
    }
    const fields: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(section)) {
      if (item === undefined) continue;
      const kind = Object.hasOwn(spec, key) ? spec[key] : undefined;
      if (!kind) throw invalid("capabilities", `unknown field "${sectionName}.${key}"`);
      if (kind === "boolean" && typeof item === "boolean") {
        fields[key] = item;
      } else if (kind === "positiveInteger" && Number.isSafeInteger(item) && (item as number) > 0) {
        fields[key] = item;
      } else if (
        kind === "stringArray" &&
        Array.isArray(item) &&
        item.every((part) => typeof part === "string" && part.length > 0)
      ) {
        fields[key] = [...item];
      } else {
        throw invalid("capabilities", `${sectionName}.${key} must be a ${describeKind(kind)}`);
      }
    }
    out[sectionName] = fields;
  }
  return out as NamespaceCapabilities;
}

function describeKind(kind: CapabilityFieldKind): string {
  switch (kind) {
    case "boolean":
      return "boolean";
    case "positiveInteger":
      return "positive integer";
    case "stringArray":
      return "list of non-empty strings";
  }
}

function normalizeMetadata(value: unknown): Record<string, unknown> {
  if (!isPlainObject(value)) throw invalid("metadata", "must be an object");
  return jsonObject({ value, path: "metadata" });
}

function jsonObject(params: { value: object; path: string }): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(params.value)) {
    if (item === undefined) continue;
    out[key] = jsonValue({ value: item, path: `${params.path}.${key}` });
  }
  return out;
}

/** A JSON-safe deep copy; throws for values JSON cannot carry. */
function jsonValue(params: { value: unknown; path: string }): unknown {
  const { value, path } = params;
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) {
    return value.map((item, i) => jsonValue({ value: item ?? null, path: `${path}[${i}]` }));
  }
  if (isPlainObject(value)) return jsonObject({ value, path });
  throw invalid("metadata", `${path.slice("metadata.".length)} is not JSON-serializable`);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

// ---------------------------------------------------------------------------
// InMemoryNamespaceRegistry
// ---------------------------------------------------------------------------

export interface InMemoryNamespaceRegistryConfig {
  /** Time source for `createdAt` / `updatedAt`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** Process-local `NamespaceRegistry`. Records are copied in and out. */
export class InMemoryNamespaceRegistry implements NamespaceRegistry {
  private readonly rows = new Map<string, NamespaceRecord>();
  private readonly clock: WallClock;

  constructor(config: InMemoryNamespaceRegistryConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async create(input: CreateNamespaceInput): Promise<NamespaceRecord> {
    const fields = normalizeNamespaceCreate(input);
    if (this.rows.has(fields.id)) throw NamespaceExistsError.of(fields.id);
    const now = new Date(this.clock.currentTimeMs());
    const row: NamespaceRecord = { ...fields, status: "active", createdAt: now, updatedAt: now };
    this.rows.set(row.id, row);
    return structuredClone(row);
  }

  async get(id: string): Promise<NamespaceRecord | null> {
    const row = this.rows.get(id);
    return row ? structuredClone(row) : null;
  }

  async list(params: { readonly status?: NamespaceStatus } = {}): Promise<NamespaceRecord[]> {
    const status = params.status !== undefined ? parseNamespaceStatus(params.status) : undefined;
    return [...this.rows.values()]
      .filter((row) => status === undefined || row.status === status)
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
      .map((row) => structuredClone(row));
  }

  async update(params: {
    readonly id: string;
    readonly patch: NamespacePatch;
  }): Promise<NamespaceRecord> {
    const patch = normalizeNamespacePatch(params);
    const existing = this.rows.get(params.id);
    if (!existing) throw NamespaceNotFoundError.of(params.id);
    const next: NamespaceRecord = {
      ...existing,
      ...patch,
      updatedAt: new Date(this.clock.currentTimeMs()),
    };
    this.rows.set(next.id, next);
    return structuredClone(next);
  }

  async delete(id: string): Promise<boolean> {
    return this.rows.delete(id);
  }
}
