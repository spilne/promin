// ---------------------------------------------------------------------------
// Namespace registry — shared entity contract for platform-level namespaces.
//
// Namespaces are cross-cutting tenant/scope ids used by Zorya, workflow
// storage, agent memory, secrets, and signals. The runtime engines treat the
// id as opaque; products validate lifecycle/policy through this registry.
// ---------------------------------------------------------------------------

export const DEFAULT_NAMESPACE_ID = "default";

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

export interface Namespace {
  readonly id: string;
  readonly displayName: string;
  readonly description: string | null;
  readonly status: "active" | "archived";
  readonly capabilities: NamespaceCapabilities;
  readonly metadata: Record<string, unknown>;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface NamespaceCreateInput {
  readonly id: string;
  readonly displayName?: string;
  readonly description?: string | null;
  readonly capabilities?: NamespaceCapabilities;
  readonly metadata?: Record<string, unknown>;
}

export interface NamespaceUpdateInput {
  readonly displayName?: string;
  readonly description?: string | null;
  readonly status?: Namespace["status"];
  readonly capabilities?: NamespaceCapabilities;
  readonly metadata?: Record<string, unknown>;
}

export interface NamespaceRegistry {
  create(input: NamespaceCreateInput): Promise<Namespace>;
  get(id: string): Promise<Namespace | null>;
  list(params?: { status?: Namespace["status"] }): Promise<Namespace[]>;
  update(id: string, patch: NamespaceUpdateInput): Promise<Namespace>;
  archive(id: string): Promise<Namespace>;
}

export function normalizeNamespaceStatus(status: string): Namespace["status"] {
  if (status === "active" || status === "archived") return status;
  throw new Error("invalid_namespace_status");
}

export class InMemoryNamespaceRegistry implements NamespaceRegistry {
  private readonly rows = new Map<string, Namespace>();
  private readonly clock: () => number;

  constructor(config: { readonly now?: () => number } = {}) {
    this.clock = config.now ?? (() => Date.now());
  }

  async create(input: NamespaceCreateInput): Promise<Namespace> {
    const id = normalizeNamespaceId(input.id);
    if (this.rows.has(id)) throw new Error(`namespace already exists: ${id}`);
    const now = this.clock();
    const row: Namespace = {
      id,
      displayName: normalizeDisplayName(input.displayName, id),
      description: input.description ?? null,
      status: "active",
      capabilities: sanitizeCapabilities(input.capabilities),
      metadata: sanitizeRecord(input.metadata),
      createdAt: now,
      updatedAt: now,
    };
    this.rows.set(id, row);
    return row;
  }

  async get(id: string): Promise<Namespace | null> {
    return this.rows.get(normalizeNamespaceId(id)) ?? null;
  }

  async list(params: { status?: Namespace["status"] } = {}): Promise<Namespace[]> {
    const rows = [...this.rows.values()];
    const status = params.status ? normalizeNamespaceStatus(params.status) : undefined;
    const filtered = status ? rows.filter((r) => r.status === status) : rows;
    return filtered.sort(
      (a, b) => a.displayName.localeCompare(b.displayName) || a.id.localeCompare(b.id),
    );
  }

  async update(id: string, patch: NamespaceUpdateInput): Promise<Namespace> {
    const key = normalizeNamespaceId(id);
    const existing = this.rows.get(key);
    if (!existing) throw new NamespaceNotFoundError(key);
    const next: Namespace = {
      ...existing,
      ...(patch.displayName !== undefined && {
        displayName: normalizeDisplayName(patch.displayName, key),
      }),
      ...(patch.description !== undefined && { description: patch.description }),
      ...(patch.status !== undefined && { status: normalizeNamespaceStatus(patch.status) }),
      ...(patch.capabilities !== undefined && {
        capabilities: sanitizeCapabilities(patch.capabilities),
      }),
      ...(patch.metadata !== undefined && { metadata: sanitizeRecord(patch.metadata) }),
      updatedAt: this.clock(),
    };
    this.rows.set(key, next);
    return next;
  }

  async archive(id: string): Promise<Namespace> {
    return this.update(id, { status: "archived" });
  }
}

export class NamespaceNotFoundError extends Error {
  constructor(readonly namespaceId: string) {
    super(`Unknown namespace: ${namespaceId}`);
  }
}

export class NamespaceArchivedError extends Error {
  constructor(readonly namespaceId: string) {
    super(`Archived namespace: ${namespaceId}`);
  }
}

export function normalizeNamespaceId(id: string): string {
  const value = id.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value)) {
    throw new Error("invalid_namespace_id");
  }
  return value;
}

export function normalizeNamespaceDisplayName(value: string | undefined, fallback: string): string {
  return normalizeDisplayName(value, fallback);
}

export function sanitizeNamespaceRecord(
  value: Record<string, unknown> | undefined,
): Record<string, unknown> {
  return sanitizeRecord(value);
}

export function sanitizeNamespaceCapabilities(
  value: NamespaceCapabilities | undefined,
): NamespaceCapabilities {
  return sanitizeCapabilities(value);
}

function normalizeDisplayName(value: string | undefined, fallback: string): string {
  const label = value?.trim();
  return label && label.length > 0 ? label.slice(0, 128) : fallback;
}

function sanitizeRecord(value: Record<string, unknown> | undefined): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    const next = sanitizeJsonValue(item, `metadata.${key}`);
    if (next !== undefined) out[key] = next;
  }
  return out;
}

function sanitizeCapabilities(value: NamespaceCapabilities | undefined): NamespaceCapabilities {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const input = value as Record<string, unknown>;
  const out: NamespaceCapabilities = {
    ...(input.ai !== undefined && {
      ai: sanitizeCapabilitySection(input.ai, {
        enabled: "boolean",
        providers: "stringArray",
        models: "stringArray",
        maxTokensPerTurn: "positiveInteger",
        maxTurnsPerRun: "positiveInteger",
      }),
    }),
    ...(input.agents !== undefined && {
      agents: sanitizeCapabilitySection(input.agents, {
        enabled: "boolean",
        maxConcurrentThreads: "positiveInteger",
        maxInvocationsPerMinute: "positiveInteger",
      }),
    }),
    ...(input.workflows !== undefined && {
      workflows: sanitizeCapabilitySection(input.workflows, {
        enabled: "boolean",
        maxConcurrentRuns: "positiveInteger",
        maxRunMs: "positiveInteger",
      }),
    }),
    ...(input.tools !== undefined && {
      tools: sanitizeCapabilitySection(input.tools, {
        network: "boolean",
        filesystem: "boolean",
        shell: "boolean",
      }),
    }),
  };
  return out;
}

type CapabilityFieldKind = "boolean" | "positiveInteger" | "stringArray";

function sanitizeCapabilitySection<T extends Record<string, unknown>>(
  value: unknown,
  spec: Record<string, CapabilityFieldKind>,
): T {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid_namespace_capabilities");
  }
  const input = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const [key, kind] of Object.entries(spec)) {
    const item = input[key];
    if (item === undefined) continue;
    if (kind === "boolean" && typeof item === "boolean") {
      out[key] = item;
      continue;
    }
    if (kind === "positiveInteger" && Number.isInteger(item) && (item as number) > 0) {
      out[key] = item;
      continue;
    }
    if (
      kind === "stringArray" &&
      Array.isArray(item) &&
      item.every((part) => typeof part === "string" && part.length > 0)
    ) {
      out[key] = [...item];
      continue;
    }
    throw new Error("invalid_namespace_capabilities");
  }
  return out as T;
}

function sanitizeJsonValue(value: unknown, path: string): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => sanitizeJsonValue(item, `${path}[${index}]`));
  }
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      const next = sanitizeJsonValue(item, `${path}.${key}`);
      if (next !== undefined) out[key] = next;
    }
    return out;
  }
  if (value === undefined) return undefined;
  throw new Error("invalid_namespace_metadata");
}
