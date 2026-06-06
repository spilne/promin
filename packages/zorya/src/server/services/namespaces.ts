// ---------------------------------------------------------------------------
// NamespaceService — Zorya-owned namespace registry and policy surface.
//
// The workflow engine treats namespace as an opaque partition key. Zorya owns
// the entity lifecycle, default namespace setup, and future runtime policy
// checks before a request reaches workflows or agents.
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

export interface NamespaceServiceConfig {
  readonly registry?: NamespaceRegistry;
  readonly defaultNamespaceId?: string;
  readonly now?: () => number;
}

export class NamespaceService {
  readonly registry: NamespaceRegistry;
  readonly defaultNamespaceId: string;

  constructor(config: NamespaceServiceConfig = {}) {
    this.defaultNamespaceId = config.defaultNamespaceId ?? DEFAULT_NAMESPACE_ID;
    this.registry = config.registry ?? new InMemoryNamespaceRegistry({ now: config.now });
  }

  async ensureDefaultNamespace(): Promise<Namespace> {
    return this.ensure({
      id: this.defaultNamespaceId,
      displayName: "Default",
      description: "Default Zorya namespace",
    });
  }

  async ensure(input: NamespaceCreateInput): Promise<Namespace> {
    const id = normalizeNamespaceId(input.id);
    const existing = await this.registry.get(id);
    if (existing) return existing;
    return this.registry.create({ ...input, id });
  }

  async resolve(input?: string | null): Promise<Namespace> {
    if (!input || input.trim().length === 0) {
      return this.ensureDefaultNamespace();
    }
    const id = normalizeNamespaceId(input);
    const namespace = await this.registry.get(id);
    if (!namespace) {
      throw new NamespaceNotFoundError(id);
    }
    if (namespace.status !== "active") {
      throw new NamespaceArchivedError(id);
    }
    return namespace;
  }

  async listActive(): Promise<Namespace[]> {
    await this.ensureDefaultNamespace();
    return this.registry.list({ status: "active" });
  }
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
    const filtered = params.status ? rows.filter((r) => r.status === params.status) : rows;
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
      ...(patch.status !== undefined && { status: patch.status }),
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

function normalizeDisplayName(value: string | undefined, fallback: string): string {
  const label = value?.trim();
  return label && label.length > 0 ? label.slice(0, 128) : fallback;
}

function sanitizeRecord(value: Record<string, unknown> | undefined): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
}

function sanitizeCapabilities(value: NamespaceCapabilities | undefined): NamespaceCapabilities {
  return value && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
}
