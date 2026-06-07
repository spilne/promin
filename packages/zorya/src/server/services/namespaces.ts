// ---------------------------------------------------------------------------
// NamespaceService — Zorya-owned namespace resolver and policy boundary.
//
// The workflow engine treats namespace as an opaque partition key. Zorya owns
// default namespace setup, lifecycle checks, and future runtime policy checks
// before a request reaches workflows or agents.
// ---------------------------------------------------------------------------

import {
  DEFAULT_NAMESPACE_ID,
  InMemoryNamespaceRegistry,
  NamespaceArchivedError,
  NamespaceNotFoundError,
  normalizeNamespaceId,
  type Namespace,
  type NamespaceCreateInput,
  type NamespaceRegistry,
} from "@promin/core";

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

export {
  DEFAULT_NAMESPACE_ID,
  InMemoryNamespaceRegistry,
  NamespaceArchivedError,
  NamespaceNotFoundError,
  normalizeNamespaceId,
  type Namespace,
  type NamespaceCapabilities,
  type NamespaceCreateInput,
  type NamespaceRegistry,
  type NamespaceUpdateInput,
} from "@promin/core";
