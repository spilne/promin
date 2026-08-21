import { createHash } from "node:crypto";
import {
  compileWorkflow,
  type IWorkflowVersionRegistry,
  type WorkflowSchema,
  type WorkflowStepCatalog,
  type WorkflowStepCatalogEntry,
} from "@promin/workflow";

export interface AuthoredWorkflowRecord {
  readonly name: string;
  readonly version: string;
  readonly schema: WorkflowSchema;
  readonly status: "draft" | "published";
  readonly contentHash: string;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly publishedAt?: number;
}

export interface AuthoredWorkflowSaveInput {
  readonly schema: WorkflowSchema;
  readonly version?: string;
}

export interface AuthoredWorkflowStore {
  list(): Promise<ReadonlyArray<AuthoredWorkflowRecord>>;
  get(name: string, version?: string): Promise<AuthoredWorkflowRecord | null>;
  save(record: AuthoredWorkflowRecord): Promise<void>;
  delete(name: string, version: string): Promise<void>;
}

export class InMemoryAuthoredWorkflowStore implements AuthoredWorkflowStore {
  private readonly rows = new Map<string, AuthoredWorkflowRecord>();

  constructor(records: ReadonlyArray<AuthoredWorkflowRecord> = []) {
    for (const record of records) this.rows.set(key(record.name, record.version), clone(record));
  }

  async list(): Promise<ReadonlyArray<AuthoredWorkflowRecord>> {
    return [...this.rows.values()]
      .map(clone)
      .sort((a, b) => a.name.localeCompare(b.name) || b.updatedAt - a.updatedAt);
  }

  async get(name: string, version?: string): Promise<AuthoredWorkflowRecord | null> {
    if (version !== undefined) return cloneOrNull(this.rows.get(key(name, version)));
    let latest: AuthoredWorkflowRecord | null = null;
    for (const row of this.rows.values()) {
      if (row.name !== name) continue;
      if (!latest || row.updatedAt > latest.updatedAt) latest = row;
    }
    return cloneOrNull(latest);
  }

  async save(record: AuthoredWorkflowRecord): Promise<void> {
    this.rows.set(key(record.name, record.version), clone(record));
  }

  async delete(name: string, version: string): Promise<void> {
    this.rows.delete(key(name, version));
  }
}

export interface ZoryaWorkflowBuilderConfig {
  readonly catalog: WorkflowStepCatalog;
  readonly versionRegistry: IWorkflowVersionRegistry;
  readonly store?: AuthoredWorkflowStore;
  readonly now?: () => number;
}

export class ZoryaWorkflowBuilder {
  readonly catalog: WorkflowStepCatalog;
  readonly store: AuthoredWorkflowStore;
  readonly versionRegistry: IWorkflowVersionRegistry;
  private readonly now: () => number;

  constructor(config: ZoryaWorkflowBuilderConfig) {
    this.catalog = config.catalog;
    this.versionRegistry = config.versionRegistry;
    this.store = config.store ?? new InMemoryAuthoredWorkflowStore();
    this.now = config.now ?? (() => Date.now());
  }

  steps(): WorkflowStepCatalogEntry[] {
    return this.catalog.entries();
  }

  async list(): Promise<ReadonlyArray<AuthoredWorkflowRecord>> {
    return this.store.list();
  }

  async get(name: string, version?: string): Promise<AuthoredWorkflowRecord | null> {
    return this.store.get(name, version);
  }

  async save(input: AuthoredWorkflowSaveInput): Promise<AuthoredWorkflowRecord> {
    const version = input.version ?? "v1";
    const existing = await this.store.get(input.schema.name, version);
    const now = this.now();
    const contentHash = hashSchema(input.schema);
    const status =
      existing?.status === "published" && existing.contentHash === contentHash
        ? "published"
        : "draft";
    const record: AuthoredWorkflowRecord = {
      name: input.schema.name,
      version,
      schema: structuredClone(input.schema),
      status,
      contentHash,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
      ...(status === "published" && existing?.publishedAt !== undefined
        ? { publishedAt: existing.publishedAt }
        : {}),
    };
    await this.store.save(record);
    return clone(record);
  }

  async delete(name: string, version: string): Promise<void> {
    const existing = await this.store.get(name, version);
    await this.store.delete(name, version);
    if (existing) await this.versionRegistry.deregister(name, version);
  }

  async publish(
    name: string,
    version?: string,
    opts: { readonly promote?: boolean } = {},
  ): Promise<AuthoredWorkflowRecord> {
    const existing = await this.store.get(name, version);
    if (!existing) throw new Error("authored_workflow_not_found");
    const workflow = compileWorkflow({
      schema: existing.schema,
      registry: this.catalog,
      version: existing.version,
      type: "authored",
    });
    await this.versionRegistry.register(workflow);
    if (opts.promote !== false) await this.versionRegistry.promote?.(name, existing.version);
    const now = this.now();
    const record: AuthoredWorkflowRecord = {
      ...existing,
      status: "published",
      updatedAt: now,
      publishedAt: now,
    };
    await this.store.save(record);
    return clone(record);
  }
}

function hashSchema(schema: WorkflowSchema): string {
  return createHash("sha256").update(JSON.stringify(schema)).digest("hex");
}

function key(name: string, version: string): string {
  return `${name}\u0000${version}`;
}

function clone(record: AuthoredWorkflowRecord): AuthoredWorkflowRecord {
  return structuredClone(record);
}

function cloneOrNull(
  record: AuthoredWorkflowRecord | undefined | null,
): AuthoredWorkflowRecord | null {
  return record ? clone(record) : null;
}
