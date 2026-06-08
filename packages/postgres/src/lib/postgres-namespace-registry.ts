// ---------------------------------------------------------------------------
// PostgresNamespaceRegistry — durable NamespaceRegistry over Postgres.
// ---------------------------------------------------------------------------

import { asc, eq } from "drizzle-orm";
import {
  NamespaceNotFoundError,
  normalizeNamespaceDisplayName,
  normalizeNamespaceId,
  normalizeNamespaceStatus,
  sanitizeNamespaceCapabilities,
  sanitizeNamespaceRecord,
  type Namespace,
  type NamespaceCapabilities,
  type NamespaceCreateInput,
  type NamespaceRegistry,
  type NamespaceUpdateInput,
} from "@promin/core";
import type { DrizzleDb } from "./drizzle-db.ts";
import { zoryaNamespace } from "./schema.ts";
import { ensureTable } from "./schema-utils.ts";

export interface PostgresNamespaceRegistryConfig {
  readonly db: DrizzleDb;
  readonly now?: () => number;
}

export class PostgresNamespaceRegistry implements NamespaceRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresNamespaceRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async ensureTable(): Promise<void> {
    await ensureTable(this.db, zoryaNamespace);
  }

  async create(input: NamespaceCreateInput): Promise<Namespace> {
    const id = normalizeNamespaceId(input.id);
    const now = this.clock();
    const row: Namespace = {
      id,
      displayName: normalizeNamespaceDisplayName(input.displayName, id),
      description: input.description ?? null,
      status: "active",
      capabilities: sanitizeNamespaceCapabilities(input.capabilities),
      metadata: sanitizeNamespaceRecord(input.metadata),
      createdAt: now,
      updatedAt: now,
    };
    try {
      await this.db.insert(zoryaNamespace).values({
        id: row.id,
        displayName: row.displayName,
        description: row.description,
        status: row.status,
        capabilities: row.capabilities as Record<string, unknown>,
        metadata: row.metadata,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
      });
    } catch (err) {
      if (String(err).includes("duplicate key") || String(err).includes("unique")) {
        throw new Error(`namespace already exists: ${id}`);
      }
      throw err;
    }
    return row;
  }

  async get(id: string): Promise<Namespace | null> {
    const rows = await this.db
      .select()
      .from(zoryaNamespace)
      .where(eq(zoryaNamespace.id, normalizeNamespaceId(id)))
      .limit(1);
    const row = rows[0];
    return row ? toNamespace(row) : null;
  }

  async list(params: { status?: Namespace["status"] } = {}): Promise<Namespace[]> {
    const status = params.status ? normalizeNamespaceStatus(params.status) : undefined;
    const rows = status
      ? await this.db
          .select()
          .from(zoryaNamespace)
          .where(eq(zoryaNamespace.status, status))
          .orderBy(asc(zoryaNamespace.displayName), asc(zoryaNamespace.id))
      : await this.db
          .select()
          .from(zoryaNamespace)
          .orderBy(asc(zoryaNamespace.displayName), asc(zoryaNamespace.id));
    return rows.map(toNamespace);
  }

  async update(id: string, patch: NamespaceUpdateInput): Promise<Namespace> {
    const key = normalizeNamespaceId(id);
    const existing = await this.get(key);
    if (!existing) throw new NamespaceNotFoundError(key);
    const next: Namespace = {
      ...existing,
      ...(patch.displayName !== undefined && {
        displayName: normalizeNamespaceDisplayName(patch.displayName, key),
      }),
      ...(patch.description !== undefined && { description: patch.description }),
      ...(patch.status !== undefined && { status: normalizeNamespaceStatus(patch.status) }),
      ...(patch.capabilities !== undefined && {
        capabilities: sanitizeNamespaceCapabilities(patch.capabilities),
      }),
      ...(patch.metadata !== undefined && { metadata: sanitizeNamespaceRecord(patch.metadata) }),
      updatedAt: this.clock(),
    };
    await this.db
      .update(zoryaNamespace)
      .set({
        displayName: next.displayName,
        description: next.description,
        status: next.status,
        capabilities: next.capabilities as Record<string, unknown>,
        metadata: next.metadata,
        updatedAt: next.updatedAt,
      })
      .where(eq(zoryaNamespace.id, key));
    return next;
  }

  async archive(id: string): Promise<Namespace> {
    return this.update(id, { status: "archived" });
  }
}

function toNamespace(row: {
  id: string;
  displayName: string;
  description: string | null;
  status: string;
  capabilities: unknown;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}): Namespace {
  return {
    id: row.id,
    displayName: row.displayName,
    description: row.description,
    status: normalizeNamespaceStatus(row.status),
    capabilities: coerceObject<NamespaceCapabilities>(row.capabilities),
    metadata: coerceObject<Record<string, unknown>>(row.metadata),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function coerceObject<T extends object>(value: unknown): T {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as T) : ({} as T);
}
