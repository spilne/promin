// ---------------------------------------------------------------------------
// `InMemoryVersionedRegistry` — the reference `VersionedRegistry`. One head
// per id (active pointer, activation history, kill switch, next sequence
// number) over a map of immutable versions. A removed version stays as a
// tombstone and a head is never dropped, so labels and sequence numbers are
// never reused. Every method runs without an `await` between its read and
// its write, so concurrent calls in one process never interleave.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  assertListLimit,
  autoVersionLabel,
  facetQuery,
  matchesFacets,
  nextActiveVersion,
  prepareRegister,
  pushHistory,
} from "./store-helpers.ts";
import {
  RegistryConflictError,
  RegistryNotFoundError,
  type ListVersionedParams,
  type RegisterVersionInput,
  type RegistryKind,
  type Versioned,
  type VersionedPage,
  type VersionedRegistry,
  type VersionTrust,
} from "./types.ts";

export interface InMemoryVersionedRegistryConfig<TBody, TFilter, TInput> {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  /** Time source for `createdAt`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface Row<TBody> {
  readonly version: string;
  readonly seq: number;
  readonly body: TBody;
  readonly hash: string;
  readonly facets: ReadonlyArray<string>;
  readonly createdAt: number;
  trust: VersionTrust;
  readonly source?: string;
  removed: boolean;
}

interface Head<TBody> {
  /** `null` while the id has no live version. */
  active: string | null;
  /** Earlier active versions, oldest first. */
  history: string[];
  enabled: boolean;
  nextSeq: number;
  readonly rows: Map<string, Row<TBody>>;
}

export class InMemoryVersionedRegistry<TBody, TFilter, TInput = TBody> implements VersionedRegistry<
  TBody,
  TFilter,
  TInput
> {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  private readonly clock: WallClock;
  private readonly heads = new Map<string, Head<TBody>>();

  constructor(config: InMemoryVersionedRegistryConfig<TBody, TFilter, TInput>) {
    this.kind = config.kind;
    this.clock = config.clock ?? SystemWallClock;
  }

  async register(input: RegisterVersionInput<TInput>): Promise<Versioned<TBody>> {
    const prepared = prepareRegister({ kind: this.kind, input });
    const { options } = prepared;
    const existing = this.heads.get(options.id);
    this.assertActive({ id: options.id, head: existing, expected: options.expectedActive });

    let row: Row<TBody> | undefined;
    if (options.version !== undefined) {
      row = existing?.rows.get(options.version);
      if (row && row.hash !== prepared.hash) {
        throw new RegistryConflictError({
          code: "version_conflict",
          kind: this.kind.name,
          id: options.id,
          version: options.version,
        });
      }
    } else if (existing) {
      // The same body: prefer a live version, then the latest one.
      for (const candidate of existing.rows.values()) {
        if (candidate.hash !== prepared.hash) continue;
        if (
          !row ||
          (row.removed && !candidate.removed) ||
          (row.removed === candidate.removed && candidate.seq > row.seq)
        ) {
          row = candidate;
        }
      }
    }

    const head: Head<TBody> = existing ?? {
      active: null,
      history: [],
      enabled: true,
      nextSeq: 1,
      rows: new Map(),
    };
    if (row) {
      row.removed = false; // a tombstone with the same body is restored
    } else {
      const seq = head.nextSeq;
      row = {
        version: options.version ?? autoVersionLabel({ seq, taken: (v) => head.rows.has(v) }),
        seq,
        body: structuredClone(prepared.body),
        hash: prepared.hash,
        facets: prepared.facets,
        createdAt: this.clock.currentTimeMs(),
        trust: options.trust ?? "trusted",
        ...(options.source !== undefined && { source: options.source }),
        removed: false,
      };
      head.rows.set(row.version, row);
      head.nextSeq = seq + 1;
    }
    if (head.active === null) {
      // The id's first live version: active, switch reset.
      head.active = row.version;
      head.history = [];
      head.enabled = true;
    } else if (options.promote && head.active !== row.version) {
      head.history = pushHistory({
        history: head.history,
        previous: head.active,
        next: row.version,
      });
      head.active = row.version;
    }
    if (options.enabled !== undefined) head.enabled = options.enabled;
    this.heads.set(options.id, head);
    return this.toEntry({ id: options.id, head, row });
  }

  async get(params: { id: string; version?: string }): Promise<Versioned<TBody> | null> {
    const head = this.heads.get(params.id);
    if (!head || head.active === null) return null;
    const row = head.rows.get(params.version ?? head.active);
    return row && !row.removed ? this.toEntry({ id: params.id, head, row }) : null;
  }

  async list(params: ListVersionedParams<TFilter> = {}): Promise<VersionedPage<Versioned<TBody>>> {
    assertListLimit(params.limit);
    const query = facetQuery({ kind: this.kind, filter: params.filter });
    const ids = [...this.heads.keys()].sort(compareIds);
    const items: Versioned<TBody>[] = [];
    let nextCursor: string | null = null;
    for (const id of ids) {
      if (params.cursor !== undefined && compareIds(id, params.cursor) <= 0) continue;
      const head = this.heads.get(id)!;
      if (head.active === null) continue;
      if (params.enabled !== undefined && head.enabled !== params.enabled) continue;
      const row = head.rows.get(head.active)!;
      if (params.source !== undefined && row.source !== params.source) continue;
      if (!matchesFacets({ facets: row.facets, query })) continue;
      if (params.limit !== undefined && items.length === params.limit) {
        nextCursor = items[items.length - 1]!.id;
        break;
      }
      items.push(this.toEntry({ id, head, row }));
    }
    return { items, nextCursor };
  }

  async versions(params: { id: string }): Promise<Versioned<TBody>[]> {
    const head = this.heads.get(params.id);
    if (!head) return [];
    return this.live(head).map((row) => this.toEntry({ id: params.id, head, row }));
  }

  async promote(params: {
    id: string;
    version: string;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    const head = this.heads.get(params.id);
    this.assertActive({ id: params.id, head, expected: params.expectedActive });
    const row = head?.rows.get(params.version);
    if (!head || !row || row.removed) {
      throw new RegistryNotFoundError({
        kind: this.kind.name,
        id: params.id,
        version: params.version,
      });
    }
    head.history = pushHistory({ history: head.history, previous: head.active, next: row.version });
    head.active = row.version;
    return this.toEntry({ id: params.id, head, row });
  }

  async setEnabled(params: {
    id: string;
    enabled: boolean;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    const head = this.heads.get(params.id);
    this.assertActive({ id: params.id, head, expected: params.expectedActive });
    if (!head || head.active === null) {
      throw new RegistryNotFoundError({ kind: this.kind.name, id: params.id });
    }
    head.enabled = params.enabled;
    return this.toEntry({ id: params.id, head, row: head.rows.get(head.active)! });
  }

  async setTrust(params: {
    id: string;
    version: string;
    trust: VersionTrust;
  }): Promise<Versioned<TBody>> {
    const head = this.heads.get(params.id);
    const row = head?.rows.get(params.version);
    if (!head || !row || row.removed) {
      throw new RegistryNotFoundError({
        kind: this.kind.name,
        id: params.id,
        version: params.version,
      });
    }
    row.trust = params.trust;
    return this.toEntry({ id: params.id, head, row });
  }

  async unregister(params: {
    id: string;
    version?: string;
    expectedActive?: string | null;
    promote?: string;
  }): Promise<boolean> {
    const head = this.heads.get(params.id);
    this.assertActive({ id: params.id, head, expected: params.expectedActive });
    if (!head || head.active === null) return false;
    if (params.version === undefined) {
      for (const row of head.rows.values()) row.removed = true;
      head.active = null;
      head.history = [];
      return true;
    }
    const row = head.rows.get(params.version);
    if (!row || row.removed) return false;
    const removingActive = head.active === params.version;
    if (removingActive && params.promote !== undefined) {
      const replacement = head.rows.get(params.promote);
      if (!replacement || replacement.removed || params.promote === params.version) {
        throw new RegistryNotFoundError({
          kind: this.kind.name,
          id: params.id,
          version: params.promote,
        });
      }
    }
    row.removed = true;
    const live = this.live(head).map((r) => r.version);
    if (removingActive) {
      head.active = nextActiveVersion({
        ...(params.promote !== undefined && { promote: params.promote }),
        history: head.history,
        live,
      });
    }
    head.history = head.history.filter((v) => live.includes(v) && v !== head.active);
    return true;
  }

  private live(head: Head<TBody>): Row<TBody>[] {
    return [...head.rows.values()].filter((r) => !r.removed).sort((a, b) => a.seq - b.seq);
  }

  private assertActive(params: {
    id: string;
    head: Head<TBody> | undefined;
    expected: string | null | undefined;
  }): void {
    if (params.expected === undefined) return;
    const actual = params.head?.active ?? null;
    if (actual !== params.expected) {
      throw new RegistryConflictError({
        code: "active_mismatch",
        kind: this.kind.name,
        id: params.id,
        version: params.expected,
        actual,
      });
    }
  }

  private toEntry(params: { id: string; head: Head<TBody>; row: Row<TBody> }): Versioned<TBody> {
    const { id, head, row } = params;
    return {
      ...structuredClone(row.body),
      id,
      version: row.version,
      createdAt: row.createdAt,
      active: head.active === row.version,
      enabled: head.enabled,
      trust: row.trust,
      ...(row.source !== undefined && { source: row.source }),
    };
  }
}

/** Code-unit order: the SQL stores' `COLLATE "C"` / BINARY order for every BMP id. */
function compareIds(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
