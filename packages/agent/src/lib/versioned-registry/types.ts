// ---------------------------------------------------------------------------
// `VersionedRegistry` — the one store behind agents, roles, skills, DAGs and
// prompt fragments.
//
// Every entry is an id with a history of immutable versions and an explicit
// active pointer:
//
//   - A version's body never changes, and a label is never bound to another
//     body — not even after the version was removed (a removed version stays
//     as a tombstone). Registering an existing (id, version) with the same
//     body is a no-op (a removed one is restored); with a different body it
//     is a conflict. Registering without a version reuses the version that
//     holds the same body (restoring it if removed), or mints the next
//     `v<n>` — the sequence never restarts, so a pin like `id@v1` always
//     names the same text.
//   - The first version of an id becomes active. Later versions stay drafts
//     until promoted (`promote`, or `register({ promote: true })`). Removing
//     the active version activates `unregister({ promote })`, else the
//     version that was active before it, else the latest remaining one.
//   - Writes take `expectedActive` for compare-and-set: the write applies only
//     while the id's active version is still the one the caller read (`null`
//     = the id has no versions yet).
//   - Per id, `enabled` is the operator kill switch; per version, `trust`
//     records whether an operator has reviewed that body.
//   - `list` answers one entry per id (its active version) in id order, with
//     the kind's filter applied before the page is cut.
//
// A `RegistryKind` adapts the store to one kind of body: it normalises and
// validates input and derives the facets `list` filters on. Implementations:
// `InMemoryVersionedRegistry` here, `SqliteVersionedRegistry` in
// `@promin/sqlite`, `PostgresVersionedRegistry` in `@promin/postgres`. All
// pass `versionedRegistryTestSuite` for every kind.
// ---------------------------------------------------------------------------

/** Review state of one version. `needs-review` versions are never used by agents. */
export type VersionTrust = "trusted" | "needs-review";

/** Store-assigned fields every entry carries next to its body. */
export interface VersionInfo {
  readonly id: string;
  /** Immutable version label (`v1`, `v2`, … unless the caller named it). */
  readonly version: string;
  /** When this version was registered (epoch ms, the registry's clock). */
  readonly createdAt: number;
  /** Whether this version is the id's active one. */
  readonly active: boolean;
  /** The id's kill switch: `false` keeps the entry but refuses new use. */
  readonly enabled: boolean;
  /** Review state of this version's body. */
  readonly trust: VersionTrust;
  /**
   * Who wrote this version: `"file"` for scanner-discovered entries,
   * absent for operator-authored ones. Scanner sync only ever removes
   * `"file"` versions.
   */
  readonly source?: string;
}

/** A stored version: the kind's body plus the store-assigned fields. */
export type Versioned<TBody> = TBody & VersionInfo;

/** Fields `register` takes besides the kind's input body. */
export interface RegisterOptions {
  readonly id: string;
  /**
   * Version label. Omitted: reuse the version that already holds this exact
   * body, else mint the next `v<n>`.
   */
  readonly version?: string;
  /** Make this version the active one. The first version of an id always is. */
  readonly promote?: boolean;
  /** Compare-and-set: the id's active version must still be this (`null` = no versions). */
  readonly expectedActive?: string | null;
  /** Provenance of a newly created version (`"file"` for scanners). */
  readonly source?: string;
  /** Review state of a newly created version. Default `"trusted"`. */
  readonly trust?: VersionTrust;
  /**
   * Set the id's kill switch with this write. Omitted: unchanged (an id
   * with no live versions starts enabled).
   */
  readonly enabled?: boolean;
}

/** `register` input: the kind's input body plus `RegisterOptions`. */
export type RegisterVersionInput<TInput> = TInput & RegisterOptions;

export interface ListVersionedParams<TFilter> {
  readonly filter?: TFilter;
  /** Only ids whose kill switch is in this state. */
  readonly enabled?: boolean;
  /** Only ids whose active version has this `source` (`"file"`: file-managed). */
  readonly source?: string;
  /** Page size, a positive integer. Omitted: every matching id. */
  readonly limit?: number;
  /** `nextCursor` of the previous page. */
  readonly cursor?: string;
}

export interface VersionedPage<T> {
  readonly items: T[];
  /** Pass as `cursor` for the next page; `null` on the last page. */
  readonly nextCursor: string | null;
}

/** A store of immutable, versioned bodies with an active pointer per id. */
export interface VersionedRegistry<TBody, TFilter, TInput = TBody> {
  /** The kind this registry stores. */
  readonly kind: RegistryKind<TBody, TFilter, TInput>;

  /**
   * Register a version. Throws `RegistryConflictError` when `version` exists
   * with another body (`version_conflict`) or `expectedActive` no longer
   * holds (`active_mismatch`); nothing is written then.
   */
  register(input: RegisterVersionInput<TInput>): Promise<Versioned<TBody>>;

  /** `version` given: that version. Omitted: the active one. `null` when absent. */
  get(params: { readonly id: string; readonly version?: string }): Promise<Versioned<TBody> | null>;

  /** Active version per id, ordered by id, filtered before paging. */
  list(params?: ListVersionedParams<TFilter>): Promise<VersionedPage<Versioned<TBody>>>;

  /** Every live version of `id`, oldest first. Empty when the id has none. */
  versions(params: { readonly id: string }): Promise<Versioned<TBody>[]>;

  /**
   * Make `version` the active one. Throws `RegistryNotFoundError` for an
   * unknown version, `RegistryConflictError` when `expectedActive` fails.
   */
  promote(params: {
    readonly id: string;
    readonly version: string;
    readonly expectedActive?: string | null;
  }): Promise<Versioned<TBody>>;

  /** Flip the id's kill switch. Returns the active version; throws when the id is unknown. */
  setEnabled(params: {
    readonly id: string;
    readonly enabled: boolean;
    readonly expectedActive?: string | null;
  }): Promise<Versioned<TBody>>;

  /** Record an operator's review of one version. Throws when the version is unknown. */
  setTrust(params: {
    readonly id: string;
    readonly version: string;
    readonly trust: VersionTrust;
  }): Promise<Versioned<TBody>>;

  /**
   * Remove one version, or every version of the id. A removed version
   * keeps its label (a tombstone): it can only come back with the same
   * body. Removing the active version activates `promote` when given, else
   * the version that was active before it, else the latest remaining one.
   * Returns whether anything was removed; throws `RegistryConflictError`
   * when `expectedActive` fails, `RegistryNotFoundError` for an unknown
   * `promote` version.
   */
  unregister(params: {
    readonly id: string;
    readonly version?: string;
    readonly expectedActive?: string | null;
    /** With `version` = the active one: the version to activate instead. */
    readonly promote?: string;
  }): Promise<boolean>;
}

/** Facets a filter requires and excludes. */
export interface FacetQuery {
  readonly include: ReadonlyArray<string>;
  readonly exclude: ReadonlyArray<string>;
}

/** Adapts the generic store to one kind of body. */
export interface RegistryKind<TBody, TFilter, TInput = TBody> {
  /** Kind name; the SQL stores key rows by it. */
  readonly name: string;
  /** Normalise (defaults) and validate an input body. Throw to reject it. */
  prepare(params: { readonly id: string; readonly input: TInput }): TBody;
  /** Strings `list` filters on (`tag:beta`, `cap:chat`, …). */
  facets(params: { readonly id: string; readonly body: TBody }): ReadonlyArray<string>;
  /** Facets an entry must carry / must not carry to match `filter`. */
  query(filter: TFilter): FacetQuery;
}

/** Version labels: URL- and header-safe. */
export const VERSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

/** A write lost a compare-and-set or would rewrite an immutable version. */
export class RegistryConflictError extends Error {
  readonly _tag = "RegistryConflictError";
  readonly code: "version_conflict" | "active_mismatch";
  readonly kind: string;
  readonly id: string;
  /** The version in conflict, or the active version the caller expected. */
  readonly version: string | null;
  /** For `active_mismatch`: the id's actual active version. */
  readonly actual: string | null;

  constructor(params: {
    readonly code: "version_conflict" | "active_mismatch";
    readonly kind: string;
    readonly id: string;
    readonly version: string | null;
    readonly actual?: string | null;
  }) {
    super(
      params.code === "version_conflict"
        ? `${params.kind} "${params.id}@${params.version}" already exists with different content; register a new version`
        : `${params.kind} "${params.id}": expected active version ${String(params.version)}, found ${String(params.actual ?? null)}`,
    );
    this.name = "RegistryConflictError";
    this.code = params.code;
    this.kind = params.kind;
    this.id = params.id;
    this.version = params.version;
    this.actual = params.actual ?? null;
  }
}

/** The id or version a write names does not exist. */
export class RegistryNotFoundError extends Error {
  readonly _tag = "RegistryNotFoundError";
  constructor(params: { readonly kind: string; readonly id: string; readonly version?: string }) {
    super(
      `${params.kind} "${params.id}${params.version !== undefined ? `@${params.version}` : ""}" is not registered`,
    );
    this.name = "RegistryNotFoundError";
  }
}

/** A register input is malformed (bad version label, invalid body). */
export class RegistryInputError extends Error {
  readonly _tag = "RegistryInputError";
  constructor(message: string) {
    super(message);
    this.name = "RegistryInputError";
  }
}

/** Whether `err` is a `RegistryInputError`, also across module copies. */
export function isRegistryInputError(err: unknown): err is RegistryInputError {
  return (err as { _tag?: unknown } | null)?._tag === "RegistryInputError";
}

/** Whether `err` is a `RegistryConflictError`, also across module copies. */
export function isRegistryConflict(err: unknown): err is RegistryConflictError {
  return (err as { _tag?: unknown } | null)?._tag === "RegistryConflictError";
}

/** Whether `err` is a `RegistryNotFoundError`, also across module copies. */
export function isRegistryNotFound(err: unknown): err is RegistryNotFoundError {
  return (err as { _tag?: unknown } | null)?._tag === "RegistryNotFoundError";
}

/** Read every page of `registry.list`. */
export async function listAllVersioned<TBody, TFilter, TInput>(params: {
  readonly registry: VersionedRegistry<TBody, TFilter, TInput>;
  readonly filter?: TFilter;
  readonly enabled?: boolean;
  readonly source?: string;
}): Promise<Versioned<TBody>[]> {
  const out: Versioned<TBody>[] = [];
  let cursor: string | undefined;
  for (;;) {
    const page = await params.registry.list({
      ...(params.filter !== undefined && { filter: params.filter }),
      ...(params.enabled !== undefined && { enabled: params.enabled }),
      ...(params.source !== undefined && { source: params.source }),
      limit: 500,
      ...(cursor !== undefined && { cursor }),
    });
    out.push(...page.items);
    if (page.nextCursor === null) return out;
    cursor = page.nextCursor;
  }
}
