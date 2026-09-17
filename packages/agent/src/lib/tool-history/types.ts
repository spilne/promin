// ---------------------------------------------------------------------------
// Tool catalog history — durable audit trail of which tools the host has
// exposed over time.
//
// The live `AgentToolCatalog` is intentionally ephemeral: every
// `listAll()` walks live sources. This store is the opt-in persistence
// layer over it — `AgentToolCatalogHistory` snapshots the catalog
// periodically and upserts here, so a compliance / forensic query can
// answer "what tools could an agent invoke between date X and Y" and
// "when did tool foo change its schema / disappear".
//
// Each row is one (name, source, schema-version) tuple. A tool
// re-observed with the same schema bumps `lastSeenAt`; a schema change
// lands as a new row. Rows are never deleted by the store — a tool that
// vanishes simply stops having its `lastSeenAt` advanced.
// ---------------------------------------------------------------------------

/** Where a catalogued tool's implementation came from. */
export type ToolHistorySourceKind = "in-process" | "file" | "mcp";

/**
 * One observation of a tool, as fed to `ToolHistoryStore.recordSnapshot`.
 * The `(name, sourceKind, sourceDetail, schemaHash)` tuple is the
 * identity — two observations with the same tuple are the same row.
 */
export interface ToolObservation {
  /** Tool name as the catalog reports it (MCP names are server-prefixed). */
  readonly name: string;
  /** Which source kind exposed the tool. */
  readonly sourceKind: ToolHistorySourceKind;
  /**
   * Disambiguates the source within its kind: `""` for in-process, the
   * file path for `file`, the server name for `mcp`.
   */
  readonly sourceDetail: string;
  /** SHA-256 of the canonical JSON Schema of the tool's parameters. */
  readonly schemaHash: string;
  /** The tool's description at observation time. */
  readonly description: string;
}

/** A persisted observation — `ToolObservation` plus its seen-window. */
export interface ToolHistoryRecord extends ToolObservation {
  /** Epoch ms the tuple was first recorded. */
  readonly firstSeenAt: number;
  /** Epoch ms the tuple was most recently observed. */
  readonly lastSeenAt: number;
}

/** Filters for the history read path. All fields are optional. */
export interface ToolHistoryQuery {
  /** Restrict to one tool name. */
  readonly name?: string;
  /** Restrict to one source kind. */
  readonly sourceKind?: ToolHistorySourceKind;
  /** Only rows last observed at or after this epoch-ms timestamp. */
  readonly since?: number;
  /** Cap on rows returned. */
  readonly limit?: number;
}

/**
 * Persistence for tool-catalog observations. `recordSnapshot` is an
 * upsert keyed by the identity tuple; `list` is the read path. The
 * store owns the timestamps — observations carry no time of their own.
 */
export interface ToolHistoryStore {
  /**
   * Upsert a batch of observations. A tuple not seen before is inserted
   * with `firstSeenAt == lastSeenAt == now`; a tuple already present has
   * its `lastSeenAt` advanced to now (and `description` refreshed).
   */
  recordSnapshot(observations: ReadonlyArray<ToolObservation>): Promise<void>;
  /** Read recorded rows, most recently seen first. */
  list(query?: ToolHistoryQuery): Promise<ToolHistoryRecord[]>;
}
