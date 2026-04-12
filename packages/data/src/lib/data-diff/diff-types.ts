// ---------------------------------------------------------------------------
// Data diff types
// ---------------------------------------------------------------------------

export interface DataDiffResult {
  readonly summary: {
    added: number;
    removed: number;
    modified: number;
    unchanged: number;
    total: number;
  };
  readonly addedRows: Record<string, unknown>[];
  readonly removedRows: Record<string, unknown>[];
  readonly modifications: Modification[];
}

export interface Modification {
  readonly key: unknown;
  readonly column: string;
  readonly before: unknown;
  readonly after: unknown;
}

export interface DiffOptions {
  /** Column to match rows on. */
  key: string;
  /** Which columns to compare. Default: all. */
  columns?: string[];
  /** Numeric tolerance for floating point comparison. Default: 0. */
  tolerance?: number;
  /** Max modifications to return. Default: 100. */
  sampleModifications?: number;
}

export interface SchemaDiffResult {
  readonly addedColumns: string[];
  readonly removedColumns: string[];
  readonly compatible: boolean;
}
