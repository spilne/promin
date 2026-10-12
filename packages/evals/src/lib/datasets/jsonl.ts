// ---------------------------------------------------------------------------
// jsonlDataset — one JSON-encoded EvalCase per line, read from disk.
//
// A line may omit `id` (synthesised as `<datasetId>-<lineIndex>`); `input`
// is required. Blank lines are skipped.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalDataset } from "../types.ts";

interface RawCase {
  readonly id?: string;
  readonly input: unknown;
  readonly expected?: unknown;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Read a `.jsonl` file as an `EvalDataset`. */
export function jsonlDataset(params: {
  readonly path: string;
  /** Dataset id. Default: the path. */
  readonly id?: string;
}): EvalDataset {
  const { path } = params;
  const datasetId = params.id ?? path;
  return {
    id: datasetId,
    async *cases() {
      const text = await Bun.file(path).text();
      let lineIndex = 0;
      for (const line of text.split("\n")) {
        const trimmed = line.trim();
        if (trimmed === "") continue;
        const raw = JSON.parse(trimmed) as RawCase;
        const evalCase: EvalCase = {
          id: raw.id ?? `${datasetId}-${lineIndex}`,
          input: raw.input,
          ...(raw.expected !== undefined && { expected: raw.expected }),
          ...(raw.tags !== undefined && { tags: raw.tags }),
          ...(raw.metadata !== undefined && { metadata: raw.metadata }),
        };
        yield evalCase;
        lineIndex += 1;
      }
    },
  };
}
