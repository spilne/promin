// ---------------------------------------------------------------------------
// Journal format — the branch-path grammar used to key journal entries that
// are recorded inside `ctx.parallel` branches, and the detection rules that
// let a step replay a journal written under an older grammar.
//
// Every journal entry is keyed by `(activityIndex, branchPath)`. Top-level
// yields always use `branchPath = ""` and draw `activityIndex` from the
// step's sequential counter; that part is identical across every format. The
// formats differ only in how a yield INSIDE a parallel branch is addressed.
//
// Format 1 (legacy, read-only):
//   A branch's first yield takes the branch prefix verbatim and later yields
//   append `.n`; a parallel's branches append `.i` to the parallel's own
//   path. The two suffixes share one namespace, so a nested parallel and a
//   sibling sequential yield can collide (`"0.1"` used twice). `ctx.sleep`,
//   `ctx.signal` and `ctx.child` inside a branch take a top-level index with
//   `branchPath = ""`, so their slot depends on sibling timing. Kept only so
//   journals written before format 2 still replay the way they were written.
//
// Format 2 (current, written by every new journal):
//   path    := segment+
//   segment := "/" branch "." seq
//   `branch` is the branch position in its `ctx.parallel([...])` call and
//   `seq` the 0-based yield counter inside that branch. A yield's path lists
//   one segment per enclosing parallel, outermost first:
//     ctx.parallel([A, function* () { yield* B; yield* ctx.parallel([C, D]) }])
//       A → "/0.0"   B → "/1.0"   C → "/1.1/0.0"   D → "/1.1/1.0"
//   Every yield inside a branch (activity, nested parallel, sleep, signal,
//   child) consumes the branch's `seq` counter, so a slot depends only on
//   the branch's own control flow, never on how fast siblings complete.
//   The leading "/" never appears in a format 1 path, which is how the two
//   are told apart. The grammar uses only digits, "." and "/" so every
//   backend stores it in the existing `branch_path` text column unchanged.
//
// Detection (per workflow + journaled step, from the loaded journal):
//   - any path starting with "/"          → format 2
//   - any other non-empty path            → format 1
//   - empty journal                       → format 2
//   - only `""` paths (non-empty journal) → undecided until the first
//     top-level `ctx.parallel`. Before that point both formats allocate the
//     same slots. At that parallel's index `p`: if the journal already holds
//     an entry at an index >= p, the run that wrote it got past this
//     parallel without recording anything inside it under format 2 — only
//     format 1 records branch sleeps/signals/children at later top-level
//     indices — so replay continues as format 1. Otherwise nothing recorded
//     depends on the choice, and the step continues as format 2.
//   Both formats never coexist in one journal: format 1 is only chosen when
//   no format 2 path exists, and vice versa.
// ---------------------------------------------------------------------------

import type { JournalEntry } from "./activity-journal.ts";

/** Legacy branch-path grammar. Replayed, never written for a new journal. */
export const JOURNAL_FORMAT_LEGACY = 1;
/** Current branch-path grammar. Every new journal is written in this format. */
export const JOURNAL_FORMAT_CURRENT = 2;

export type JournalFormatVersion = typeof JOURNAL_FORMAT_LEGACY | typeof JOURNAL_FORMAT_CURRENT;

/**
 * Format of an existing journal, or `undefined` when the journal holds only
 * top-level (`""`) entries and the choice is deferred to the first top-level
 * `ctx.parallel` (see `resolveUndecidedJournalFormat`).
 */
export function detectJournalFormat(
  journal: readonly Pick<JournalEntry, "branchPath">[],
): JournalFormatVersion | undefined {
  if (journal.length === 0) return JOURNAL_FORMAT_CURRENT;
  let legacy = false;
  let current = false;
  for (const entry of journal) {
    const path = entry.branchPath;
    if (path === "") continue;
    if (path.startsWith("/")) current = true;
    else legacy = true;
  }
  if (legacy && current) {
    throw new Error(
      `journal mixes branch-path formats ${JOURNAL_FORMAT_LEGACY} and ${JOURNAL_FORMAT_CURRENT}; ` +
        `it was not written by the journaled-step engine and cannot be replayed`,
    );
  }
  if (current) return JOURNAL_FORMAT_CURRENT;
  if (legacy) return JOURNAL_FORMAT_LEGACY;
  return undefined;
}

/**
 * Decide the format of a journal that `detectJournalFormat` left undecided,
 * at the first top-level `ctx.parallel`, which occupies `parallelIndex`.
 */
export function resolveUndecidedJournalFormat(params: {
  journal: readonly Pick<JournalEntry, "activityIndex">[];
  parallelIndex: number;
}): JournalFormatVersion {
  return params.journal.some((e) => e.activityIndex >= params.parallelIndex)
    ? JOURNAL_FORMAT_LEGACY
    : JOURNAL_FORMAT_CURRENT;
}

/**
 * Path prefix of branch `branch` of a parallel whose own slot has path
 * `parallelPath` (`""` for a top-level parallel).
 */
export function branchPrefix(params: {
  format: JournalFormatVersion;
  parallelPath: string;
  branch: number;
}): string {
  const { format, parallelPath, branch } = params;
  if (format === JOURNAL_FORMAT_LEGACY) {
    return parallelPath ? `${parallelPath}.${branch}` : String(branch);
  }
  return `${parallelPath}/${branch}`;
}

/** Path of the `seq`-th yield inside a branch whose prefix is `prefix`. */
export function pathInBranch(params: {
  format: JournalFormatVersion;
  prefix: string;
  seq: number;
}): string {
  const { format, prefix, seq } = params;
  if (format === JOURNAL_FORMAT_LEGACY) {
    if (seq === 0) return prefix;
    return prefix ? `${prefix}.${seq}` : String(seq);
  }
  return `${prefix}.${seq}`;
}
