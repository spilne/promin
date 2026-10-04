// ---------------------------------------------------------------------------
// Journal format — the branch-path grammar used to key journal entries that
// are recorded inside `ctx.parallel` branches.
//
// Every journal entry is keyed by `(activityIndex, branchPath)`. Top-level
// yields always use `branchPath = ""` and draw `activityIndex` from the
// step's sequential counter. A yield INSIDE a parallel branch shares the
// parallel's `activityIndex` and is addressed by its branch path:
//
//   path    := segment+
//   segment := "/" branch "." seq
//
// `branch` is the branch position in its `ctx.parallel([...])` call and
// `seq` the 0-based yield counter inside that branch. A yield's path lists
// one segment per enclosing parallel, outermost first:
//   ctx.parallel([A, function* () { yield* B; yield* ctx.parallel([C, D]) }])
//     A → "/0.0"   B → "/1.0"   C → "/1.1/0.0"   D → "/1.1/1.0"
// Every yield inside a branch (activity, nested parallel, sleep, signal,
// child) consumes the branch's `seq` counter, so a slot depends only on the
// branch's own control flow, never on how fast siblings complete. The
// grammar uses only digits, "." and "/" so every backend stores it in a
// plain `branch_path` text column.
// ---------------------------------------------------------------------------

/**
 * Path prefix of branch `branch` of a parallel whose own slot has path
 * `parallelPath` (`""` for a top-level parallel).
 */
export function branchPrefix(params: { parallelPath: string; branch: number }): string {
  return `${params.parallelPath}/${params.branch}`;
}

/** Path of the `seq`-th yield inside a branch whose prefix is `prefix`. */
export function pathInBranch(params: { prefix: string; seq: number }): string {
  return `${params.prefix}.${params.seq}`;
}
