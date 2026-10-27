// ---------------------------------------------------------------------------
// Journal errors — the engine-integrity failures of a journaled step: the
// body drifted from its journal, or the storage can't journal at all.
// ---------------------------------------------------------------------------

/**
 * Thrown when the engine detects that the step body's structure has diverged
 * from the journaled execution. Catches three kinds of divergence:
 *   - activity-name drift (same index, different name),
 *   - step-type drift (a `sleep` journal entry collides with an `activity`
 *     yield at the same index, etc.),
 *   - payload-hash drift (same activity + index, but the canonicalized
 *     input's SHA-256 disagrees — opt-in via the `payloadHash` option).
 *
 * `expected` and `actual` are short descriptors in a "<kind>=<value>" form
 * suitable for logging and test assertions.
 */
export class JournalNonDeterminismError extends Error {
  readonly _tag = "JournalNonDeterminismError";
  constructor(
    readonly stepName: string,
    readonly activityIndex: number,
    readonly expected: string,
    readonly actual: string,
  ) {
    super(
      `journaled step "${stepName}" diverged at activity ${activityIndex}: ` +
        `expected "${expected}", got "${actual}". ` +
        `This usually means workflow code changed between runs. ` +
        `Either bump the workflow \`version\` (strict policy throws cleanly) ` +
        `or use \`onVersionMismatch: "drain"\` + \`previousVersions\` to let ` +
        `in-flight workflows finish on their original code.`,
    );
  }
}

/**
 * Thrown when a `.journaled()` step runs on a storage that doesn't
 * implement `JournalStore`. Checked when the step executes (a
 * built workflow has no storage until a runner binds one), before the body
 * starts, rather than silently losing journal entries.
 */
export class JournalStorageMissingError extends Error {
  readonly _tag = "JournalStorageMissingError";
  constructor(stepName: string) {
    super(
      `journaled step "${stepName}" requires a WorkflowStorage that implements ` +
        `JournalStore. Supported built-in backends: InMemoryWorkflowStorage, ` +
        `PostgresWorkflowStorage, RedisWorkflowStorage, SqliteWorkflowStorage, ` +
        `RemoteWorkflowStorage. Extend your custom storage with loadJournal, ` +
        `appendEntry, appendPendingEntry, completePendingEntry, findDueSleeps and ` +
        `findPendingSignal if you need a different backend.`,
    );
  }
}
