// ---------------------------------------------------------------------------
// JournaledStep — generator-based step body with per-activity replay
//
// A `.journaled()` step body is a generator function. Each `yield* ctx.activity(name, fn)`
// is a checkpoint: on first run the activity fn executes and its result is
// appended to the activity journal; on retry/replay the generator re-runs from
// the top, and already-journaled activities return their recorded value
// without re-executing.
//
// Execution model (like `co` / `redux-saga`):
//   - Body is a sync generator (`function*`), NOT async generator.
//   - ctx.activity() builds the Promise for the side effect + journal write,
//     then yields the promise.
//   - The runner awaits each yielded promise and resumes the generator with
//     the resolved value via `.next(value)`. Errors flow via `.throw(err)`.
//
// Why sync generator: the body signature `Generator<ActivityYield, R, any>`
// makes raw `await` a compile-time type error — every side effect must go
// through `ctx.activity`, which is what makes the journal/replay safe.
//
// Failures and step attempts:
//   - An activity (or child) that fails after its own `retry` is journaled
//     as a Failure with the error's `_tag` / `name` and fields. While the
//     body that saw it is still in flight (suspended on sleep/signal, or
//     re-driven after a crash) replay rethrows an error of the same kind, so
//     a body that catches the failure takes the same branch again.
//   - When a failure escapes the body, the step attempt is over. After the
//     compensation unwind, the engine discards the attempt's recorded
//     failures (failed compensations included, so the next unwind retries
//     them), plus every activity whose compensation completed together with
//     that compensation's row: its effect was rolled back. The next attempt
//     (step-level `retry`, or a later resume of the failed workflow)
//     re-executes those activities. Successful activities that were not
//     rolled back still replay and never run twice.
//   - Control-flow and engine-integrity exits (suspend, continue-as-new,
//     tripwire, non-determinism, ambiguous outcome, lock loss) discard
//     nothing.
// ---------------------------------------------------------------------------

import { retryAsync, type RetryPolicy } from "../shared/retry-policy.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import { LosslessJsonCodec, payloadHash as hashPayload } from "@spilne/perfect-core/connect";
import {
  isJournaledSuspendStorage,
  type CompletePendingResult,
  type JournalEntry,
  type JournalExit,
  type JournalSlot,
  type ActivityJournalStorage,
  type JournaledSuspendStorage,
} from "./activity-journal.ts";
import {
  decodeSignalExitValue,
  deliveredSignalExitValue,
  failureExit,
  rehydrateFailure,
  timedOutSignalExitValue,
} from "./journal-exit.ts";
import type { Workflow } from "./durable-pipeline.ts";
import {
  AmbiguousActivityOutcome,
  LoopLimitExceededError,
  RetryableError,
  TerminalError,
  WorkflowContinueAsNewError,
  WorkflowSuspendedError,
} from "./durable-pipeline-error.ts";
import {
  activityScope,
  journaledBodyScope,
  nextPathInScope,
  type ActivityScope,
} from "./journaled-body-scope.ts";
import {
  JOURNAL_FORMAT_LEGACY,
  branchPrefix,
  detectJournalFormat,
  resolveUndecidedJournalFormat,
  type JournalFormatVersion,
} from "./journal-format.ts";
import { registerQueryHandler } from "./query-registry.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";
import {
  approvalSignal,
  type ApprovalDecision,
  type SignalType,
} from "../signals/define-signal.ts";

// ---------------------------------------------------------------------------
// Ctx types
// ---------------------------------------------------------------------------

/**
 * The yield shape from a journaled step body. The runner reads `.promise`
 * to do the async work; `.name` is preserved for debugging/observability.
 */
export interface ActivityYield {
  readonly _tag: "Activity";
  readonly name: string;
  readonly promise: Promise<unknown>;
}

/** Per-activity configuration. */
export interface ActivityOptions<T = unknown> {
  readonly retry?: RetryPolicy<unknown>;
  /**
   * Override the codec used for this activity's result. Defaults to the
   * step's codec (which defaults to LosslessJsonCodec at the pipeline level),
   * so every activity round-trips through a lossless serializer unless the
   * caller opts out explicitly.
   */
  readonly codec?: Codec<unknown>;
  /**
   * Whether re-running this activity is safe. Controls behaviour when a
   * worker crashes between the pending-row write and the completion write —
   * the engine can't tell whether the side effect ran:
   *
   * - `false` (default) — throw `AmbiguousActivityOutcome`. Safe default
   *   for payments, emails, webhooks, any external side effect you can't
   *   deduplicate. The workflow halts; operator inspects the external
   *   system and either resumes, compensates, or cancels.
   * - `true` — re-run the activity body. Use only when the call is
   *   naturally idempotent (pure computation, GET request, upsert by key
   *   with a caller-supplied idempotency header, etc.).
   */
  readonly idempotent?: boolean;
  /**
   * Rollback callback for saga-style intra-step compensation. Registered
   * AFTER the activity completes successfully (on fresh run OR replay) and
   * popped in reverse order if a LATER activity in the same journaled body
   * throws. Each compensation runs as its own journaled activity
   * (stepType="compensation") so replay after a crash is safe.
   *
   * Receives the activity's successful return value for easy "create here,
   * cancel here" patterns. Does NOT run if this activity itself fails —
   * nothing was done to roll back.
   *
   * Compensations run only for genuine failures. Control-flow and
   * engine-integrity exits propagate without running them: suspension
   * (`ctx.sleep` / `ctx.signal`), `ctx.continueAsNew`, tripwire,
   * `JournalNonDeterminismError` (the code drifted from the journal — a
   * deploy problem, not a business failure), `AmbiguousActivityOutcome`
   * (an operator must inspect the external system first) and lock loss
   * (`WorkflowLockError` / `FenceTokenMismatchError` — another worker owns
   * the workflow now). Completed work and the journal are left as they are.
   *
   * ```ts
   * yield* ctx.activity("createOrder", () => api.create(), {
   *   compensate: (order) => api.cancel(order.id),
   * });
   * ```
   */
  readonly compensate?: (result: T) => void | Promise<void>;
  /**
   * Opt in or out of payload hashing for this activity. Requires the 3-arg
   * `ctx.activity(name, input, fn)` form — the 2-arg form can't hash
   * because the input isn't reified. When set, the engine canonicalizes
   * `input` to SHA-256 hex, stores it on the journal row, and on replay
   * compares the stored hash against the recomputed one. A mismatch throws
   * `JournalNonDeterminismError` to catch silent payload drift (same
   * activity name, different input across runs).
   *
   * - `true` — hash this activity's input. Throws at call time if used with
   *   the 2-arg form, since there's no separate input to hash.
   * - `false` — explicit opt-out even when the pipeline enables hashing
   *   globally via `workflow({ payloadHash: true })`.
   * - missing — inherit the pipeline default (off by default).
   */
  readonly payloadHash?: boolean;
}

/**
 * Context passed to a journaled step body. The ONLY supported way to produce
 * side effects is `ctx.activity()`. Direct I/O in the body will re-fire on
 * replay and cause non-determinism bugs.
 */
export interface JournaledContext<Input, Prev> {
  readonly input: Input;
  readonly prev: Prev;
  readonly workflowId: string;

  /**
   * `true` when the body is re-executing on top of pre-existing journal
   * entries — i.e. a worker restart / signal-resume / continueAsNew
   * recovery is replaying earlier yields from the journal. `false` on
   * the very first execution (no journal entries yet).
   *
   * The flag is fixed for the duration of one body invocation. It does
   * NOT flip to `false` mid-body when the cursor passes the journal
   * tail; "this body has run before" is the useful question for hook
   * authors, and the simpler answer.
   *
   * Use it to gate non-idempotent side effects in code that runs
   * BETWEEN `ctx.activity` yields (the body re-runs from the top each
   * worker pass, so unguarded `metrics.record(...)` between yields
   * double-counts on every restart). Side effects INSIDE
   * `ctx.activity` callbacks don't need this gate — journal hits
   * short-circuit the callback so it only fires fresh.
   *
   * ```ts
   * .journaled("step", function*(ctx) {
   *   const result = yield* ctx.activity("a", () => api.fetch());
   *   if (!ctx.isReplay) metrics.record("step.progressed");
   *   return result;
   * })
   * ```
   */
  readonly isReplay: boolean;

  /**
   * The version the workflow row was created under, as stored in the DB.
   * Exposed for user-space custom version-comparison logic (e.g. semver,
   * date-based ordering) when `ctx.patched()`'s set-membership model
   * isn't enough. The framework itself uses this ONLY for display — all
   * drain/patch logic uses equality/membership.
   *
   * `undefined` when the workflow was created without a `version` field.
   */
  readonly workflowVersion?: string;

  /**
   * Returns `true` if `name` is in the currently-running workflow
   * definition's `patches` array. Inline version branches:
   *
   * ```typescript
   * if (ctx.patched("use-new-pricing")) {
   *   // v2+ code path
   * } else {
   *   // v1 code path (for workflows resuming under v1's definition)
   * }
   * ```
   *
   * Pure set membership — no version comparison. The drain policy ensures
   * each stored version's own definition (with its own patch list) is the
   * one running, so `ctx.patched` naturally reflects what the stored
   * version knew about.
   *
   * Throws if `name` wasn't declared in the workflow's `patches` config —
   * catches typos at runtime instead of silently returning false.
   */
  patched(name: string): boolean;

  /**
   * Record an activity as a journal checkpoint. First run executes `fn`,
   * persists the result, resolves to it. Replay resolves to the persisted
   * value without calling `fn`.
   *
   * Two forms:
   *
   * - 2-arg `ctx.activity(name, fn)` — inputs are captured inside the
   *   closure. Simplest and what most activities need.
   * - 3-arg `ctx.activity(name, input, fn)` — input is reified as a
   *   separate arg so the engine can canonicalize + fingerprint it via
   *   SHA-256. Required when `payloadHash` is enabled (per-activity
   *   option or pipeline-level `workflow({ payloadHash: true })`). On
   *   replay the engine recomputes the hash and throws
   *   `JournalNonDeterminismError` on drift — catches "same activity
   *   name, different input" bugs that the 2-arg form can't see through
   *   the closure.
   *
   * Failures: if `fn` still throws after `options.retry`, the failure is
   * journaled and rethrown. A replay inside the same step attempt (resume
   * after sleep/signal, crash recovery) rethrows an error of the same kind:
   * `TerminalError` / `RetryableError` and the other engine errors come back
   * as their own class, any other `_tag` comes back as an `Error` with that
   * `_tag`, `name` and fields. Once a failure escapes the body, the recorded
   * failures are discarded, so a step-level `retry` (or a later resume)
   * runs the failed activity again while successful ones replay.
   *
   * Must be consumed with `yield*` — the sub-generator delegates its single
   * yielded promise to the runner and returns the resolved value.
   */
  activity<T>(
    name: string,
    fn: () => T | Promise<T>,
    options?: ActivityOptions<T>,
  ): Generator<ActivityYield, T, T>;
  activity<I, T>(
    name: string,
    input: I,
    fn: (input: I) => T | Promise<T>,
    options?: ActivityOptions<T>,
  ): Generator<ActivityYield, T, T>;

  /**
   * Durable sleep inside a journaled step. First run writes a pending journal
   * entry with `wakeAt = now + duration` and throws `WorkflowSuspendedError`,
   * releasing the worker. Replay after the scanner (or test driver) completes
   * the entry resolves to the actual wake time.
   *
   * Requires the configured storage to implement `JournaledSuspendStorage`;
   * throws a clear error at first use if not.
   */
  sleep(duration: number | Date): Generator<ActivityYield, Date, Date>;

  /**
   * Durable signal wait inside a journaled step. First run writes a pending
   * journal entry naming the signal and throws `WorkflowSuspendedError`.
   * External `completeSignal(...)` delivers a value, completes the entry,
   * and enqueues resume. Replay returns the delivered value.
   *
   * The generic `T` types the delivered payload; runtime validation via a
   * per-signal Zod codec is a planned refinement.
   *
   * With a `timeout`, the suspend is bounded: the existing sleep scanner
   * completes pending signals past their `wakeAt` with a timeout outcome,
   * and the call returns a result envelope instead of `T` directly. This
   * mirrors the timeout already available on the builder-level
   * `.waitForSignal({ timeoutMs })` step.
   *
   * A delivery and the timeout race for the same journal entry; whichever
   * completes it first wins, and both the live run and every replay take
   * that outcome. The entry stores a tagged value (delivered or timeout), so
   * a delivered payload shaped like `{ ok: false, ... }` is returned as
   * `{ ok: true, value: { ok: false, ... } }`, never mistaken for a timeout.
   */
  signal<T>(name: string): Generator<ActivityYield, T, T>;
  signal<T>(
    name: string,
    options: { readonly timeout: number | Date },
  ): Generator<
    ActivityYield,
    { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: "timeout" },
    { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: "timeout" }
  >;

  /**
   * Typed signal wait — same suspend/resume semantics as `ctx.signal`, but
   * keyed by a `SignalType` artifact (`defineSignal({ name, schema })`).
   * The JSON Schema snapshot is persisted on the suspended step so the
   * server can validate any future delivery against the shape the workflow
   * actually waited on — even if the SignalType definition later evolves.
   *
   * The return type is the schema's payload type (via the `SignalType<T>`
   * phantom), so callers get a narrowed result without an `as` cast.
   */
  validatedSignal<T>(sig: SignalType<T>): Generator<ActivityYield, T, T>;
  validatedSignal<T>(
    sig: SignalType<T>,
    options: { readonly timeout: number | Date },
  ): Generator<
    ActivityYield,
    { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: "timeout" },
    { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: "timeout" }
  >;

  /**
   * Approval preset — sugar over `validatedSignal(approvalSignal(id))`. The
   * wire-format signal name is `approve:<id>` (matches the existing
   * convention SignalScanner and the dashboard `/signals` Approve/Reject
   * shortcut already use). Returns the canonical `ApprovalDecision`
   * `{ approved, by?, reason?, metadata? }`.
   */
  approval(id: string): Generator<ActivityYield, ApprovalDecision, ApprovalDecision>;
  approval(
    id: string,
    options: { readonly timeout: number | Date },
  ): Generator<
    ActivityYield,
    | { readonly ok: true; readonly value: ApprovalDecision }
    | { readonly ok: false; readonly error: "timeout" },
    | { readonly ok: true; readonly value: ApprovalDecision }
    | { readonly ok: false; readonly error: "timeout" }
  >;

  /**
   * Run `branches` concurrently and resolve to their results in input order.
   * Each branch is a sub-generator — typically `ctx.activity(...)` — that
   * yields its own ActivityYields. The engine drives each branch in its own
   * scope so their journal entries stay uniquely identified by branch path
   * without racing for the shared activity_index counter.
   *
   * Nesting is supported: a branch can itself be `ctx.parallel([...])`.
   * `ctx.sleep`, `ctx.signal` and `ctx.child` inside a branch take their
   * journal slot from the branch, so a slot depends only on the branch's
   * own control flow, never on how fast sibling branches complete. A
   * branch that suspends rejects the whole parallel with
   * `WorkflowSuspendedError`; the step resumes from the journal later.
   * See `journal-format.ts` for the branch-path grammar.
   *
   * ```ts
   * const [user, perms] = yield* ctx.parallel([
   *   ctx.activity("fetch-user", () => api.user(input.id)),
   *   ctx.activity("fetch-perms", () => api.perms(input.id)),
   * ]);
   * ```
   *
   * Failure today uses Promise.all semantics: the first branch failure
   * rejects the whole parallel; still-running branches complete in the
   * background. Structured cancellation is planned as a follow-up.
   */
  parallel<T>(
    branches: ReadonlyArray<Generator<ActivityYield, T, T>>,
  ): Generator<ActivityYield, T[], unknown>;

  /**
   * Run another workflow inline as a durable nested execution. The child runs
   * as a separate workflow record in storage (own workflowId, own journal, own
   * compensation scope). The parent step waits for the child to complete and
   * receives its result. On replay the result is read from the journal — the
   * child is not re-executed.
   *
   * The child `workflowId` defaults to
   * `"${parentWorkflowId}.${stepName}.${activityIndex}"` so replay always
   * locates the same child record without extra bookkeeping. Inside a
   * `ctx.parallel` branch the branch path is appended, with "/" written as
   * "~" (e.g. `"wf.step.2~0.1"`).
   *
   * ```ts
   * .journaled("signup", function*(ctx, input) {
   *   const user = yield* ctx.activity("create-user", () => createUser(input));
   *   const enrichment = yield* ctx.child(enrichWorkflow, {
   *     input: { userId: user.id },
   *     workflowId: `enrich-${user.id}`,
   *   });
   *   return { user, enrichment };
   * })
   * ```
   *
   * Requires the runner to supply a `runChild` callback to `runJournaledStep`.
   * Throws a clear error if invoked without one (e.g. in a unit test that
   * drives `runJournaledStep` directly without the callback).
   */
  child<Output>(
    workflow: Workflow<unknown, Output>,
    options?: { readonly input?: unknown; readonly workflowId?: string },
  ): Generator<ActivityYield, Output, Output>;

  /**
   * Iterate `fn` as a sequence of journaled activities until `condition`
   * returns `false`. Each iteration lands in the activity journal as its
   * own entry (named `"${name}-iter-${n}"`), so a worker crash resumes at
   * the next un-journaled iteration rather than restarting from zero.
   *
   * Use inside a `.journaled()` body when the iteration is tight and
   * in-process — polling an external system, accumulating until a
   * threshold, retrying a lightweight check. For iteration over real work
   * that should distribute across workers, use the builder-level
   * `.dowhile()` / `.dountil()` which create DAG-visible step rows per
   * iteration.
   *
   * Body always runs at least once. Exits when `condition(result, iter)`
   * returns `false` or when `maxIterations` (default 100) is exceeded —
   * overflow raises `LoopLimitExceededError`.
   *
   * ```ts
   * const final = yield* ctx.dowhile(
   *   "poll",
   *   async (iter) => await checkStatus(input.id),
   *   (status) => status === "pending",
   * );
   * ```
   */
  dowhile<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown>;

  /**
   * Inverse polarity of `ctx.dowhile` — iterate `fn` until `condition`
   * returns `true`. Body always runs at least once. See `ctx.dowhile` for
   * journaling / max-iteration / use-case notes.
   *
   * ```ts
   * const final = yield* ctx.dountil(
   *   "drain",
   *   () => pullBatch(100),
   *   (batch) => batch.length === 0,
   * );
   * ```
   */
  dountil<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown>;

  /**
   * Terminate the current execution and start a fresh run under the same
   * workflowId with new input. The
   * pattern for long-running workflows that would otherwise accumulate
   * unbounded journal entries — the canonical example is a workflow that
   * loops forever processing batches.
   *
   * ```ts
   * .journaled("batch-loop", function* (ctx) {
   *   const batch = yield* ctx.activity("fetch", () => api.fetch(ctx.input.batchId));
   *   yield* ctx.activity("process", () => api.process(batch));
   *   ctx.continueAsNew({ batchId: ctx.input.batchId + 1 });
   * });
   * ```
   *
   * Semantics:
   * - Throws `WorkflowContinueAsNewError` to unwind the body.
   * - The runner catches it, calls `storage.startFreshRun(workflowId)` to
   *   archive the current run + reset state, then runs the workflow again
   *   under the same workflowId with `nextInput`.
   * - **Compensations do NOT run.** Continue-as-new is a clean restart,
   *   not a failure — the activities that already succeeded stay
   *   succeeded in the run history.
   * - Replay-safe: `startFreshRun` deletes the current run's activity
   *   journal, so the next run starts from an empty journal and never
   *   replays the previous chain link's activity results.
   *
   * Returns `never` because control unwinds — the call site is the last
   * thing the body executes.
   */
  continueAsNew(nextInput: unknown): never;

  /**
   * Register a query handler — a read of in-memory workflow state for
   * external callers (dashboard, ops tooling). They invoke
   * `handle.query(name, args?)` to get a snapshot without hitting
   * durable storage.
   *
   * ```ts
   * .journaled("checkout", function* (ctx) {
   *   let status = "pending";
   *   ctx.setQueryHandler("status", () => status);
   *   status = yield* ctx.activity("validate", () => api.validate(ctx.input.orderId));
   *   status = yield* ctx.activity("charge", () => api.charge(...));
   *   return { ok: true };
   * });
   * ```
   *
   * Semantics:
   * - Handlers are scoped per-`(workflowId, name)`. Re-registering the
   *   same name in the same body replaces the prior handler.
   * - **Not journaled.** Calling `setQueryHandler` writes to in-memory
   *   state on the running worker. On replay (worker restart) the body
   *   re-registers as it re-runs; queries against a stopped run return
   *   `WorkflowNotRunningError`.
   * - The handler is called outside the journaled body's generator
   *   context — must NOT yield activities or call `ctx.*` (treat it as
   *   a pure read of closure-captured state).
   * - Return value must be JSON-safe — sent back over the wire.
   */
  setQueryHandler<R>(name: string, handler: (args?: unknown) => R | Promise<R>): void;

  /**
   * Live-writable metadata surfaced on the workflow row. The dashboard reads
   * `WorkflowState.metadata`, so writes through `ctx.metadata.set/merge`
   * appear in the runs view in near-real-time. Useful for progress
   * surfacing (`ctx.metadata.set("progress", "7/12")`), feature-flag
   * snapshots, or any small JSON-safe state you want operators to see
   * without spelunking through the journal.
   *
   * Semantics:
   *   - Writes are fire-and-forget: storage is updated as a side effect of
   *     calling `set`/`merge`, but the body doesn't yield. Errors are
   *     surfaced through the standard step-failure path on the next
   *     activity yield (a metadata-write storage error fails the step).
   *   - Replay-safe: the same writes re-fire on body re-run with the same
   *     values. Idempotent against the storage's merge semantics.
   *   - Shallow merge — top-level keys in the patch overwrite the same
   *     keys on existing metadata. Pass `null` for a key to remove it.
   *   - `get()` returns the snapshot loaded at body-start time; in-body
   *     `set()`s are visible to subsequent `get()`s within the same
   *     invocation. Cross-invocation visibility goes through storage.
   */
  readonly metadata: {
    set(key: string, value: unknown): void;
    merge(patch: Record<string, unknown>): void;
    get(): Record<string, unknown>;
  };

  /**
   * Bind a record of activity functions into a typed proxy where each
   * method is journal-recorded under its property key. Sugar over the
   * 2-arg `ctx.activity(name, fn)` form — same semantics, less ceremony:
   *
   * ```ts
   * .journaled("checkout", function*(ctx) {
   *   const { validate, charge, ship } = ctx.proxy({ validate, charge, ship });
   *   const order = yield* validate(ctx.input.orderId);
   *   const tx    = yield* charge(order);
   *   return yield* ship(tx);
   * });
   * ```
   *
   * Each proxied call expands to `ctx.activity(<key>, () => fn(...args))`,
   * so the journal name is the property key (not `"anonymous"`), and
   * replay-determinism rules apply unchanged. Use `optionsByName` to
   * forward per-activity `ActivityOptions` (retry / codec / idempotent /
   * compensate); `defaultOptions` applies to every key that doesn't have
   * its own entry.
   *
   * Composes with the existing 2-arg / 3-arg `ctx.activity` form — the
   * proxy doesn't replace it, it just removes the closure boilerplate
   * for static activity sets. For dynamic names or 3-arg payload hashing,
   * keep using `ctx.activity` directly.
   */
  proxy<Acts extends Record<string, (...args: any[]) => any>>(
    activities: Acts,
    options?: {
      readonly defaultOptions?: ActivityOptions<unknown>;
      readonly optionsByName?: { readonly [K in keyof Acts]?: ActivityOptions<unknown> };
    },
  ): {
    readonly [K in keyof Acts]: (
      ...args: Parameters<Acts[K]>
    ) => Generator<ActivityYield, Awaited<ReturnType<Acts[K]>>, unknown>;
  };
}

/** The body function passed to `.journaled()`. */
export type JournaledStepBody<Input, Prev, Output> = (
  ctx: JournaledContext<Input, Prev>,
  prev: Prev,
) => Generator<ActivityYield, Output, unknown>;

// ---------------------------------------------------------------------------
// Errors
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
 * Thrown when a `.journaled()` step is built on a storage that doesn't
 * implement `ActivityJournalStorage`. Fail-loud at build time rather than
 * silently losing journal entries at runtime.
 */
export class JournalStorageMissingError extends Error {
  readonly _tag = "JournalStorageMissingError";
  constructor(stepName: string) {
    super(
      `journaled step "${stepName}" requires a WorkflowStorage that implements ` +
        `ActivityJournalStorage. Supported built-in backends: InMemoryWorkflowStorage, ` +
        `PostgresWorkflowStorage, RedisWorkflowStorage. Extend your custom storage ` +
        `with loadJournal/appendEntry if you need a different backend.`,
    );
  }
}

// ---------------------------------------------------------------------------
// Ctx factory — builds the Promise inside activity() and yields it
// ---------------------------------------------------------------------------

function makeCtx<Input, Prev>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  journal: JournalEntry[];
  storage: ActivityJournalStorage;
  /**
   * Full WorkflowStorage — only used by ctx.sleep/ctx.signal to call
   * suspendWorkflow() so the existing DefaultSleepScanner picks up
   * journal-suspended workflows.
   */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — exposed on ctx for user-space logic. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
  /**
   * Default codec inherited from the enclosing step; individual activities
   * may override via `ActivityOptions.codec`. Falls back to
   * LosslessJsonCodec so the fresh-run ↔ replay invariant holds even when
   * the step didn't pass one.
   */
  defaultCodec?: Codec<unknown>;
  /**
   * Workflow-level default for `ActivityOptions.payloadHash`. When `true`,
   * 3-arg `ctx.activity(name, input, fn)` calls hash by default; per-call
   * `payloadHash: false` overrides. The 2-arg form is unaffected.
   */
  defaultPayloadHash?: boolean;
  /**
   * Executes a child workflow inline. Provided by the `.journaled()` step's
   * execute closure so `ctx.child()` can call the runner without coupling
   * `journaled-step.ts` to `workflow-runner.ts`. Omitting it causes
   * `ctx.child()` to throw a clear error at call time.
   */
  runChild?: (params: {
    workflow: Workflow<unknown, unknown>;
    workflowId: string;
    input: unknown;
  }) => Promise<unknown>;
  /**
   * Snapshot of workflow metadata as of body start. `ctx.metadata.get()`
   * returns this layered with any in-body `set/merge` writes; storage is
   * updated as a side effect of those writes via `workflowStorage`.
   */
  initialMetadata?: Record<string, unknown>;
  /** Time source for sleep / signal deadlines and activity retry backoff. */
  clock?: WallClock;
  /** Fence guard passed on the ctx.sleep / ctx.signal suspension writes. */
  guard?: FenceGuard;
}): { ctx: JournaledContext<Input, Prev>; unwind: (bodyError: unknown) => Promise<JournalSlot[]> } {
  const {
    input,
    prev,
    workflowId,
    stepName,
    journal,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
    defaultCodec,
    defaultPayloadHash,
    runChild,
    initialMetadata,
    guard,
  } = params;
  const clock = params.clock ?? SystemWallClock;
  const stepCodec = defaultCodec ?? LosslessJsonCodec;
  const patchSet = new Set(patches ?? []);
  // Single counter for every journal slot — activities, sleeps, signals, and
  // compensations all draw from it. Compensations reserve their index at
  // registration time (right after the activity succeeds), BEFORE any later
  // activity is called, so replay re-derives identical indices from a
  // deterministic body. This avoids the PK collision that would arise if
  // activities and compensations each had their own 0-based counter.
  const indexRef = { next: 0 };
  // Journal lookups use the composite key `${activityIndex}:${branchPath}`.
  // At top level branchPath is always `""`, so this degrades to a plain
  // integer lookup.
  const journalKey = (activityIndex: number, branchPath: string): string =>
    `${activityIndex}:${branchPath}`;
  const journalByKey = new Map<string, JournalEntry>(
    journal.map((e) => [journalKey(e.activityIndex, e.branchPath), e]),
  );
  // Branch-path grammar for this step's journal. `undefined` until the
  // first top-level ctx.parallel when the loaded journal has only top-level
  // entries — see `journal-format.ts`.
  const formatRef: { current: JournalFormatVersion | undefined } = {
    current: detectJournalFormat(journal),
  };

  /**
   * Allocate the journal slot for one yield. Inside a parallel branch the
   * branch scope supplies it; at top level the step's counter does.
   *
   * `ctx.sleep` / `ctx.signal` / `ctx.child` pass `suspendOrChild: true`:
   * a format 1 journal recorded those on the top-level counter even inside
   * a branch, so replaying one keeps that allocation.
   */
  function allocateSlot(params: { suspendOrChild: boolean }): {
    activityIndex: number;
    branchPath: string;
  } {
    const scope = activityScope.getStore();
    if (scope && !(params.suspendOrChild && scope.format === JOURNAL_FORMAT_LEGACY)) {
      return { activityIndex: scope.parallelActivityIndex, branchPath: nextPathInScope(scope) };
    }
    return { activityIndex: indexRef.next++, branchPath: "" };
  }

  /**
   * Complete a pending slot and return the exit the journal now holds. When
   * another writer completed the slot first (a signal delivery beating the
   * timeout, or a second worker), that writer's exit is returned and
   * `won` is false: the caller must continue with it so the live run and
   * replay agree.
   */
  async function completeSlot(params: {
    slot: JournalSlot;
    exit: JournalExit;
    /**
     * Read the row back when the storage predates `CompletePendingResult`
     * and reports nothing. Only worth the read where a race is expected.
     */
    readBack: boolean;
  }): Promise<{ won: boolean; exit: JournalExit }> {
    return completeAndReport({
      storage: storage as JournaledSuspendStorage,
      workflowId,
      stepName,
      ...params,
    });
  }

  interface Compensation {
    readonly activityIndex: number; // reserved at registration time
    /** Slot of the activity this compensation rolls back. */
    readonly sourceActivityIndex: number;
    readonly activityName: string; // for diagnostics
    readonly run: () => Promise<void>; // bound to the activity's result
  }
  const compensations: Compensation[] = [];

  function* activity<I, T>(
    name: string,
    argA: I | (() => T | Promise<T>),
    argB?: ActivityOptions<T> | ((input: I) => T | Promise<T>),
    argC?: ActivityOptions<T>,
  ): Generator<ActivityYield, T, T> {
    // Disambiguate the 2-arg vs 3-arg overload.
    //   2-arg: ctx.activity(name, fn, options?)     — argA is fn
    //   3-arg: ctx.activity(name, input, fn, opts?) — argB is fn
    // We key off whether argB is a function: if so, argA is the reified
    // input and argB is the unary activity fn. Otherwise argA must be the
    // zero-arg fn.
    let fn: () => T | Promise<T>;
    let hasInput: boolean;
    let boundInput: I | undefined;
    let options: ActivityOptions<T> | undefined;
    if (typeof argB === "function") {
      hasInput = true;
      boundInput = argA as I;
      const unary = argB as (input: I) => T | Promise<T>;
      fn = () => unary(argA as I);
      options = argC;
    } else {
      if (typeof argA !== "function") {
        throw new TypeError(
          `ctx.activity("${name}"): expected 2-arg (name, fn) or 3-arg (name, input, fn) form; ` +
            `got a non-function as the second argument with no activity function in the third.`,
        );
      }
      hasInput = false;
      boundInput = undefined;
      fn = argA as () => T | Promise<T>;
      options = argB as ActivityOptions<T> | undefined;
    }
    // Position resolution: inside a ctx.parallel branch the scope supplies a
    // shared activityIndex + branch path; at top level we consume from the
    // step's flat counter with empty branch path.
    const scope = activityScope.getStore();
    const { activityIndex, branchPath } = allocateSlot({ suspendOrChild: false });
    const codec = options?.codec ?? stepCodec;
    const idempotent = options?.idempotent === true;
    const compensate = options?.compensate;
    // Payload fingerprint — per-activity opt-in wins, else fall back to the
    // pipeline default.
    //   * Explicit `options.payloadHash: true` on the 2-arg form throws —
    //     the caller asked for something they can't have, surface it.
    //   * Workflow-level default on a 2-arg call silently skips instead.
    //     The pipeline flag means "hash where you can"; forcing every
    //     existing 2-arg activity to migrate would make the flag
    //     impractical to enable in a real codebase.
    const explicitHashOpt = options?.payloadHash;
    const wantsHash = explicitHashOpt ?? defaultPayloadHash ?? false;
    if (wantsHash && !hasInput) {
      if (explicitHashOpt === true) {
        throw new Error(
          `ctx.activity("${name}"): \`payloadHash\` requires the 3-arg form ctx.activity(name, input, fn). ` +
            `The 2-arg form captures inputs inside a closure, so there's nothing separate to hash.`,
        );
      }
      // Fell through from pipeline default — no hash, no error.
    }
    const payloadHashValue = wantsHash && hasInput ? hashPayload(boundInput) : undefined;
    if (compensate && scope) {
      // Compensation indices come from the top-level counter; reserving one
      // while concurrent branches are also bumping the counter is racy and
      // would make replay non-deterministic. Forbid the combination today;
      // a per-branch compensation index space is a separate follow-up.
      throw new Error(
        `ctx.activity("${name}"): \`compensate\` is not supported inside a ctx.parallel branch. ` +
          `Hoist the compensation to an activity outside the parallel, or use step-level ` +
          `StepOptions.compensate for rollback.`,
      );
    }
    const maybeRegisterCompensation = (result: T): void => {
      if (!compensate) return;
      // Reserve a journal slot now; the unwind writes the pending+completed
      // rows later. The reservation keeps activity and compensation indices
      // deterministic across replay.
      const reservedIndex = indexRef.next++;
      compensations.push({
        activityIndex: reservedIndex,
        sourceActivityIndex: activityIndex,
        activityName: name,
        run: async () => {
          // Run outside the body scope so Date.now / random inside
          // compensations aren't flagged by instrumentNonDeterminism.
          await journaledBodyScope.exit(async () => {
            await Promise.resolve(compensate(result));
          });
        },
      });
    };
    // Two-phase record requires the pending-entry primitives. Storages that
    // only implement the base ActivityJournalStorage fall back to the legacy
    // single-phase path (side effect risks duplication under worker crash,
    // same as before this change).
    const twoPhase = isJournaledSuspendStorage(storage);

    /** Value of a completed exit: a failure rethrows, a success decodes. */
    const settle = (exit: JournalExit): T => {
      if (exit.tag === "Failure") throw rehydrateFailure(exit);
      const value = codec.decode(exit.value) as T;
      maybeRegisterCompensation(value);
      return value;
    };

    // Build the async work for this activity. Replay-or-run is decided here
    // so the runner sees a single awaitable Promise regardless of path.
    const promise = (async (): Promise<T> => {
      const recorded = journalByKey.get(journalKey(activityIndex, branchPath));
      if (recorded) {
        // Replay path — validate determinism.
        if (recorded.activityName !== name) {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            recorded.activityName,
            name,
          );
        }
        // Seeing a sleep/signal journal entry at an activity yield index
        // means the user swapped an activity for a sleep/signal at the same
        // position between runs — a determinism bug.
        const recordedType = recorded.stepType ?? "activity";
        if (recordedType !== "activity") {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `${recordedType}:${recorded.activityName}`,
            `activity:${name}`,
          );
        }
        // Payload-hash drift check. Fires only when BOTH sides opted in:
        //   - recorded.payloadHash: the original run stored a fingerprint
        //   - payloadHashValue:     this replay also computed one
        // Asymmetric cases (hashing toggled on or off between runs) don't
        // throw — that's a migration, not a bug. Forcing strictness there
        // would trap in-flight workflows whenever the operator flipped the
        // pipeline-level flag. If both hashes exist and they disagree,
        // something fed the same-named activity a different input on replay
        // — the exact silent drift this option is meant to surface.
        if (recorded.payloadHash && payloadHashValue && recorded.payloadHash !== payloadHashValue) {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `payloadHash=${recorded.payloadHash}`,
            `payloadHash=${payloadHashValue}`,
          );
        }
        const phase = recorded.phase ?? "completed";
        if (phase === "completed") {
          if (!recorded.exit) {
            throw new Error(
              `journal entry ${activityIndex} for step "${stepName}" is completed but has no exit`,
            );
          }
          // Register compensation on replay too — a LATER activity in this
          // run might still fail, and the unwind needs to call our rollback
          // with the replayed value.
          return settle(recorded.exit);
        }
        // Pending row on replay — the worker that started this activity
        // crashed between the pending write and the completion write. The
        // side effect may or may not have run.
        if (!idempotent) {
          throw new AmbiguousActivityOutcome({
            workflowId,
            stepName,
            activityIndex,
            activityName: name,
            message:
              `activity "${name}" (step "${stepName}", index ${activityIndex}) was ` +
              `interrupted after starting but before completing, and is not marked ` +
              `idempotent. Inspect the external system and either mark idempotent, ` +
              `compensate, or fail the workflow.`,
          });
        }
        // Idempotent: fall through and re-run. appendPendingEntry is a no-op
        // on the existing pending row, completePendingEntry below updates it
        // with the new exit.
      }

      // Fresh-run path — optionally write a pending row, execute, then
      // complete (or fall back to single-phase append).
      if (twoPhase) {
        await storage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          branchPath,
          activityName: name,
          payloadHash: payloadHashValue,
          stepType: "activity",
        });
      }

      // Exit the journaledBodyScope before running `fn()` — the scope is
      // meant to flag non-deterministic globals called between `yield*`
      // expressions, NOT the intentional side effects that happen inside an
      // activity. Node's AsyncLocalStorage would otherwise propagate the
      // body scope through every async continuation descending from the
      // generator tick.
      const runOnce = async (): Promise<T> =>
        journaledBodyScope.exit(async () => (await Promise.resolve(fn())) as T);
      let value: T;
      try {
        value = options?.retry
          ? await runWithRetry({ fn: runOnce, policy: options.retry, clock })
          : await runOnce();
      } catch (err) {
        const exit = failureExit(err);
        if (twoPhase) {
          const stored = await completeSlot({
            slot: { activityIndex, branchPath },
            exit,
            readBack: false,
          });
          // Another writer completed the slot first: follow the journal.
          if (!stored.won) return settle(stored.exit);
        } else {
          await storage.appendEntry({
            workflowId,
            stepName,
            activityIndex,
            branchPath,
            activityName: name,
            payloadHash: payloadHashValue,
            exit,
          });
        }
        throw err;
      }
      // Encode for storage, then return the round-tripped value so fresh-run
      // consumers see the same shape they'd see on replay. Without the decode
      // step, a step body that yields `new Date()` would see a real Date on
      // fresh run and a stringified one after restart.
      const encoded = codec.encode(value);
      const successExit = { tag: "Success", value: encoded } as const;
      if (twoPhase) {
        const stored = await completeSlot({
          slot: { activityIndex, branchPath },
          exit: successExit,
          readBack: false,
        });
        return settle(stored.exit);
      }
      await storage.appendEntry({
        workflowId,
        stepName,
        activityIndex,
        branchPath,
        activityName: name,
        payloadHash: payloadHashValue,
        exit: successExit,
      });
      return settle(successExit);
    })();

    // The runner will await the promise and resume via .next(resolvedValue);
    // that resumed value becomes the result of this `yield` expression, which
    // `yield*` hoists as the sub-generator's return value.
    return yield { _tag: "Activity", name, promise };
  }

  // -------------------------------------------------------------------------
  // ctx.sleep / ctx.signal — require JournaledSuspendStorage
  // -------------------------------------------------------------------------

  function requireSuspendStorage(op: "sleep" | "signal"): JournaledSuspendStorage {
    if (!isJournaledSuspendStorage(storage)) {
      throw new Error(
        `ctx.${op}() requires a WorkflowStorage that implements JournaledSuspendStorage. ` +
          `Supported built-in backends: InMemoryWorkflowStorage, PostgresWorkflowStorage, ` +
          `RedisWorkflowStorage. Extend your custom storage with ` +
          `appendPendingEntry/completePendingEntry/findDueSleeps/findPendingSignal ` +
          `if you need a different backend.`,
      );
    }
    return storage;
  }

  function* sleep(duration: number | Date): Generator<ActivityYield, Date, Date> {
    const suspendStorage = requireSuspendStorage("sleep");
    const { activityIndex, branchPath } = allocateSlot({ suspendOrChild: true });
    const name = "sleep";

    const promise = (async (): Promise<Date> => {
      const recorded = journalByKey.get(journalKey(activityIndex, branchPath));
      const recordedType = recorded?.stepType ?? "activity";
      if (recorded && recordedType !== "sleep") {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          `${recordedType}:${recorded.activityName}`,
          "sleep",
        );
      }

      const wokeAt = (exit: JournalExit): Date => {
        if (exit.tag === "Failure") throw rehydrateFailure(exit);
        const raw = exit.value;
        return typeof raw === "string" ? new Date(raw) : (raw as Date);
      };

      // Replay after completion — entry holds the actual wake time.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        return wokeAt(recorded.exit);
      }

      // First run or pending replay — determine wake time.
      // Use the recorded wakeAt when replaying a pending entry so time isn't
      // re-computed (which would drift on every replay).
      const wakeAt =
        recorded?.wakeAt ??
        (duration instanceof Date ? duration : new Date(clock.currentTimeMs() + duration));

      if (!recorded) {
        await suspendStorage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          branchPath,
          activityName: name,
          stepType: "sleep",
          wakeAt,
        });
      }

      // Self-healing replay: if the scanner re-ran us and our wake time has
      // passed, complete the entry here (no external completion needed) and
      // return. The DefaultSleepScanner's existing "run workflow on wake"
      // loop works unchanged — ctx.sleep does its own time check.
      // `completeDueSleeps` may complete the entry at the same moment; both
      // write the same wake time, but the stored exit is what replay sees.
      if (clock.currentTimeMs() >= wakeAt.getTime()) {
        const stored = await completeSlot({
          slot: { activityIndex, branchPath },
          exit: { tag: "Success", value: wakeAt.toISOString() },
          readBack: false,
        });
        return wokeAt(stored.exit);
      }

      // Still sleeping — mark the WORKFLOW as suspended at step level so the
      // existing DefaultSleepScanner (which scans step.wakeAt) picks it up.
      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(
          workflowId,
          stepName,
          { status: "sleeping", wakeAt },
          guard,
        );
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "sleep",
        message: `sleeping until ${wakeAt.toISOString()}`,
      });
    })();

    return yield { _tag: "Activity", name, promise };
  }

  /** Tagged outcome returned by `ctx.signal(name, { timeout })`. */
  type TimedSignalOutcome<T> =
    | { readonly ok: true; readonly value: T }
    | { readonly ok: false; readonly error: "timeout" };

  function* signalImpl<T>(
    signalName: string,
    options?: {
      readonly timeout?: number | Date;
      /**
       * Internal — JSON Schema snapshot for the suspended signal. Persisted
       * onto `step.metadata.signalJsonSchema` so server-side delivery can
       * validate payloads against the shape this suspend point waited on,
       * even if the SignalType definition later evolves. Not exposed on the
       * public `ctx.signal` overload — `ctx.validatedSignal` / `ctx.approval`
       * pass it in.
       */
      readonly jsonSchema?: unknown;
    },
  ): Generator<ActivityYield, T | TimedSignalOutcome<T>, unknown> {
    const suspendStorage = requireSuspendStorage("signal");
    const { activityIndex, branchPath } = allocateSlot({ suspendOrChild: true });
    const hasTimeout = options?.timeout !== undefined;

    const promise = (async (): Promise<T | TimedSignalOutcome<T>> => {
      const recorded = journalByKey.get(journalKey(activityIndex, branchPath));
      const recordedType = recorded?.stepType ?? "activity";
      if (recorded && recordedType !== "signal") {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          `${recordedType}:${recorded.activityName}`,
          `signal:${signalName}`,
        );
      }
      if (recorded && recorded.activityName !== signalName) {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          recorded.activityName,
          signalName,
        );
      }

      // Result of a completed entry. The stored value is tagged delivered /
      // timeout (see `journal-exit.ts`); untagged legacy rows decode by
      // their old shape. With a timeout configured a delivery is wrapped in
      // the `{ ok: true, value }` envelope so `result.ok` works uniformly.
      const outcomeOf = (exit: JournalExit): T | TimedSignalOutcome<T> => {
        if (exit.tag === "Failure") throw rehydrateFailure(exit);
        const outcome = decodeSignalExitValue({ stored: exit.value, hasTimeout });
        if (outcome.kind === "timeout") {
          if (!hasTimeout) {
            // The run that recorded this waited with a timeout; this code
            // waits without one.
            throw new JournalNonDeterminismError(
              stepName,
              activityIndex,
              `signal:${signalName} (timed out)`,
              `signal:${signalName} (no timeout)`,
            );
          }
          return { ok: false, error: "timeout" };
        }
        const value = outcome.value as T;
        return hasTimeout ? { ok: true, value } : value;
      };

      // Replay after delivery or timeout.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        return outcomeOf(recorded.exit);
      }

      // First run (or still pending) — register interest, mark the workflow
      // suspended, then suspend. The DefaultSleepScanner skips workflows
      // without a wakeAt, so an unbounded signal still requires external
      // delivery via `completeSignal`; with a timeout configured, the
      // wakeAt is set so the scanner can complete the entry on expiry.
      // Use the recorded wakeAt on replay so time isn't re-computed (which
      // would drift on every replay).
      const wakeAt = recorded?.wakeAt
        ? recorded.wakeAt
        : options?.timeout !== undefined
          ? options.timeout instanceof Date
            ? options.timeout
            : new Date(clock.currentTimeMs() + options.timeout)
          : undefined;

      if (!recorded) {
        await suspendStorage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          branchPath,
          activityName: signalName,
          stepType: "signal",
          ...(wakeAt && { wakeAt }),
        });
      }

      // Self-healing replay: if the scanner re-ran us and our timeout has
      // passed without a delivery, complete the entry with the timeout
      // outcome and return. Mirrors `ctx.sleep` — the scanner wakes us, the
      // body decides what to do. A delivery that completed the entry after
      // the journal was loaded wins the race: take its value, the same one
      // every replay will see.
      if (wakeAt && clock.currentTimeMs() >= wakeAt.getTime()) {
        const stored = await completeSlot({
          slot: { activityIndex, branchPath },
          exit: { tag: "Success", value: timedOutSignalExitValue() },
          readBack: true,
        });
        return outcomeOf(stored.exit);
      }

      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(
          workflowId,
          stepName,
          {
            status: "waiting_for_signal",
            signalName,
            ...(wakeAt && { signalTimeoutAt: wakeAt }),
            // Schema snapshot — a dedicated field on `StepState`. The server's
            // delivery path (POST /api/runs/:id/signal + the public token
            // complete route) reads `step.signalJsonSchema` and validates
            // inbound payloads against it before calling deliverSignal. Lives
            // on the suspend record (not the journal entry) so it survives a
            // SignalType definition change between suspend and delivery.
            ...(options?.jsonSchema !== undefined && {
              signalJsonSchema: options.jsonSchema,
            }),
          },
          guard,
        );
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "signal",
        message: wakeAt
          ? `waiting for signal "${signalName}" (timeout at ${wakeAt.toISOString()})`
          : `waiting for signal "${signalName}"`,
      });
    })();

    return (yield { _tag: "Activity", name: signalName, promise }) as T | TimedSignalOutcome<T>;
  }

  // -------------------------------------------------------------------------
  // ctx.validatedSignal — typed wrapper around signalImpl
  //
  // Same suspend/resume mechanics, but keyed by a SignalType artifact. The
  // schema's `jsonSchema` is passed through to signalImpl, which writes it
  // onto `step.metadata.signalJsonSchema` so the server can validate any
  // future delivery against the shape this suspend point waited on.
  // -------------------------------------------------------------------------

  // Overloaded declaration so the interface's two-overload shape matches
  // (TS won't accept a single union-return impl assigned to overloaded
  // interface signatures unless the impl is declared with overloads).
  function validatedSignalImpl<T>(sig: SignalType<T>): Generator<ActivityYield, T, T>;
  function validatedSignalImpl<T>(
    sig: SignalType<T>,
    options: { readonly timeout: number | Date },
  ): Generator<ActivityYield, TimedSignalOutcome<T>, TimedSignalOutcome<T>>;
  function validatedSignalImpl<T>(
    sig: SignalType<T>,
    options?: { readonly timeout: number | Date },
  ): Generator<ActivityYield, T | TimedSignalOutcome<T>, unknown> {
    return signalImpl<T>(sig.name, {
      ...(options?.timeout !== undefined && { timeout: options.timeout }),
      jsonSchema: sig.schema.jsonSchema,
    });
  }

  // -------------------------------------------------------------------------
  // ctx.approval — preset sugar over validatedSignal
  //
  // Wire-format signal name is `approve:<id>` (matches the convention
  // agentLoop, SignalScanner, and the dashboard /signals Approve/Reject
  // shortcut already use). Returns the canonical ApprovalDecision.
  // -------------------------------------------------------------------------

  function approvalImpl(id: string): Generator<ActivityYield, ApprovalDecision, ApprovalDecision>;
  function approvalImpl(
    id: string,
    options: { readonly timeout: number | Date },
  ): Generator<
    ActivityYield,
    TimedSignalOutcome<ApprovalDecision>,
    TimedSignalOutcome<ApprovalDecision>
  >;
  function approvalImpl(
    id: string,
    options?: { readonly timeout: number | Date },
  ): Generator<ActivityYield, ApprovalDecision | TimedSignalOutcome<ApprovalDecision>, unknown> {
    return options !== undefined
      ? validatedSignalImpl(approvalSignal(id), options)
      : validatedSignalImpl(approvalSignal(id));
  }

  // -------------------------------------------------------------------------
  // ctx.parallel — concurrent sub-generators, one scope each
  // -------------------------------------------------------------------------

  function* parallel<T>(
    branches: ReadonlyArray<Generator<ActivityYield, T, T>>,
  ): Generator<ActivityYield, T[], unknown> {
    // Compute parallel's own position the same way ctx.activity does: if
    // we're already inside a parallel branch, bump that scope's counter;
    // otherwise take a slot from the step-level counter.
    const { activityIndex: parallelActivityIndex, branchPath: parallelPath } = allocateSlot({
      suspendOrChild: false,
    });
    // A journal with only top-level entries settles its format at the first
    // top-level parallel; nested parallels inherit it from their scope.
    const format = (formatRef.current ??= resolveUndecidedJournalFormat({
      journal,
      parallelIndex: parallelActivityIndex,
    }));

    // Drive each branch sub-generator in its own ActivityScope so its
    // yields consume slots from a branch-local counter with a branch-
    // specific path prefix.
    const promise = Promise.all(
      branches.map((branchGen, i) => {
        const branchScope: ActivityScope = {
          parallelActivityIndex,
          pathPrefix: branchPrefix({ format, parallelPath, branch: i }),
          localCounter: { next: 0 },
          format,
        };
        return activityScope.run(branchScope, () => driveSubGenerator(branchGen));
      }),
    );

    return (yield { _tag: "Activity", name: "parallel", promise }) as unknown as T[];
  }

  // -------------------------------------------------------------------------
  // ctx.child — inline child workflow execution
  // -------------------------------------------------------------------------

  /**
   * The child workflow suspended. Suspend this step too, waking when the
   * child does: the child's earliest sleep wake time or signal deadline.
   * The sleep scanner then re-drives the parent, whose `ctx.child` resumes
   * the child. A child waiting on a signal without a deadline gives the
   * parent no wake time; the parent stays suspended until it is resumed.
   */
  async function suspendForChild(params: {
    childWorkflowId: string;
    childError: unknown;
  }): Promise<WorkflowSuspendedError> {
    const { childWorkflowId, childError } = params;
    const reason = (childError as { reason?: unknown }).reason === "sleep" ? "sleep" : "signal";
    let wakeAt: Date | undefined;
    if (workflowStorage) {
      const child = await workflowStorage.loadWorkflow(childWorkflowId);
      for (const step of Object.values(child?.steps ?? {})) {
        const at =
          step.status === "sleeping"
            ? step.wakeAt
            : step.status === "waiting_for_signal"
              ? step.signalTimeoutAt
              : undefined;
        if (at && (!wakeAt || at.getTime() < wakeAt.getTime())) wakeAt = at;
      }
      await workflowStorage.suspendWorkflow(
        workflowId,
        stepName,
        { status: "sleeping", ...(wakeAt && { wakeAt }) },
        guard,
      );
    }
    return new WorkflowSuspendedError({
      workflowId,
      stepName,
      reason,
      message:
        `waiting for child workflow "${childWorkflowId}"` +
        (wakeAt ? ` (wakes at ${wakeAt.toISOString()})` : ""),
    });
  }

  function* childImpl<Output>(
    workflow: Workflow<unknown, Output>,
    options?: { readonly input?: unknown; readonly workflowId?: string },
  ): Generator<ActivityYield, Output, Output> {
    const { activityIndex, branchPath } = allocateSlot({ suspendOrChild: true });
    // Inside a parallel branch the default id also carries the branch path
    // ("/" mapped to "~" so the id stays URL-safe); top-level ids keep the
    // `${workflowId}.${stepName}.${activityIndex}` shape.
    const childWorkflowId =
      options?.workflowId ??
      `${workflowId}.${stepName}.${activityIndex}${branchPath.replaceAll("/", "~")}`;
    const childInput = options?.input;
    const activityName = workflow.name;

    /** Value of a completed exit: a failure rethrows, a success decodes. */
    const settle = (exit: JournalExit): Output => {
      if (exit.tag === "Failure") throw rehydrateFailure(exit);
      return stepCodec.decode(exit.value) as Output;
    };

    const promise = (async (): Promise<Output> => {
      const recorded = journalByKey.get(journalKey(activityIndex, branchPath));

      if (recorded) {
        const recordedType = recorded.stepType ?? "activity";
        if (recordedType !== "child") {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `${recordedType}:${recorded.activityName}`,
            `child:${activityName}`,
          );
        }
        if (recorded.activityName !== activityName) {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `child:${recorded.activityName}`,
            `child:${activityName}`,
          );
        }
        // Replay — return cached result without re-running the child.
        const phase = recorded.phase ?? "completed";
        if (phase === "completed") {
          if (!recorded.exit) {
            throw new Error(
              `journal entry ${activityIndex} for step "${stepName}" (child: ${activityName}) is completed but has no exit`,
            );
          }
          return settle(recorded.exit);
        }
        // Pending row — previous worker started the child but didn't record
        // the result. Re-running is safe: the child workflow has its own
        // storage row and idempotency, so calling runChild again just resumes
        // it from where it left off.
      }

      if (!runChild) {
        throw new Error(
          `ctx.child("${activityName}"): requires a \`runChild\` callback to be provided ` +
            `to runJournaledStep. When using WorkflowRunner, this is wired automatically. ` +
            `If you are calling runJournaledStep directly from tests, pass a stub runChild.`,
        );
      }

      const twoPhase = isJournaledSuspendStorage(storage);
      if (twoPhase) {
        await storage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          branchPath,
          activityName,
          stepType: "child",
        });
      }

      let result: Output;
      try {
        result = (await journaledBodyScope.exit(async () =>
          runChild({
            workflow: workflow as Workflow<unknown, unknown>,
            workflowId: childWorkflowId,
            input: childInput,
          }),
        )) as Output;
      } catch (err) {
        const tag = errorTag(err);
        if (tag === "WorkflowSuspendedError") {
          // The child is parked on a sleep or signal: not an outcome. The
          // entry stays pending, so the parent's next run calls runChild
          // again, which resumes the child.
          throw await suspendForChild({ childWorkflowId, childError: err });
        }
        // Another worker holds the child: nothing happened that the journal
        // should remember.
        if (tag === "WorkflowLockError" || tag === "FenceTokenMismatchError") throw err;
        const exit = failureExit(err);
        if (twoPhase) {
          const stored = await completeSlot({
            slot: { activityIndex, branchPath },
            exit,
            readBack: false,
          });
          // Another writer completed the slot first: follow the journal.
          if (!stored.won) return settle(stored.exit);
        } else {
          await storage.appendEntry({
            workflowId,
            stepName,
            activityIndex,
            branchPath,
            activityName,
            exit,
          });
        }
        throw err;
      }

      const encoded = stepCodec.encode(result);
      const successExit = { tag: "Success", value: encoded } as const;
      if (twoPhase) {
        const stored = await completeSlot({
          slot: { activityIndex, branchPath },
          exit: successExit,
          readBack: false,
        });
        return settle(stored.exit);
      }
      await storage.appendEntry({
        workflowId,
        stepName,
        activityIndex,
        branchPath,
        activityName,
        exit: successExit,
      });
      return settle(successExit);
    })();

    return yield { _tag: "Activity", name: activityName, promise };
  }

  function patched(name: string): boolean {
    // Pure set membership. Returns false (not throws) for names not in the
    // currently-running definition's patches array — this is load-bearing
    // for the "same code file, different versions" pattern:
    //
    //   if (ctx.patched("new-pricing")) {
    //     // v2 code path (patches = ["new-pricing"])
    //   } else {
    //     // v1 code path (patches = [])
    //   }
    //
    // A throw-on-unknown design would break v1's false branch. Typo catching
    // is a linter concern, not a runtime one.
    return patchSet.has(name);
  }

  // ---------------------------------------------------------------------------
  // unwind — runs compensations in reverse after a body failure
  // ---------------------------------------------------------------------------

  /**
   * Run every registered compensation in reverse. Each runs via the
   * two-phase journal machinery with stepType="compensation" so a crash
   * during unwind replays cleanly — completed compensations are skipped.
   *
   * Compensation failures do NOT halt the unwind — the engine records the
   * failure in the journal and continues with the remaining compensations.
   * The caller (runJournaledStep) rethrows the original body error after.
   *
   * Returns the slots of every rolled-back activity together with its
   * completed compensation, so the caller can discard them and the next
   * step attempt re-executes the activity.
   */
  async function unwind(_bodyError: unknown): Promise<JournalSlot[]> {
    const twoPhase = isJournaledSuspendStorage(storage);
    const rolledBack: JournalSlot[] = [];
    const markRolledBack = (comp: Compensation): void => {
      rolledBack.push(
        { activityIndex: comp.sourceActivityIndex, branchPath: "" },
        { activityIndex: comp.activityIndex, branchPath: "" },
      );
    };
    for (let i = compensations.length - 1; i >= 0; i--) {
      const comp = compensations[i]!;
      const compIdx = comp.activityIndex;
      const compName = `compensation:${comp.activityName}`;

      // Replay: if a completed row already exists for this compensation
      // index, the previous worker finished it — skip.
      const recorded = journalByKey.get(journalKey(compIdx, ""));
      if (recorded && (recorded.phase ?? "completed") === "completed") {
        if (recorded.exit?.tag === "Success") markRolledBack(comp);
        continue;
      }

      if (twoPhase) {
        try {
          await storage.appendPendingEntry({
            workflowId,
            stepName,
            activityIndex: compIdx,
            activityName: compName,
            stepType: "compensation",
          });
        } catch {
          // Journal unreachable — nothing to do.
          continue;
        }
        let exit: JournalExit;
        try {
          await comp.run();
          exit = { tag: "Success", value: null };
        } catch (err) {
          exit = failureExit(err);
        }
        try {
          await storage.completePendingEntry({
            workflowId,
            stepName,
            activityIndex: compIdx,
            exit,
          });
        } catch {
          /* journal unreachable — give up on this one, continue unwind */
          continue;
        }
        if (exit.tag === "Success") markRolledBack(comp);
      } else {
        // Legacy single-phase storage — best-effort record. The side effect
        // risk of a crash here is the same as a non-two-phase activity,
        // documented at the journal-suspend layer.
        try {
          await comp.run();
          await storage.appendEntry({
            workflowId,
            stepName,
            activityIndex: compIdx,
            activityName: compName,
            exit: { tag: "Success", value: null },
          });
        } catch (err) {
          await storage
            .appendEntry({
              workflowId,
              stepName,
              activityIndex: compIdx,
              activityName: compName,
              exit: failureExit(err),
            })
            .catch(() => undefined);
        }
      }
    }
    return rolledBack;
  }

  // Journaled-activity loop. Each iteration yields through `ctx.activity`
  // so it lands in the journal as its own entry; the generator delegates
  // each yield to the outer driver, preserving the single-shot per-yield
  // contract the engine expects.
  function* dowhileImpl<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown> {
    const max = options?.maxIterations ?? 100;
    if (max < 1) {
      throw new LoopLimitExceededError({
        workflowId,
        stepName,
        maxIterations: max,
        message: `ctx.dowhile("${name}"): maxIterations must be >= 1`,
      });
    }
    let result: T = undefined as unknown as T;
    let iter = 0;
    while (true) {
      if (iter >= max) {
        throw new LoopLimitExceededError({
          workflowId,
          stepName,
          maxIterations: max,
          message: `ctx.dowhile("${name}") exceeded ${max} iterations without converging`,
        });
      }
      const currentIter = iter;
      const iterGen = activity(`${name}-iter-${currentIter}`, () => fn(currentIter)) as Generator<
        ActivityYield,
        T,
        unknown
      >;
      result = yield* iterGen;
      iter++;
      if (!condition(result, currentIter)) break;
    }
    return result;
  }

  // `dountil(cond) ≡ dowhile(!cond)` — no need for a parallel loop body.
  function dountilImpl<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown> {
    return dowhileImpl(name, fn, (r, i) => !condition(r, i), options);
  }

  function proxyImpl<Acts extends Record<string, (...args: any[]) => any>>(
    activities: Acts,
    options?: {
      readonly defaultOptions?: ActivityOptions<unknown>;
      readonly optionsByName?: { readonly [K in keyof Acts]?: ActivityOptions<unknown> };
    },
  ): {
    readonly [K in keyof Acts]: (
      ...args: Parameters<Acts[K]>
    ) => Generator<ActivityYield, Awaited<ReturnType<Acts[K]>>, unknown>;
  } {
    const out: Record<string, (...args: unknown[]) => Generator<ActivityYield, unknown, unknown>> =
      {};
    for (const key of Object.keys(activities)) {
      const fn = activities[key as keyof Acts];
      const perKey = options?.optionsByName?.[key as keyof Acts];
      const merged = perKey ?? options?.defaultOptions;
      out[key] = (...args: unknown[]) =>
        // Proxy methods are static activities — closure over `args` keeps
        // input capture local to this call, matching the 2-arg
        // `ctx.activity(name, fn)` form. Names are taken from the property
        // key (not from `fn.name`, which is mangled by bundlers).
        activity(key, () => fn(...args), merged as ActivityOptions<unknown> | undefined);
    }
    return out as {
      readonly [K in keyof Acts]: (
        ...args: Parameters<Acts[K]>
      ) => Generator<ActivityYield, Awaited<ReturnType<Acts[K]>>, unknown>;
    };
  }

  // ---------------------------------------------------------------------------
  // ctx.metadata — live-writable workflow metadata, surfaced to the dashboard
  // ---------------------------------------------------------------------------
  //
  // In-process snapshot layered with side-effect storage writes. Reads return
  // a copy so callers can't mutate the canonical object. Writes update the
  // local snapshot synchronously and dispatch an async storage merge —
  // fire-and-forget, errors logged. Replay re-fires the same writes (same
  // values), idempotent against the storage's merge semantics.
  const metadataState: Record<string, unknown> = { ...(initialMetadata ?? {}) };
  const writeMetadataPatch = (patch: Record<string, unknown>): void => {
    if (!workflowStorage) return; // tests that drive runJournaledStep without WorkflowStorage skip persistence
    workflowStorage.setWorkflowMetadata(workflowId, patch).catch((err) => {
      console.warn(
        `[ctx.metadata] failed to persist for workflow ${workflowId}:`,
        err instanceof Error ? err.message : String(err),
      );
    });
  };
  const metadata = {
    set(key: string, value: unknown): void {
      if (value === null) delete metadataState[key];
      else metadataState[key] = value;
      writeMetadataPatch({ [key]: value });
    },
    merge(patch: Record<string, unknown>): void {
      for (const [k, v] of Object.entries(patch)) {
        if (v === null) delete metadataState[k];
        else metadataState[k] = v;
      }
      writeMetadataPatch(patch);
    },
    get(): Record<string, unknown> {
      return { ...metadataState };
    },
  };

  const ctx: JournaledContext<Input, Prev> = {
    input,
    prev,
    workflowId,
    isReplay: journal.length > 0,
    workflowVersion,
    activity,
    sleep,
    signal: signalImpl,
    validatedSignal: validatedSignalImpl,
    approval: approvalImpl,
    patched,
    parallel,
    child: childImpl,
    dowhile: dowhileImpl,
    dountil: dountilImpl,
    metadata,
    proxy: proxyImpl,
    setQueryHandler: <R>(name: string, handler: (args?: unknown) => R | Promise<R>): void => {
      // Process-local registry — query handlers close over the body's
      // in-memory state, so they're inherently per-process. The worker
      // control socket reads from the same registry to answer queries
      // routed in from the server.
      registerQueryHandler(workflowId, name, handler as (args?: unknown) => unknown);
    },
    continueAsNew: (nextInput: unknown): never => {
      throw new WorkflowContinueAsNewError({
        workflowId,
        nextInput,
        message: `Workflow "${workflowId}" requested continue-as-new`,
      });
    },
  };
  return { ctx, unwind };
}

/**
 * Drive a branch sub-generator to completion, awaiting each yielded
 * ActivityYield. Mirrors the outer runner but stays inside whatever
 * `activityScope` the caller has set, so activities inside the branch see
 * the branch-local scope.
 */
async function driveSubGenerator<T>(gen: Generator<ActivityYield, T, T>): Promise<T> {
  let step = gen.next();
  while (!step.done) {
    const yielded = step.value;
    try {
      const resolved = await yielded.promise;
      step = gen.next(resolved as never);
    } catch (err) {
      step = gen.throw(err);
    }
  }
  return step.value;
}

// ---------------------------------------------------------------------------
// completeSignal — external API used to deliver a value to a suspended step
// ---------------------------------------------------------------------------

/**
 * Deliver a signal value to a workflow awaiting it via `ctx.signal(name)`.
 * Finds the matching pending journal entry and completes it with the given
 * value. Subsequent replay of the journaled step unblocks at the signal and
 * continues.
 *
 * The caller is responsible for re-enqueuing the workflow for execution after
 * delivery (via PgStepQueue, in-memory scheduler, or direct re-run). Today
 * callers drive resume themselves; an automatic resume path through the
 * step queue is planned as a refinement.
 *
 * Returns `true` if this call completed the pending entry; `false` if no
 * matching pending signal exists (already delivered, never registered) or
 * the entry was completed concurrently — by another delivery, or by the
 * body recording the signal's timeout. On `false` the value was not
 * delivered and the workflow will not see it.
 */
export async function completeSignal(params: {
  storage: JournaledSuspendStorage;
  workflowId: string;
  stepName: string;
  signalName: string;
  value: unknown;
}): Promise<boolean> {
  const hit = await params.storage.findPendingSignal({
    workflowId: params.workflowId,
    stepName: params.stepName,
    signalName: params.signalName,
  });
  if (!hit) return false;

  const result: CompletePendingResult | undefined = await params.storage.completePendingEntry({
    workflowId: params.workflowId,
    stepName: params.stepName,
    activityIndex: hit.activityIndex,
    branchPath: hit.branchPath,
    exit: { tag: "Success", value: deliveredSignalExitValue(params.value) },
  });
  // A storage written before `CompletePendingResult` reports nothing; keep
  // its old answer.
  return result?.completed ?? true;
}

/**
 * Complete a pending journal entry and return the exit the journal now
 * holds. `won` is false when another writer completed the entry first; the
 * returned exit is then that writer's. Storages that predate
 * `CompletePendingResult` return nothing: with `readBack` the entry is read
 * back to learn the stored exit, otherwise the caller's exit is assumed.
 */
async function completeAndReport(params: {
  storage: JournaledSuspendStorage;
  workflowId: string;
  stepName: string;
  slot: JournalSlot;
  exit: JournalExit;
  readBack: boolean;
}): Promise<{ won: boolean; exit: JournalExit }> {
  const { storage, workflowId, stepName, slot, exit, readBack } = params;
  const result: CompletePendingResult | undefined = await storage.completePendingEntry({
    workflowId,
    stepName,
    activityIndex: slot.activityIndex,
    branchPath: slot.branchPath,
    exit,
  });
  if (result) {
    // No stored exit means the entry is gone (purged under us): nothing to
    // follow, keep the local outcome.
    if (result.completed || result.exit === undefined) return { won: true, exit };
    return { won: false, exit: result.exit };
  }
  if (!readBack) return { won: true, exit };
  const stored = (await storage.loadJournal(workflowId, stepName)).find(
    (e) => e.activityIndex === slot.activityIndex && e.branchPath === slot.branchPath,
  );
  if (stored?.exit && (stored.phase ?? "completed") === "completed") {
    return { won: false, exit: stored.exit };
  }
  return { won: true, exit };
}

/**
 * Scanner hook — complete all due sleeps up to `limit`. Returns the
 * completed entries so a caller (or test) can re-enqueue the workflows.
 *
 * Usage:
 * ```ts
 * const due = await completeDueSleeps({ storage, now, limit: 100 });
 * for (const { workflowId } of due) {
 *   await workflow.run({ workflowId }); // re-drive to consume completion
 * }
 * ```
 */
export async function completeDueSleeps(params: {
  storage: JournaledSuspendStorage;
  now: Date;
  limit: number;
}): Promise<
  Array<{
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath: string;
    wakeAt: Date;
  }>
> {
  const due = await params.storage.findDueSleeps({
    now: params.now,
    limit: params.limit,
  });
  for (const entry of due) {
    await params.storage.completePendingEntry({
      workflowId: entry.workflowId,
      stepName: entry.stepName,
      activityIndex: entry.activityIndex,
      branchPath: entry.branchPath,
      // Store as ISO string for consistent JSON roundtrip; the generator
      // hydrates back to Date on replay.
      exit: { tag: "Success", value: entry.wakeAt.toISOString() },
    });
  }
  return due;
}

// ---------------------------------------------------------------------------
// Runner — drive the generator, await each yielded promise, relay the value
// ---------------------------------------------------------------------------

/**
 * Drive a journaled step body to completion. Loads the existing journal,
 * runs the generator from the top, awaits each yielded activity promise
 * (which handles replay-or-execute internally), and resumes the generator
 * with the resolved value.
 *
 * Safe to call multiple times for the same (workflowId, stepName) — each
 * call re-runs deterministically against the current journal state.
 */
export async function runJournaledStep<Input, Prev, Output>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  storage: ActivityJournalStorage;
  /**
   * Full WorkflowStorage — when provided, ctx.sleep/ctx.signal call
   * suspendWorkflow() so the existing DefaultSleepScanner resumes them.
   * Omit only when driving runJournaledStep directly from tests.
   */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — surfaced on ctx.workflowVersion. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
  /**
   * Default codec for activities inside this step. Each activity can still
   * override via its own options. Defaults to LosslessJsonCodec so fresh-run
   * values round-trip to the same shape replay would produce.
   */
  codec?: Codec<unknown>;
  /**
   * Workflow-level default for `ActivityOptions.payloadHash`. When `true`,
   * every 3-arg `ctx.activity(name, input, fn)` in this step's body
   * fingerprints its input by default; per-activity `payloadHash: false`
   * still opts out. The 2-arg form is unaffected (no reified input to hash).
   */
  payloadHash?: boolean;
  /**
   * Executes a child workflow inline. Wired automatically when called through
   * `WorkflowRunner` / the `.journaled()` builder; pass a stub in unit tests
   * that call `runJournaledStep` directly and want to exercise `ctx.child`.
   */
  runChild?: (params: {
    workflow: Workflow<unknown, unknown>;
    workflowId: string;
    input: unknown;
  }) => Promise<unknown>;
  /**
   * Time source for `ctx.sleep` / `ctx.signal` deadlines and activity retry
   * backoff. Wired from the runner's clock; default `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Fence guard of the runner's lock on this run, passed on the
   * ctx.sleep / ctx.signal suspension writes.
   */
  guard?: FenceGuard;
  body: JournaledStepBody<Input, Prev, Output>;
}): Promise<Output> {
  const {
    input,
    prev,
    workflowId,
    stepName,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
    codec,
    payloadHash,
    runChild,
    clock,
    guard,
    body,
  } = params;

  const journal = await storage.loadJournal(workflowId, stepName);
  // Load workflow metadata snapshot for `ctx.metadata.get()` — reads are
  // synchronous from the body, so we materialize the snapshot up front.
  // Writes go through `setWorkflowMetadata` independently.
  const wfState = workflowStorage ? await workflowStorage.loadWorkflow(workflowId) : null;
  const { ctx, unwind } = makeCtx({
    input,
    prev,
    workflowId,
    stepName,
    journal,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
    defaultCodec: codec,
    defaultPayloadHash: payloadHash,
    runChild,
    ...(clock !== undefined && { clock }),
    ...(guard !== undefined && { guard }),
    ...(wfState?.metadata !== undefined && { initialMetadata: wfState.metadata }),
  });
  const gen = body(ctx, prev);

  // Code between `yield*` expressions inside the generator runs when we call
  // gen.next / gen.throw. Running each tick inside `journaledBodyScope` lets
  // dev tooling (e.g. instrumentNonDeterminism) detect direct Date.now /
  // Math.random calls from the body. Activity fn bodies run outside this
  // scope — they're supposed to touch the outside world.
  const bodyCtx = { stepName };
  const tick = <R>(fn: () => R): R => journaledBodyScope.run(bodyCtx, fn);

  /** Drive the generator until it returns, propagating or catching errors. */
  const driveBody = async (): Promise<Output> => {
    let step: IteratorResult<ActivityYield, Output>;
    step = tick(() => gen.next());
    while (!step.done) {
      const yielded = step.value;
      try {
        const resolved = await yielded.promise;
        step = tick(() => gen.next(resolved as never));
      } catch (err) {
        // Let the body's try/catch handle it if it wants; otherwise re-throw.
        step = tick(() => gen.throw(err));
      }
    }
    return step.value;
  };

  try {
    // Drive outside any enclosing parallel-branch scope: a child workflow
    // started from a branch (ctx.child → runChild → runJournaledStep) must
    // allocate its slots from its own top-level counter, not the parent's
    // branch.
    return await activityScope.exit(() => driveBody());
  } catch (bodyError) {
    if (runsCompensations(bodyError)) {
      const rolledBack = await unwind(bodyError);
      await discardFailedAttempt({ storage, workflowId, stepName, rolledBack });
    }
    throw bodyError;
  }
}

/**
 * A failure escaped the body, so this step attempt is over. Drop what the
 * next attempt must re-execute rather than replay: recorded failures of
 * activities, children and compensations, and every rolled-back activity
 * with its compensation row. Best effort: if the journal can't be read or
 * written, the next attempt replays the failure and discards it then.
 */
async function discardFailedAttempt(params: {
  storage: ActivityJournalStorage;
  workflowId: string;
  stepName: string;
  rolledBack: readonly JournalSlot[];
}): Promise<void> {
  const { storage, workflowId, stepName, rolledBack } = params;
  const discard = (storage as Partial<JournaledSuspendStorage>).discardJournalEntries;
  if (typeof discard !== "function") return;
  try {
    const journal = await storage.loadJournal(workflowId, stepName);
    const slots = new Map<string, JournalSlot>();
    const add = (slot: JournalSlot): void => {
      slots.set(`${slot.activityIndex}:${slot.branchPath}`, slot);
    };
    for (const entry of journal) {
      const type = entry.stepType ?? "activity";
      if (type !== "activity" && type !== "child" && type !== "compensation") continue;
      if ((entry.phase ?? "completed") !== "completed" || entry.exit?.tag !== "Failure") continue;
      add({ activityIndex: entry.activityIndex, branchPath: entry.branchPath });
    }
    for (const slot of rolledBack) add(slot);
    if (slots.size === 0) return;
    await discard.call(storage, { workflowId, stepName, slots: [...slots.values()] });
  } catch {
    // Journal unreachable: the body error is what the caller needs to see.
  }
}

/**
 * Exits that are NOT business failures and so must not trigger the
 * intra-step compensation unwind. Matched by `_tag` so an error thrown from
 * another copy of this module is still recognised.
 *
 *  - `WorkflowSuspendedError`: ctx.sleep / ctx.signal parked the workflow.
 *  - `WorkflowContinueAsNewError`: a clean restart, not a rollback.
 *  - `WorkflowTripwireError`: an intentional early end.
 *  - `JournalNonDeterminismError`: code drifted from the journal; rolling
 *    back completed work would turn a deploy problem into data loss.
 *  - `AmbiguousActivityOutcome`: the workflow halts so an operator can
 *    inspect the external system before anything else runs.
 *  - `WorkflowLockError` / `FenceTokenMismatchError` /
 *    `WorkflowLockLostError`: this worker lost the workflow; the new owner
 *    re-drives it from storage.
 *  - `CheckpointError`: a durable write failed past its retries; recovery
 *    re-drives the workflow.
 */
const NON_COMPENSATING_EXITS: ReadonlySet<string> = new Set([
  "WorkflowSuspendedError",
  "WorkflowContinueAsNewError",
  "WorkflowTripwireError",
  "JournalNonDeterminismError",
  "AmbiguousActivityOutcome",
  "WorkflowLockError",
  "FenceTokenMismatchError",
  "WorkflowLockLostError",
  "CheckpointError",
]);

/** Whether a body error is a genuine failure that unwinds compensations. */
function runsCompensations(bodyError: unknown): boolean {
  const tag = errorTag(bodyError);
  return !(tag !== undefined && NON_COMPENSATING_EXITS.has(tag));
}

/** `_tag` of a thrown value, when it has one. */
function errorTag(err: unknown): string | undefined {
  if (typeof err !== "object" || err === null) return undefined;
  const tag = (err as { readonly _tag?: unknown })._tag;
  return typeof tag === "string" ? tag : undefined;
}

/**
 * Run an activity body under its retry policy on the shared retry loop.
 * Class-based classification wins over the policy's `when`: a
 * `TerminalError` is never retried, a `RetryableError` always is (while
 * retries remain), so a lax predicate can't re-run the first and a strict
 * one can't skip the second.
 */
function runWithRetry<T>(params: {
  fn: () => Promise<T>;
  policy: RetryPolicy<unknown>;
  clock: WallClock;
}): Promise<T> {
  const { policy } = params;
  const userWhen = policy.when;
  return retryAsync({
    policy: {
      ...policy,
      when: (err) => {
        if (err instanceof TerminalError) return false;
        if (err instanceof RetryableError) return true;
        return userWhen === undefined || userWhen(err);
      },
    },
    clock: params.clock,
    run: () => params.fn(),
  });
}
