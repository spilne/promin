// ---------------------------------------------------------------------------
// Journaled context types — the public surface a `.journaled()` step body
// sees: `JournaledContext` (the `ctx` argument), the per-activity options,
// the `ActivityYield` the runner drives, and the body signature.
//
// Types only: the implementations live in the `journaled-ctx-*.ts` modules
// and are assembled by `journaled-ctx.ts`, which share `JournaledCtxEnv`.
// ---------------------------------------------------------------------------

import type { RetryPolicy } from "../shared/retry-policy.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import type { CompensationStack } from "./compensation-stack.ts";
import type { Workflow } from "./durable-pipeline.ts";
import type { JournalCursor } from "./journal-cursor.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";
import type { ApprovalDecision, SignalType } from "../signals/define-signal.ts";

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
   * A name no version declares returns `false` (a v1 definition with
   * `patches: []` must answer `false` for every later patch name), so a
   * typo is not caught at runtime.
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
   * child is not re-executed. A child that suspends parks the parent step
   * until the child's own wake time or until the child run ends, whichever
   * comes first; the scanners then resume the parent.
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
   * - The runner catches it, calls `storage.startFreshRun({ workflowId })` to
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

/** Tagged outcome returned by `ctx.signal(name, { timeout })`. */
export type TimedSignalOutcome<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: "timeout" };

/**
 * Executes a child workflow inline for `ctx.child`. The runner supplies it
 * so the journal engine needn't depend on `workflow-runner.ts`.
 */
export type RunChild = (params: {
  workflow: Workflow<unknown, unknown>;
  workflowId: string;
  input: unknown;
}) => Promise<unknown>;

/** What the `ctx` method implementations of one body run share. */
export interface JournaledCtxEnv {
  readonly workflowId: string;
  readonly stepName: string;
  readonly cursor: JournalCursor;
  readonly compensations: CompensationStack;
  /**
   * Full WorkflowStorage: ctx.sleep / ctx.signal / ctx.child mark the
   * workflow suspended through it so the scanners resume it, and
   * ctx.metadata persists through it. Absent only when tests drive
   * `runJournaledStep` without one.
   */
  readonly workflowStorage: WorkflowStorage | undefined;
  /** Fence guard of the run's lock, passed on every suspend and metadata write. */
  readonly guard: FenceGuard | undefined;
  /** Time source for sleep / signal deadlines and activity retry backoff. */
  readonly clock: WallClock;
  /** The step's codec; an activity may override it via `ActivityOptions.codec`. */
  readonly stepCodec: Codec<unknown>;
  /** Workflow-level default for `ActivityOptions.payloadHash`. */
  readonly defaultPayloadHash: boolean | undefined;
  readonly runChild: RunChild | undefined;
}
