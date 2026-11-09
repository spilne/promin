// ---------------------------------------------------------------------------
// Journaled updates — the run-side half of workflow updates: the handlers a
// journaled body registers (`ctx.setUpdateHandler`) and the update points
// where it applies the requests callers have delivered.
//
// Ordering semantics
// ------------------
// - Update points. A body applies pending updates only between its own
//   top-level yields: right after each `yield*` settles (a value, or the
//   suspension of a sleep / signal wait), before the generator resumes. Never
//   inside a `ctx.parallel` branch, never while an activity is executing,
//   never two handlers at once. A handler therefore sees the body's state at
//   a yield boundary and the body sees the handler's writes at the next one.
// - Order. Pending requests are applied in delivery order (`deliveredAt`,
//   then update id), at most `MAX_UPDATES_PER_POINT` per point; the rest wait
//   for the next point (or the next resume of a suspended run).
// - Journal. Each applied update is one journal entry in the slot after the
//   yield that preceded it (an `activity` entry named `workflow.update:<id>`;
//   no new entry kind, so no backend schema change). A replay finds recorded
//   updates at the same point and re-applies them in the same order to
//   rebuild the body's state; only past the recorded journal does it look at
//   new requests. Handlers must be deterministic functions of their args and
//   the body's state, and must not call `ctx`.
// - Rejections do not run the handler and are not journaled: an update for a
//   handler the run does not have, or one whose args fail the schema or the
//   validator, only gets a `rejected` result.
// - A handler that throws is recorded as `failed`; replay runs it again for
//   its effect on the state and keeps the recorded outcome.
// - Crash windows. The handler is journaled pending before it runs and
//   completed after: a crash in between re-runs it on resume (at-least-once,
//   like an activity). The result signal is written after the completion
//   and re-written by the replay if missing.
// - Serving. Handlers are served while the registering journaled step runs
//   or is suspended. When that step finishes, requests still pending are
//   rejected (`not_applied`) and the published handler list is emptied.
// ---------------------------------------------------------------------------

import type { JournalEntry } from "./activity-journal.ts";
import type { JournaledCtxEnv } from "./journaled-context.ts";
import { activityScope } from "./journaled-body-scope.ts";
import type { SignalState } from "./workflow-state.ts";
import { validate } from "../schema/validator.ts";
import {
  UPDATE_HANDLERS_SIGNAL,
  UPDATE_REQUEST_SIGNAL_PREFIX,
  parseUpdateOutcome,
  parseUpdateRequest,
  updateIdOfRequestSignal,
  updateRequestSignalName,
  updateResultSignalName,
  type PublishedUpdateHandlers,
  type UpdateOutcome,
  type UpdateRejectionReason,
  type UpdateRequest,
  type UpdateType,
} from "../updates/update-protocol.ts";

/** Most updates one update point applies; the rest wait for the next one. */
export const MAX_UPDATES_PER_POINT = 100;

/** Name of the journal entry that records the application of update `updateId`. */
export const UPDATE_ENTRY_PREFIX = "workflow.update:";

const isUpdateEntry = (entry: JournalEntry): boolean =>
  (entry.stepType ?? "activity") === "activity" &&
  entry.activityName.startsWith(UPDATE_ENTRY_PREFIX);

/** What a handler registration may add to the handler. */
export interface UpdateHandlerOptions<Args> {
  /**
   * Checked before the handler runs, after the schema. Throw, or return a
   * message, to reject the update: the handler does not run and nothing is
   * journaled.
   */
  readonly validate?: (args: Args) => string | void;
}

/** One live update, reported to `onUpdate` (the tracer). */
export interface UpdateEvent {
  readonly stepName: string;
  readonly updateId: string;
  readonly updateName: string;
  readonly outcome: "completed" | "failed" | "rejected";
  readonly reason?: UpdateRejectionReason;
}

interface RegisteredHandler {
  readonly handler: (args: never) => unknown;
  readonly update: UpdateType<never, unknown> | undefined;
  readonly validate: ((args: never) => string | void) | undefined;
}

/** The update handlers of one body run and the update points that serve them. */
export class UpdateInbox {
  private readonly handlers = new Map<string, RegisteredHandler>();
  private published: string | undefined;

  constructor(
    private readonly env: JournaledCtxEnv,
    private readonly onUpdate: ((event: UpdateEvent) => void) | undefined,
  ) {}

  register(params: {
    readonly name: string;
    readonly update?: UpdateType<never, unknown>;
    readonly handler: (args: never) => unknown;
    readonly options?: UpdateHandlerOptions<never>;
  }): void {
    this.handlers.set(params.name, {
      handler: params.handler,
      update: params.update,
      validate: params.options?.validate,
    });
  }

  /** Whether the body registered any handler. */
  get hasHandlers(): boolean {
    return this.handlers.size > 0;
  }

  /**
   * Publish the handler list when it changed since the last publish, so a
   * caller can reject an unknown update before delivering it.
   */
  async publish(): Promise<void> {
    const storage = this.env.workflowStorage;
    if (!storage || this.handlers.size === 0) return;
    const payload: PublishedUpdateHandlers = {
      updates: [...this.handlers].map(([name, h]) => ({
        name,
        ...(h.update !== undefined && { jsonSchema: h.update.input.jsonSchema }),
      })),
    };
    const key = JSON.stringify(payload);
    if (key === this.published) return;
    await storage.deliverSignal({
      workflowId: this.env.workflowId,
      signalName: UPDATE_HANDLERS_SIGNAL,
      payload,
    });
    this.published = key;
  }

  /**
   * An update point: replay the updates the journal recorded here, then
   * apply the new requests, in order. Top-level only.
   */
  async applyAtUpdatePoint(): Promise<void> {
    if (activityScope.getStore() !== undefined) return;
    const { cursor } = this.env;
    let signals: SignalState[] | undefined;
    const loadSignals = async (): Promise<SignalState[]> =>
      (signals ??= await this.env.workflowStorage!.loadSignals(this.env.workflowId));

    // Recorded here: replay them, in order.
    for (let entry = cursor.peekTopLevel(); entry; entry = cursor.peekTopLevel()) {
      if (!isUpdateEntry(entry)) return;
      await this.replay({ entry, slot: cursor.allocateSlot(), loadSignals });
    }
    // Past the recorded journal: new requests.
    if (!this.env.workflowStorage || this.handlers.size === 0) return;
    let applied = 0;
    while (applied < MAX_UPDATES_PER_POINT) {
      const pending = pendingRequests(
        await this.env.workflowStorage.loadSignals(this.env.workflowId),
      );
      if (pending.length === 0) return;
      const rest = MAX_UPDATES_PER_POINT - applied;
      for (const request of pending.slice(0, rest)) {
        await this.applyNew(request);
        applied += 1;
      }
    }
  }

  /**
   * The registering step finished: answer the requests still pending with a
   * rejection and publish an empty handler list, so callers stop sending.
   */
  async close(): Promise<void> {
    const storage = this.env.workflowStorage;
    if (!storage || (this.handlers.size === 0 && this.published === undefined)) return;
    for (const request of pendingRequests(await storage.loadSignals(this.env.workflowId))) {
      await this.reply({
        request,
        outcome: {
          status: "rejected",
          reason: "not_applied",
          message: `workflow "${this.env.workflowId}" finished step "${this.env.stepName}" before applying the update`,
        },
      });
    }
    await storage.deliverSignal({
      workflowId: this.env.workflowId,
      signalName: UPDATE_HANDLERS_SIGNAL,
      payload: { updates: [] } satisfies PublishedUpdateHandlers,
    });
    this.published = undefined;
  }

  private async reply(params: {
    readonly request: UpdateRequest;
    readonly outcome: UpdateOutcome;
    readonly live?: boolean;
  }): Promise<void> {
    const { request, outcome } = params;
    await this.env.workflowStorage?.deliverSignal({
      workflowId: this.env.workflowId,
      signalName: updateResultSignalName(request.updateId),
      payload: outcome,
    });
    if (params.live !== false) {
      this.emit({ request, outcome });
    }
  }

  private emit(params: { readonly request: UpdateRequest; readonly outcome: UpdateOutcome }): void {
    const { request, outcome } = params;
    this.onUpdate?.({
      stepName: this.env.stepName,
      updateId: request.updateId,
      updateName: request.updateName,
      outcome: outcome.status,
      ...(outcome.status === "rejected" && { reason: outcome.reason }),
    });
  }

  /** Why `request` cannot be applied, or undefined when it can. */
  private rejection(
    request: UpdateRequest,
  ): { reason: UpdateRejectionReason; message: string } | undefined {
    const registered = this.handlers.get(request.updateName);
    if (!registered) {
      return {
        reason: "unknown_update",
        message: `workflow "${this.env.workflowId}" has no update handler "${request.updateName}"`,
      };
    }
    if (registered.update !== undefined) {
      const check = validate(request.args, registered.update.input.jsonSchema);
      if (!check.ok) {
        return {
          reason: "validation",
          message: check.errors
            .map((e) => (e.path ? `${e.path}: ${e.message}` : e.message))
            .join("; "),
        };
      }
    }
    if (registered.validate !== undefined) {
      try {
        const verdict = registered.validate(request.args as never);
        if (typeof verdict === "string") return { reason: "validation", message: verdict };
      } catch (error) {
        return { reason: "validation", message: errorMessage(error) };
      }
    }
    return undefined;
  }

  private async applyNew(request: UpdateRequest): Promise<void> {
    const bad = this.rejection(request);
    if (bad) {
      await this.reply({ request, outcome: { status: "rejected", ...bad } });
      return;
    }
    const { cursor } = this.env;
    const slot = cursor.allocateSlot();
    const name = `${UPDATE_ENTRY_PREFIX}${request.updateId}`;
    await cursor.writePending({ slot, kind: "activity", name });
    const outcome = await this.run(request);
    const stored = await cursor.complete({
      slot,
      exit: {
        tag: "Success",
        value: { updateName: request.updateName, args: request.args, outcome },
      },
    });
    // Another writer completed the slot first: follow the journal.
    const recorded = stored.won ? outcome : (outcomeOfExit(stored.exit) ?? outcome);
    await this.reply({ request, outcome: recorded });
  }

  private async replay(params: {
    readonly entry: JournalEntry;
    readonly slot: { readonly activityIndex: number; readonly branchPath: string };
    readonly loadSignals: () => Promise<SignalState[]>;
  }): Promise<void> {
    const { entry, slot } = params;
    const updateId = entry.activityName.slice(UPDATE_ENTRY_PREFIX.length);
    const exit = entry.phase === "pending" ? undefined : entry.exit;
    if (exit?.tag === "Success") {
      const recorded = exit.value as { updateName?: string; args?: unknown; outcome?: unknown };
      const outcome = parseUpdateOutcome(recorded.outcome);
      const request: UpdateRequest = {
        updateId,
        updateName: recorded.updateName ?? "",
        args: recorded.args,
      };
      // Rebuild the body's state; the recorded outcome stands.
      await this.run(request);
      if (outcome !== undefined && this.env.workflowStorage) {
        const signals = await params.loadSignals();
        if (!signals.some((x) => x.signalName === updateResultSignalName(updateId))) {
          await this.reply({ request, outcome, live: false });
        }
      }
      return;
    }
    // Pending: the run stopped between journaling and completing. The request
    // is still stored; run the handler again and complete the entry.
    const signals = await params.loadSignals();
    const stored = signals.find((x) => x.signalName === updateRequestSignalName(updateId));
    const request = parseUpdateRequest(stored?.payload);
    if (!request) return;
    const outcome = await this.run(request);
    const done = await this.env.cursor.complete({
      slot,
      exit: {
        tag: "Success",
        value: { updateName: request.updateName, args: request.args, outcome },
      },
    });
    await this.reply({
      request,
      outcome: done.won ? outcome : (outcomeOfExit(done.exit) ?? outcome),
    });
  }

  /** Run the handler of `request`; its result, or its failure, as an outcome. */
  private async run(request: UpdateRequest): Promise<UpdateOutcome> {
    const registered = this.handlers.get(request.updateName);
    if (!registered) {
      return {
        status: "failed",
        error: { message: `no update handler "${request.updateName}" is registered` },
      };
    }
    try {
      const result = await registered.handler(request.args as never);
      return { status: "completed", result: result === undefined ? null : toJson(result) };
    } catch (error) {
      const tag = (error as { _tag?: unknown } | null)?._tag;
      return {
        status: "failed",
        error: { ...(typeof tag === "string" && { tag }), message: errorMessage(error) },
      };
    }
  }
}

/** Requests with no result yet, oldest delivery first. */
function pendingRequests(signals: readonly SignalState[]): UpdateRequest[] {
  const answered = new Set(
    signals
      .filter((x) => x.signalName.startsWith("workflow.update-result:"))
      .map((x) => x.signalName),
  );
  return signals
    .filter(
      (x) =>
        x.signalName.startsWith(UPDATE_REQUEST_SIGNAL_PREFIX) &&
        !answered.has(updateResultSignalName(updateIdOfRequestSignal(x.signalName) ?? "")),
    )
    .sort(
      (a, b) =>
        a.deliveredAt.getTime() - b.deliveredAt.getTime() ||
        (a.signalName < b.signalName ? -1 : a.signalName > b.signalName ? 1 : 0),
    )
    .map(
      (x) =>
        parseUpdateRequest(x.payload) ?? {
          // Not a request the protocol wrote: answer it with a rejection.
          updateId: updateIdOfRequestSignal(x.signalName) ?? x.signalName,
          updateName: "",
          args: null,
        },
    );
}

function outcomeOfExit(
  exit: { readonly tag: string; readonly value?: unknown } | undefined,
): UpdateOutcome | undefined {
  if (exit?.tag !== "Success") return undefined;
  return parseUpdateOutcome((exit.value as { outcome?: unknown } | null)?.outcome);
}

function toJson(value: unknown): unknown {
  const json = JSON.stringify(value);
  return json === undefined ? null : (JSON.parse(json) as unknown);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
