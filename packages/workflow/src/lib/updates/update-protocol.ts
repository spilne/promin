// ---------------------------------------------------------------------------
// Workflow updates — the wire protocol, definitions and caller-side errors.
//
// An update is a named, typed request an external caller makes of a *running*
// run and answers with a validated response, like a signal that talks back.
// It is built on signals, so every storage backend carries it unchanged:
//
//   request   signal `workflow.update:<updateId>`
//             payload `{ updateId, updateName, args }`
//   result    signal `workflow.update-result:<updateId>`
//             payload `UpdateOutcome`, written by the run that applied it
//   handlers  signal `workflow.update-handlers`
//             payload `{ updates: [{ name, jsonSchema? }] }`, published by the
//             journaled body that registered handlers; callers read it to
//             reject an unknown name or a bad payload before delivering
//
// Signals are values keyed by name, so a repeated request under one update
// id is the same signal: that is the idempotency. The journaled body applies
// pending requests at its update points (see `durable/journaled-updates.ts`)
// and journals each application, so a replay re-applies them in the same
// order.
// ---------------------------------------------------------------------------

import { TaggedError } from "@spilne/perfect-core";
import type { Infer, JsonSchema, Schema } from "../schema/builder.ts";

/** Prefix of the signal carrying an update request: `workflow.update:<updateId>`. */
export const UPDATE_REQUEST_SIGNAL_PREFIX = "workflow.update:";
/** Prefix of the signal carrying an update's outcome: `workflow.update-result:<updateId>`. */
export const UPDATE_RESULT_SIGNAL_PREFIX = "workflow.update-result:";
/** The signal listing the update handlers a run currently serves. */
export const UPDATE_HANDLERS_SIGNAL = "workflow.update-handlers";

/** Name of the request signal of `updateId`. */
export function updateRequestSignalName(updateId: string): string {
  return `${UPDATE_REQUEST_SIGNAL_PREFIX}${updateId}`;
}

/** Name of the result signal of `updateId`. */
export function updateResultSignalName(updateId: string): string {
  return `${UPDATE_RESULT_SIGNAL_PREFIX}${updateId}`;
}

/** The update id of a request signal name; undefined for any other signal. */
export function updateIdOfRequestSignal(signalName: string): string | undefined {
  if (!signalName.startsWith(UPDATE_REQUEST_SIGNAL_PREFIX)) return undefined;
  const id = signalName.slice(UPDATE_REQUEST_SIGNAL_PREFIX.length);
  return id.length > 0 ? id : undefined;
}

/** Payload of a request signal. `args` must be JSON-safe. */
export interface UpdateRequest {
  readonly updateId: string;
  readonly updateName: string;
  readonly args: unknown;
}

/** Why an update was not applied. */
export type UpdateRejectionReason =
  /** The payload failed the schema or the handler's validator. Nothing ran. */
  | "validation"
  /** The run serves no update handler of that name. */
  | "unknown_update"
  /** The run serves no update handlers (its body registered none, or has moved past them). */
  | "no_handlers"
  /** The run had ended, or ended before the update was applied. */
  | "run_terminal"
  /** The run does not exist. */
  | "run_not_found"
  /** The update id is taken by a different request. */
  | "id_conflict"
  /** The run moved on without applying it (its handlers went away, or it restarted). */
  | "not_applied";

/**
 * What the run recorded for an update, as stored in the result signal:
 * the handler's result, the handler's own failure, or a rejection (the
 * handler never ran).
 */
export type UpdateOutcome =
  | { readonly status: "completed"; readonly result: unknown }
  | {
      readonly status: "failed";
      readonly error: { readonly tag?: string; readonly message: string };
    }
  | {
      readonly status: "rejected";
      readonly reason: UpdateRejectionReason;
      readonly message: string;
    };

/** One handler in the `workflow.update-handlers` signal. */
export interface PublishedUpdateHandler {
  readonly name: string;
  /** JSON Schema of the update's `args`, when the handler was defined with one. */
  readonly jsonSchema?: JsonSchema;
}

/** Payload of the `workflow.update-handlers` signal. */
export interface PublishedUpdateHandlers {
  readonly updates: readonly PublishedUpdateHandler[];
}

/**
 * The update was not applied: see `reason`. Thrown to the caller of
 * `handle.update()` — for a rejection the caller detects itself (unknown
 * name, terminal run, bad payload) and for one the run recorded.
 */
export class UpdateRejectedError extends TaggedError("UpdateRejectedError")<{
  readonly workflowId: string;
  readonly updateId?: string;
  readonly updateName?: string;
  readonly reason: UpdateRejectionReason;
  readonly message: string;
}>() {}

/** The handler ran and threw. The run recorded the failure under the update id. */
export class UpdateFailedError extends TaggedError("UpdateFailedError")<{
  readonly workflowId: string;
  readonly updateId: string;
  readonly updateName?: string;
  /** `_tag` of the error the handler threw, when it had one. */
  readonly errorTag?: string;
  readonly message: string;
}>() {}

/** No outcome was recorded within the wait's `timeoutMs`. The update may still be applied later. */
export class UpdateTimeoutError extends TaggedError("UpdateTimeoutError")<{
  readonly workflowId: string;
  readonly updateId: string;
  readonly message: string;
}>() {}

/**
 * A typed update: its name and the schemas of its `args` and result. Define
 * it once and import it where the handler is registered
 * (`ctx.setUpdateHandler`) and where it is sent (`handle.update`).
 *
 * ```ts
 * const SetPriority = defineUpdate({
 *   name: "setPriority",
 *   input: s.object({ priority: s.integer() }),
 *   output: s.object({ previous: s.integer() }),
 * });
 * ```
 */
export interface UpdateType<Args = unknown, Result = unknown> {
  readonly name: string;
  readonly input: Schema<Args>;
  readonly output?: Schema<Result>;
  /** Phantoms — carry the types. Never read at runtime. */
  readonly _args?: Args;
  readonly _result?: Result;
}

/** What the client needs of an update definition: its name and the schema of its args. */
export interface UpdateLike {
  readonly name: string;
  readonly input: { readonly jsonSchema: JsonSchema };
}

/** The args type of an `UpdateType`. */
export type UpdateArgs<U> = U extends UpdateType<infer A, infer _R> ? A : never;
/** The result type of an `UpdateType`. */
export type UpdateResult<U> = U extends UpdateType<infer _A, infer R> ? R : never;
/** The result an update resolves to: typed for a definition, `unknown` for a bare name. */
export type UpdateResultOf<U> = U extends string ? unknown : UpdateResult<U>;
/** The args an update takes: typed for a definition, `unknown` for a bare name. */
export type UpdateArgsOf<U> = U extends string ? unknown : UpdateArgs<U>;

export function defineUpdate<
  I extends Schema<unknown>,
  O extends Schema<unknown> = Schema<unknown>,
>(params: {
  readonly name: string;
  readonly input: I;
  readonly output?: O;
}): UpdateType<Infer<I>, Infer<O>> {
  if (!params.name || typeof params.name !== "string") {
    throw new TypeError("defineUpdate: name must be a non-empty string");
  }
  return {
    name: params.name,
    input: params.input as Schema<Infer<I>>,
    ...(params.output !== undefined && { output: params.output as Schema<Infer<O>> }),
  };
}

/** Narrow a stored payload to an `UpdateRequest`; undefined when it is not one. */
export function parseUpdateRequest(payload: unknown): UpdateRequest | undefined {
  if (typeof payload !== "object" || payload === null) return undefined;
  const p = payload as Record<string, unknown>;
  if (typeof p.updateId !== "string" || typeof p.updateName !== "string") return undefined;
  return { updateId: p.updateId, updateName: p.updateName, args: p.args };
}

/** Narrow a stored payload to an `UpdateOutcome`; undefined when it is not one. */
export function parseUpdateOutcome(payload: unknown): UpdateOutcome | undefined {
  if (typeof payload !== "object" || payload === null) return undefined;
  const p = payload as Record<string, unknown>;
  if (p.status === "completed") return { status: "completed", result: p.result ?? null };
  if (p.status === "failed" && typeof p.error === "object" && p.error !== null) {
    const e = p.error as Record<string, unknown>;
    return {
      status: "failed",
      error: {
        ...(typeof e.tag === "string" && { tag: e.tag }),
        message: typeof e.message === "string" ? e.message : "update handler failed",
      },
    };
  }
  if (p.status === "rejected" && typeof p.reason === "string") {
    return {
      status: "rejected",
      reason: p.reason as UpdateRejectionReason,
      message: typeof p.message === "string" ? p.message : String(p.reason),
    };
  }
  return undefined;
}

/** Narrow a stored payload to the published handler list; empty when it is not one. */
export function parsePublishedHandlers(payload: unknown): PublishedUpdateHandlers {
  if (typeof payload !== "object" || payload === null) return { updates: [] };
  const list = (payload as { updates?: unknown }).updates;
  if (!Array.isArray(list)) return { updates: [] };
  const updates: PublishedUpdateHandler[] = [];
  for (const item of list) {
    if (typeof item !== "object" || item === null) continue;
    const { name, jsonSchema } = item as { name?: unknown; jsonSchema?: unknown };
    if (typeof name !== "string") continue;
    updates.push({
      name,
      ...(jsonSchema !== undefined && { jsonSchema: jsonSchema as JsonSchema }),
    });
  }
  return { updates };
}
