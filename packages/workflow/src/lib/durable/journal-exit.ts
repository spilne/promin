// ---------------------------------------------------------------------------
// Journal exit encoding: how signal outcomes and failures are stored in a
// journal entry's `exit`, and how replay turns them back into values and
// errors.
//
// Signal entries (`stepType: "signal"`) store a tagged success value:
//   delivered → { $signal: "delivered", value }
//   timed out → { $signal: "timeout" }
// Journals written before the tag hold the bare delivered value, or
// `{ ok: false, error: "timeout" }` when the timeout completed the entry.
// Those still decode: the exact legacy timeout shape on a signal waited on
// with a timeout reads as a timeout, anything else as a delivered value.
//
// Failures (`tag: "Failure"`) store the message plus the error's `_tag` (or
// non-default `name`) and its other public fields, so replay rethrows an
// error of the same kind: a `TerminalError` stays a `TerminalError` and the
// retry classification of the live run and the replay agree.
// ---------------------------------------------------------------------------

import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import type { JournalFailureExit } from "./activity-journal.ts";
import {
  AmbiguousActivityOutcome,
  FenceTokenMismatchError,
  GuardError,
  LoopLimitExceededError,
  RetryableError,
  StepError,
  StepTimeoutError,
  StorageError,
  TerminalError,
  WorkflowDeadlineError,
  WorkflowError,
  WorkflowLockError,
  WorkflowTimeoutError,
} from "./durable-pipeline-error.ts";

// ---------------------------------------------------------------------------
// Signal outcomes
// ---------------------------------------------------------------------------

/** Key of the tag on a signal entry's stored value. */
export const SIGNAL_EXIT_KEY = "$signal";

/** Stored value of a signal entry completed by a delivery. */
export function deliveredSignalExitValue(value: unknown): unknown {
  return { [SIGNAL_EXIT_KEY]: "delivered", value };
}

/** Stored value of a signal entry completed by its timeout. */
export function timedOutSignalExitValue(): unknown {
  return { [SIGNAL_EXIT_KEY]: "timeout" };
}

/** Decoded outcome of a completed signal entry. */
export type SignalOutcome =
  | { readonly kind: "delivered"; readonly value: unknown }
  | { readonly kind: "timeout" };

/**
 * Decode a signal entry's stored success value. `hasTimeout` is whether the
 * waiting `ctx.signal` call has a timeout; it only matters for legacy rows,
 * where the timeout was recognised by shape.
 */
export function decodeSignalExitValue(params: {
  stored: unknown;
  hasTimeout: boolean;
}): SignalOutcome {
  const { stored, hasTimeout } = params;
  if (isRecord(stored) && SIGNAL_EXIT_KEY in stored) {
    const tag = stored[SIGNAL_EXIT_KEY];
    if (tag === "timeout") return { kind: "timeout" };
    if (tag === "delivered") return { kind: "delivered", value: stored.value };
  }
  if (hasTimeout && isLegacyTimeoutValue(stored)) return { kind: "timeout" };
  return { kind: "delivered", value: stored };
}

/** The exact `{ ok: false, error: "timeout" }` value legacy timeouts stored. */
function isLegacyTimeoutValue(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const keys = Object.keys(value);
  return keys.length === 2 && value.ok === false && value.error === "timeout";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

type ErrorClass = new (props: Record<string, unknown>) => Error;

/**
 * Engine error classes rebuilt as themselves on replay. Errors with any other
 * `_tag` replay as an `Error` carrying that `_tag`, `name` and fields, so
 * code that branches on `_tag` behaves the same on the live run and replay.
 */
const KNOWN_TAGGED_ERRORS: ReadonlyMap<string, ErrorClass> = new Map(
  (
    [
      TerminalError,
      RetryableError,
      AmbiguousActivityOutcome,
      FenceTokenMismatchError,
      GuardError,
      LoopLimitExceededError,
      StepError,
      StepTimeoutError,
      StorageError,
      WorkflowDeadlineError,
      WorkflowError,
      WorkflowLockError,
      WorkflowTimeoutError,
    ] as unknown as ReadonlyArray<ErrorClass & { readonly _tag: string }>
  ).map((cls) => [cls._tag, cls]),
);

/** Built-in error constructors rebuilt by `name`. */
const BUILTIN_ERRORS: Readonly<Record<string, new (message: string) => Error>> = {
  TypeError,
  RangeError,
  SyntaxError,
  ReferenceError,
  EvalError,
  URIError,
};

const RESERVED_ERROR_KEYS: ReadonlySet<string> = new Set(["_tag", "name", "message", "stack"]);

/** Journal exit for a thrown value. */
export function failureExit(err: unknown): JournalFailureExit {
  if (!(err instanceof Error)) {
    const tag = isRecord(err) && typeof err._tag === "string" ? err._tag : undefined;
    const message = isRecord(err) && typeof err.message === "string" ? err.message : String(err);
    return { tag: "Failure", error: message, ...(tag !== undefined && { errorTag: tag }) };
  }
  const tag = (err as { _tag?: unknown })._tag;
  const errorTag = typeof tag === "string" ? tag : undefined;
  const errorName =
    errorTag === undefined && err.name && err.name !== "Error" ? err.name : undefined;
  const errorData = encodeErrorFields(err);
  return {
    tag: "Failure",
    error: err.message,
    ...(errorTag !== undefined && { errorTag }),
    ...(errorName !== undefined && { errorName }),
    ...(errorData !== undefined && { errorData }),
  };
}

/**
 * Own enumerable fields other than `_tag` / `name` / `message` / `stack`,
 * encoded with `LosslessJsonCodec`. Fields that can't be encoded are dropped.
 */
function encodeErrorFields(err: Error): unknown {
  const fields: Record<string, unknown> = {};
  let any = false;
  for (const key of Object.keys(err)) {
    if (RESERVED_ERROR_KEYS.has(key)) continue;
    fields[key] = (err as unknown as Record<string, unknown>)[key];
    any = true;
  }
  if (!any) return undefined;
  try {
    return LosslessJsonCodec.encode(fields);
  } catch {
    const kept: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(fields)) {
      try {
        LosslessJsonCodec.encode(value);
        kept[key] = value;
      } catch {
        // Not serializable (function, cyclic structure, ...): drop it.
      }
    }
    return Object.keys(kept).length > 0 ? LosslessJsonCodec.encode(kept) : undefined;
  }
}

function decodeErrorFields(exit: JournalFailureExit): Record<string, unknown> {
  if (exit.errorData === undefined) return {};
  try {
    const decoded = LosslessJsonCodec.decode(exit.errorData);
    return isRecord(decoded) ? decoded : {};
  } catch {
    return {};
  }
}

/** Rebuild the error a `Failure` exit recorded. */
export function rehydrateFailure(exit: JournalFailureExit): Error {
  const fields = decodeErrorFields(exit);
  if (exit.errorTag !== undefined) {
    const cls = KNOWN_TAGGED_ERRORS.get(exit.errorTag);
    if (cls) return new cls({ ...fields, message: exit.error });
    const err = new Error(exit.error);
    Object.assign(err, fields);
    err.name = exit.errorTag;
    (err as Error & { _tag: string })._tag = exit.errorTag;
    return err;
  }
  const builtin = exit.errorName !== undefined ? BUILTIN_ERRORS[exit.errorName] : undefined;
  const err = builtin ? new builtin(exit.error) : new Error(exit.error);
  Object.assign(err, fields);
  if (exit.errorName !== undefined && !builtin) err.name = exit.errorName;
  return err;
}
