import { RunSecurityContextConflictError } from "./durable-pipeline-error.ts";

/** Principal kinds shared by durable workflow hosts without importing a host package. */
export type RunPrincipalKind = "operator" | "worker" | "user" | "service-account";

/** Dependency-neutral reference to a security principal. */
export interface RunPrincipalRef {
  readonly kind: RunPrincipalKind;
  readonly id: string;
}

/**
 * Immutable identity attached to a durable run.
 *
 * Only stable references are stored. Roles, tokens, email addresses and
 * other authorization snapshots must be resolved again by the host before
 * execution. `effective` is omitted when the actor runs as itself.
 */
export interface RunSecurityContext {
  readonly actor: RunPrincipalRef;
  readonly effective?: RunPrincipalRef;
}

/** Validate and copy the exact persistable shape, dropping extra runtime fields. */
export function normalizeRunSecurityContext(context: RunSecurityContext): RunSecurityContext {
  // Snapshot every caller-controlled property once. Apart from making the
  // stored shape exact, this prevents getters/proxies from returning one
  // value for validation and another for persistence.
  const actorRef = context.actor;
  const effectiveRef = context.effective;
  const actor = normalizePrincipalRef(actorRef, "actor");
  const requestedEffective =
    effectiveRef === undefined ? undefined : normalizePrincipalRef(effectiveRef, "effective");
  // There is one canonical representation for acting as self. This keeps a
  // caller that spells out `effective: actor` compatible with stored rows
  // that correctly omit the redundant effective reference.
  const effective =
    requestedEffective !== undefined && !samePrincipalRef(actor, requestedEffective)
      ? requestedEffective
      : undefined;
  return Object.freeze({
    actor,
    ...(effective !== undefined && { effective }),
  });
}

/** Decode a stored context. Missing values are legacy rows and remain readable. */
export function decodeRunSecurityContext(value: unknown): RunSecurityContext | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("run security context must be an object");
  }
  return normalizeRunSecurityContext(value as RunSecurityContext);
}

/** Compare contexts by their normalized actor/effective references. */
export function sameRunSecurityContext(
  left: RunSecurityContext | undefined,
  right: RunSecurityContext | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return (
    samePrincipalRef(left.actor, right.actor) &&
    (left.effective === undefined || right.effective === undefined
      ? left.effective === right.effective
      : samePrincipalRef(left.effective, right.effective))
  );
}

/**
 * Reject attaching an explicitly secured create to a row with another
 * identity. An omitted requested context means "use the stored identity",
 * which is required by recovery drivers and legacy callers.
 */
export function assertRunSecurityContextCompatible(params: {
  readonly workflowId: string;
  readonly stored: RunSecurityContext | undefined;
  readonly requested: RunSecurityContext | undefined;
}): void {
  if (params.requested === undefined) return;
  const requested = normalizeRunSecurityContext(params.requested);
  if (sameRunSecurityContext(params.stored, requested)) return;
  throw new RunSecurityContextConflictError({
    workflowId: params.workflowId,
    message: `Workflow "${params.workflowId}" already exists with a different security context`,
  });
}

function normalizePrincipalRef(ref: RunPrincipalRef, label: string): RunPrincipalRef {
  const kind = ref?.kind;
  const id = ref?.id;
  if (
    ref === null ||
    typeof ref !== "object" ||
    typeof kind !== "string" ||
    !isRunPrincipalKind(kind) ||
    typeof id !== "string" ||
    id.trim().length === 0 ||
    INVALID_PRINCIPAL_ID.test(id)
  ) {
    throw new TypeError(
      `run security context ${label} must contain a supported kind and a non-blank id without control or format characters`,
    );
  }
  return Object.freeze({ kind, id });
}

const RUN_PRINCIPAL_KINDS: ReadonlySet<string> = new Set([
  "operator",
  "worker",
  "user",
  "service-account",
]);

const INVALID_PRINCIPAL_ID = /[\p{Cc}\p{Cf}]/u;

function isRunPrincipalKind(value: string): value is RunPrincipalKind {
  return RUN_PRINCIPAL_KINDS.has(value);
}

function samePrincipalRef(left: RunPrincipalRef, right: RunPrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}
