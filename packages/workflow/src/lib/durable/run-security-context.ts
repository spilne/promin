import { RunSecurityContextConflictError } from "./durable-pipeline-error.ts";

/** Dependency-neutral reference to a security principal. */
export interface RunPrincipalRef {
  readonly kind: string;
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
  const actor = normalizePrincipalRef(context.actor, "actor");
  const effective =
    context.effective === undefined
      ? undefined
      : normalizePrincipalRef(context.effective, "effective");
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
  if (
    ref === null ||
    typeof ref !== "object" ||
    typeof ref.kind !== "string" ||
    ref.kind.length === 0 ||
    typeof ref.id !== "string" ||
    ref.id.length === 0
  ) {
    throw new TypeError(`run security context ${label} must contain non-empty kind and id`);
  }
  return Object.freeze({ kind: ref.kind, id: ref.id });
}

function samePrincipalRef(left: RunPrincipalRef, right: RunPrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}
