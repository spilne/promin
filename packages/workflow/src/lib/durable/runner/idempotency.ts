// ---------------------------------------------------------------------------
// Idempotency — TTL resolution for the workflow-level result cache.
// ---------------------------------------------------------------------------

import type { IdempotencyConfig } from "../durable-pipeline.ts";

/**
 * Resolve the idempotency TTL for a given terminal status.
 * Returns `undefined` when no idempotency config is active or no TTL is
 * configured for the passed status.
 */
export function getIdempotencyTtl(
  idempotency: IdempotencyConfig | undefined,
  status: string,
): number | undefined {
  if (!idempotency) return undefined;
  const ttl = idempotency.ttl;
  if (typeof ttl === "number") return ttl;
  if (status === "completed") return ttl.success;
  if (status === "failed") return ttl.failure;
  return undefined;
}
