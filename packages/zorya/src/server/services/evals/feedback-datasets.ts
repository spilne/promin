// ---------------------------------------------------------------------------
// Ids of the per-tenant datasets operator feedback writes. The namespace is
// part of the id so a run can check a stored dataset belongs to its tenant.
// ---------------------------------------------------------------------------

import type { DatasetSpec } from "@promin/evals";

const FEEDBACK_PREFIX = "agent-feedback";
const REGRESSION_PREFIX = "agent-regression";

function datasetId(params: {
  prefix: string;
  namespaceId: string;
  agentId: string;
  resourceId: string;
}): string {
  const parts = [params.namespaceId, params.agentId, params.resourceId].map(encodeURIComponent);
  return [params.prefix, ...parts].join(":");
}

/** A stable dataset per tenant and agent, usable by a stored eval suite. */
export function feedbackDatasetId(
  namespaceId: string,
  agentId: string,
  resourceId: string,
): string {
  return datasetId({ prefix: FEEDBACK_PREFIX, namespaceId, agentId, resourceId });
}

export function regressionDatasetId(
  namespaceId: string,
  agentId: string,
  resourceId: string,
): string {
  return datasetId({ prefix: REGRESSION_PREFIX, namespaceId, agentId, resourceId });
}

/**
 * The namespace a feedback / regression dataset id belongs to, or null for
 * any other id (operator-managed datasets are not tenant-bound).
 */
export function feedbackDatasetNamespace(id: string): string | null {
  const [prefix, namespace] = id.split(":");
  if ((prefix !== FEEDBACK_PREFIX && prefix !== REGRESSION_PREFIX) || namespace === undefined) {
    return null;
  }
  try {
    return decodeURIComponent(namespace);
  } catch {
    return "";
  }
}

/**
 * Whether a suite reads a feedback dataset of a namespace other than the
 * one its run executes in. Such a run would replay another tenant's user
 * content, so it is refused.
 */
export function readsForeignFeedback(params: {
  dataset: DatasetSpec;
  namespaceId: string;
}): boolean {
  if (params.dataset.kind !== "stored") return false;
  const namespace = feedbackDatasetNamespace(params.dataset.datasetId);
  return namespace !== null && namespace !== params.namespaceId;
}
