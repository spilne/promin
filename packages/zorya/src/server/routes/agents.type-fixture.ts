// ---------------------------------------------------------------------------
// Compile-time fixture for `AgentInvokeBody` — proves the discriminated
// union enforces "namespaceId AND (resourceId XOR ownerId)" at the type
// level, so cross-scope leaks are prevented by the type system at every
// caller of agent invoke. Bun runs this through tsc as part of the
// typecheck target; any line here that compiles when it shouldn't is a
// regression.
// ---------------------------------------------------------------------------

import type { AgentInvokeBody } from "./agents.ts";

// ✓ resourceId set, ownerId absent — accepted.
const _ok1: AgentInvokeBody = {
  task: "summarize",
  namespaceId: "acme",
  resourceId: "doc-42",
};
void _ok1;

// ✓ ownerId set, resourceId absent — accepted.
const _ok2: AgentInvokeBody = {
  task: "summarize",
  namespaceId: "acme",
  ownerId: "u-9",
};
void _ok2;

// ✗ Both resourceId AND ownerId — rejected: the union variants are
// `resourceId?: never` / `ownerId?: never` paired against the other.
// @ts-expect-error — conflicting identity
const _bad1: AgentInvokeBody = {
  task: "summarize",
  namespaceId: "acme",
  resourceId: "doc-42",
  ownerId: "u-9",
};
void _bad1;

// ✗ Neither resourceId nor ownerId — rejected: matches no variant of
// the union (each variant requires one of them).
// @ts-expect-error — missing scope identity
const _bad2: AgentInvokeBody = {
  task: "summarize",
  namespaceId: "acme",
};
void _bad2;

// ✗ Missing namespaceId — rejected: required in both variants.
// @ts-expect-error — missing namespaceId
const _bad3: AgentInvokeBody = {
  task: "summarize",
  resourceId: "doc-42",
};
void _bad3;

// ✗ Missing task — rejected: required in both variants.
// @ts-expect-error — missing task
const _bad4: AgentInvokeBody = {
  namespaceId: "acme",
  resourceId: "doc-42",
};
void _bad4;
