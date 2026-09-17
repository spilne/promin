// ---------------------------------------------------------------------------
// Compile-time fixtures for `ZoryaClient.start` typed dispatch.
//
// `start<I, O>(workflow, params)` must:
//   1. Accept `params.input` exactly when it matches the workflow's Input.
//   2. Reject mismatched input shapes at compile time.
//   3. Thread the workflow's Output into the returned `WorkflowHandle<O>`
//      so `handle.result()` resolves to that exact type, not `unknown`.
//
// Pure types — no runtime exports beyond a marker constant. Compiled by
// tsc as part of the lib build, so a regression flips a build error.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowHandle } from "@promin/workflow";
import type { ZoryaClient } from "./zorya-client.ts";

declare const client: ZoryaClient;
declare const orderWf: Workflow<{ orderId: number }, { ok: boolean }>;

// 1. Happy path — typed input, typed output.
async function _typedStartHappy(): Promise<{ ok: boolean }> {
  const handle: WorkflowHandle<{ ok: boolean }> = await client.start(orderWf, {
    input: { orderId: 42 },
  });
  // result() infers from the workflow's Output, not unknown.
  return handle.result();
}
void _typedStartHappy;

// 2. Mismatched input is a compile error. Wrapping in `@ts-expect-error`
//    pins the rejection: if the type relationship ever loosens (e.g.
//    accidentally widening Input to `any`), tsc complains that the
//    expect-error directive is unused, and the build fails.
async function _typedStartRejectsBadInput() {
  // @ts-expect-error orderId must be number, not string
  await client.start(orderWf, { input: { orderId: "not-a-number" } });
  // @ts-expect-error missing required input field
  await client.start(orderWf, { input: {} });
}
void _typedStartRejectsBadInput;

// 3. Untyped fallback still works and yields an unknown-typed handle.
async function _untypedStartByName(): Promise<unknown> {
  const handle: WorkflowHandle<unknown> = await client.startByName("anything", {
    input: { whatever: 1 },
  });
  return handle.result();
}
void _untypedStartByName;

export const _zoryaClientStartFixtureCompiles = true as const;
