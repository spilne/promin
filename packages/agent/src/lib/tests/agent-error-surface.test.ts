import { describe, it, expect } from "bun:test";
import { WorkflowSuspendedError } from "@promin/workflow";
import { surfaceAgentError } from "../agent-shared.ts";
import { MaxStepsError } from "../agent-action.ts";

// `@promin/workflow` doesn't re-export TerminalError / RetryableError
// publicly. surfaceAgentError discriminates by the `_tag` shape that
// Effect's Data.TaggedError stamps onto the instance, so the tests
// fabricate the same shape — no public dependency on the constructor.
class TerminalError extends Error {
  readonly _tag = "TerminalError" as const;
  constructor(message: string) {
    super(message);
    this.name = "TerminalError";
  }
}
class RetryableError extends Error {
  readonly _tag = "RetryableError" as const;
  constructor(message: string) {
    super(message);
    this.name = "RetryableError";
  }
}

// Effect's Data.TaggedError uses this symbol to attach a cause to the
// FiberFailure shape exposed to the runner. Mirrors what the runner sees
// when an error propagates out of a journaled body via `runSafe`.
const FIBER_FAILURE_CAUSE = Symbol.for("effect/Runtime/FiberFailure/Cause");

function asFiberFailureDie(defect: unknown): Error {
  const wrapper = new Error("FiberFailure");
  (wrapper as unknown as Record<symbol, unknown>)[FIBER_FAILURE_CAUSE] = {
    _tag: "Die",
    defect,
  };
  return wrapper;
}

function asFiberFailureFail(error: unknown): Error {
  const wrapper = new Error("FiberFailure");
  (wrapper as unknown as Record<symbol, unknown>)[FIBER_FAILURE_CAUSE] = {
    _tag: "Fail",
    error,
  };
  return wrapper;
}

describe("surfaceAgentError — classifies + unwraps thrown values", () => {
  it("classifies a direct WorkflowSuspendedError as 'suspended'", () => {
    const err = new WorkflowSuspendedError({
      workflowId: "w-1",
      stepName: "s",
      reason: "signal",
      message: "waiting",
    });
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("suspended");
  });

  it("classifies a FiberFailure-wrapped WorkflowSuspendedError as 'suspended'", () => {
    const inner = new WorkflowSuspendedError({
      workflowId: "w-2",
      stepName: "s",
      reason: "signal",
      message: "waiting",
    });
    const wrapped = asFiberFailureDie(inner);
    const result = surfaceAgentError(wrapped);
    expect(result.kind).toBe("suspended");
  });

  it("classifies MaxStepsError as 'step-limit' and preserves the tag", () => {
    const err = new MaxStepsError(20);
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("step-limit");
    expect((result.error as { _tag?: string })._tag).toBe("MaxStepsError");
    expect(result.error.message).toContain("20");
  });

  it("classifies TerminalError as 'user-error' and preserves the tag", () => {
    const err = new TerminalError("policy: forbidden");
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("user-error");
    expect((result.error as { _tag?: string })._tag).toBe("TerminalError");
  });

  it("classifies RetryableError as 'infra-error'", () => {
    const err = new RetryableError("transient: 503");
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("infra-error");
    expect((result.error as { _tag?: string })._tag).toBe("RetryableError");
  });

  it("classifies a plain thrown Error as 'user-error'", () => {
    const err = new Error("tool blew up");
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("user-error");
    expect(result.error.message).toBe("tool blew up");
  });

  it("unwraps FiberFailure 'Fail' to surface the original error verbatim", () => {
    const inner = new Error("inner cause");
    const wrapped = asFiberFailureFail(inner);
    const result = surfaceAgentError(wrapped);
    expect(result.kind).toBe("user-error");
    expect(result.error).toBe(inner);
  });

  it("unwraps FiberFailure 'Die' to surface a TerminalError defect", () => {
    const inner = new TerminalError("rejected");
    const wrapped = asFiberFailureDie(inner);
    const result = surfaceAgentError(wrapped);
    expect(result.kind).toBe("user-error");
    expect((result.error as { _tag?: string })._tag).toBe("TerminalError");
  });

  it("classifies non-Error / unknown values as 'infra-error'", () => {
    const result = surfaceAgentError({ weird: true });
    expect(result.kind).toBe("infra-error");
    expect(result.error).toBeInstanceOf(Error);
  });

  it("classifies a string defect as 'infra-error' with the string as message", () => {
    const result = surfaceAgentError("raw string defect");
    expect(result.kind).toBe("infra-error");
    expect(result.error.message).toBe("raw string defect");
  });
});
