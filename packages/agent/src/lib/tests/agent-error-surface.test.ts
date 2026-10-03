import { describe, it, expect } from "bun:test";
import { fail } from "@spilne/perfect-core";
import {
  WorkflowSuspendedError,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { surfaceAgentError } from "../agent-shared.ts";
import { MaxStepsError } from "../agent-action.ts";

// `@promin/workflow` doesn't re-export TerminalError / RetryableError
// publicly. surfaceAgentError discriminates by the `_tag` shape tagged
// errors stamp onto the instance, so the tests fabricate the same shape —
// no public dependency on the constructor.
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

/** What `runner.run()` rejects with when the workflow fails. */
async function runnerRejection(
  build: () => { build(): Workflow<number, unknown> },
): Promise<unknown> {
  const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
  try {
    await runner.run({ workflow: build().build(), workflowId: "wf-surface", input: 0 });
  } catch (err) {
    return err;
  }
  throw new Error("expected the workflow run to reject");
}

describe("surfaceAgentError — classifies thrown values", () => {
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

  it("classifies a runner rejection from a sleeping workflow as 'suspended'", async () => {
    const err = await runnerRejection(() =>
      workflow<number>({ name: "sleeper" }).journaled("body", function* (ctx) {
        yield* ctx.sleep(60_000);
        return 1;
      }),
    );
    expect(err).toBeInstanceOf(WorkflowSuspendedError);
    expect(surfaceAgentError(err).kind).toBe("suspended");
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

  it("surfaces a typed step failure from the runner verbatim", async () => {
    const inner = new TerminalError("rejected");
    const err = await runnerRejection(() =>
      workflow<number>({ name: "typed-fail" }).step("s", () => fail(inner)),
    );
    expect(err).toBe(inner);
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("user-error");
    expect((result.error as { _tag?: string })._tag).toBe("TerminalError");
  });

  it("surfaces a thrown stepAsync error from the runner verbatim", async () => {
    const inner = new Error("inner cause");
    const err = await runnerRejection(() =>
      workflow<number>({ name: "async-throw" }).stepAsync("s", async () => {
        throw inner;
      }),
    );
    expect(err).toBe(inner);
    const result = surfaceAgentError(err);
    expect(result.kind).toBe("user-error");
    expect(result.error).toBe(inner);
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
