import { TaggedError, fail } from "@spilne/perfect-core";
import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import {
  decodeRunSecurityContext,
  normalizeRunSecurityContext,
  sameRunSecurityContext,
  type RunPrincipalKind,
} from "../run-security-context.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

class ExpectedFailure extends TaggedError("ExpectedFailure")<{
  readonly message: string;
}>() {}

const actor1 = { actor: { kind: "service-account", id: "sa-1" } } as const;
const actor2 = { actor: { kind: "service-account", id: "sa-2" } } as const;

function gate(): { readonly promise: Promise<void>; readonly open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

describe("durable run security context", () => {
  it("rejects a different actor before returning a cached success", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "secured-cached-success" })
      .step("done", async () => "ok")
      .build({ idempotency: { ttl: 60_000 } });

    expect(
      await runner.run({
        workflow: wf,
        workflowId: "secured-cached-success-1",
        input: {},
        securityContext: actor1,
      }),
    ).toBe("ok");
    await expect(
      runner.run({
        workflow: wf,
        workflowId: "secured-cached-success-1",
        input: {},
        securityContext: actor2,
      }),
    ).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
  });

  it("rejects a different actor before returning a cached failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "secured-cached-failure" })
      .step("fail", () => fail(new ExpectedFailure({ message: "expected" })))
      .build({ idempotency: { ttl: { success: 60_000, failure: 60_000 } } });

    await expect(
      runner.run({
        workflow: wf,
        workflowId: "secured-cached-failure-1",
        input: {},
        securityContext: actor1,
      }),
    ).rejects.toBeInstanceOf(ExpectedFailure);
    await expect(
      runner.run({
        workflow: wf,
        workflowId: "secured-cached-failure-1",
        input: {},
        securityContext: actor2,
      }),
    ).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
  });

  it("rejects a different actor before joining an in-flight start", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const release = gate();
    const wf = workflow({ name: "secured-in-flight" })
      .step("wait", async () => {
        await release.promise;
        return "ok";
      })
      .build({ idempotency: { ttl: 60_000, onInFlight: "join" } });

    const first = await runner.start({
      workflow: wf,
      workflowId: "secured-in-flight-1",
      input: {},
      securityContext: actor1,
    });
    try {
      await expect(
        runner.start({
          workflow: wf,
          workflowId: "secured-in-flight-1",
          input: {},
          securityContext: actor2,
        }),
      ).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
    } finally {
      release.open();
    }
    expect(await first.result({ intervalMs: 1 })).toBe("ok");
  });

  it("keeps the requested actor through version-drain recursion", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const v1 = workflow({ name: "secured-drain", version: "1" })
      .step("done", async () => "v1")
      .build();
    const v2 = workflow({
      name: "secured-drain",
      version: "2",
      onVersionMismatch: "drain",
      previousVersions: [v1],
    })
      .step("done", async () => "v2")
      .build();

    await runner.run({
      workflow: v1,
      workflowId: "secured-drain-1",
      input: {},
      securityContext: actor1,
    });
    await expect(
      runner.run({
        workflow: v2,
        workflowId: "secured-drain-1",
        input: {},
        securityContext: actor2,
      }),
    ).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
  });

  it("accepts only supported principal kinds and safe ids", () => {
    const kinds: RunPrincipalKind[] = ["operator", "worker", "user", "service-account"];
    for (const kind of kinds) {
      expect(decodeRunSecurityContext({ actor: { kind, id: `${kind}-1` } })).toEqual({
        actor: { kind, id: `${kind}-1` },
      });
    }

    expect(() => decodeRunSecurityContext({ actor: { kind: "admin", id: "admin-1" } })).toThrow(
      TypeError,
    );
    for (const id of ["", "two words", "tab\tid", "line\nid", "nul\u0000id", "zero\u200Bid"]) {
      expect(() => decodeRunSecurityContext({ actor: { kind: "user", id } })).toThrow(TypeError);
    }
  });

  it("canonicalizes an explicit self effective principal to omission", () => {
    const explicitSelf = normalizeRunSecurityContext({
      actor: { kind: "user", id: "user-1" },
      effective: { kind: "user", id: "user-1" },
    });
    const omittedSelf = normalizeRunSecurityContext({
      actor: { kind: "user", id: "user-1" },
    });

    expect(explicitSelf).toEqual(omittedSelf);
    expect(explicitSelf.effective).toBeUndefined();
    expect(sameRunSecurityContext(explicitSelf, omittedSelf)).toBe(true);
  });
});
