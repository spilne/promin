import { TaggedError, fail } from "@spilne/perfect-core";
import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import {
  decodeRunSecurityContext,
  normalizeRunSecurityContext,
  sameRunSecurityContext,
  type RunPrincipalKind,
  type RunSecurityContext,
} from "../run-security-context.ts";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import type {
  CreateWorkflowParams,
  CreateWorkflowResult,
  TryLockAndLoadResult,
  TryLockParams,
} from "../workflow-storage.ts";

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

/** Holds the lock winner immediately before its first row create. */
class GatedCreateStorage extends InMemoryWorkflowStorage {
  private readonly createGate = gate();
  private readonly createEnteredGate = gate();
  private readonly loserLockGate = gate();
  private lockAttempts = 0;

  readonly createEntered = this.createEnteredGate.promise;
  readonly loserLockAttempted = this.loserLockGate.promise;

  constructor(private readonly failCreate = false) {
    super();
  }

  allowCreate(): void {
    this.createGate.open();
  }

  override async tryLockAndLoad(params: TryLockParams): Promise<TryLockAndLoadResult> {
    const result = await super.tryLockAndLoad(params);
    this.lockAttempts++;
    if (this.lockAttempts === 2) this.loserLockGate.open();
    return result;
  }

  override async createWorkflow(params: CreateWorkflowParams): Promise<CreateWorkflowResult> {
    this.createEnteredGate.open();
    await this.createGate.promise;
    if (this.failCreate) throw new Error("winner stopped before create");
    return super.createWorkflow(params);
  }
}

function joinWorkflow() {
  return workflow({ name: "secured-pre-create-race" })
    .step("done", async () => "ok")
    .build({ idempotency: { ttl: 60_000, onInFlight: "join" } });
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

  it("rejects malformed and hostile contexts before exposing a cached secured run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow({ name: "secured-cached-malformed-start" })
      .step("done", async () => "secret")
      .build({ idempotency: { ttl: 60_000, onInFlight: "join" } });
    const workflowId = "secured-cached-malformed-start-1";

    expect(await runner.run({ workflow: wf, workflowId, input: {}, securityContext: actor1 })).toBe(
      "secret",
    );

    for (const securityContext of [
      { actor: { kind: "admin", id: "admin-1" } },
      { actor: { kind: "service-account", id: "bad\nid" } },
    ]) {
      await expect(
        runner.start({
          workflow: wf,
          workflowId,
          input: {},
          securityContext: securityContext as unknown as RunSecurityContext,
        }),
      ).rejects.toBeInstanceOf(TypeError);
    }

    let throwingReads = 0;
    await expect(
      runner.start({
        workflow: wf,
        workflowId,
        input: {},
        get securityContext(): RunSecurityContext {
          throwingReads++;
          throw new TypeError("hostile cached context getter");
        },
      }),
    ).rejects.toThrow("hostile cached context getter");
    expect(throwingReads).toBe(1);

    let statefulReads = 0;
    await expect(
      runner.start({
        workflow: wf,
        workflowId,
        input: {},
        get securityContext(): RunSecurityContext {
          statefulReads++;
          return statefulReads === 1
            ? ({ actor: { kind: "admin", id: "admin-1" } } as unknown as RunSecurityContext)
            : actor1;
        },
      }),
    ).rejects.toBeInstanceOf(TypeError);
    expect(statefulReads).toBe(1);
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

  it("rejects malformed and hostile contexts before exposing an in-flight secured run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const release = gate();
    const wf = workflow({ name: "secured-in-flight-malformed-start" })
      .step("wait", async () => {
        await release.promise;
        return "secret";
      })
      .build({ idempotency: { ttl: 60_000, onInFlight: "join" } });
    const workflowId = "secured-in-flight-malformed-start-1";
    const first = await runner.start({
      workflow: wf,
      workflowId,
      input: {},
      securityContext: actor1,
    });

    try {
      for (const securityContext of [
        { actor: { kind: "admin", id: "admin-1" } },
        { actor: { kind: "service-account", id: "bad\u0000id" } },
      ]) {
        await expect(
          runner.start({
            workflow: wf,
            workflowId,
            input: {},
            securityContext: securityContext as unknown as RunSecurityContext,
          }),
        ).rejects.toBeInstanceOf(TypeError);
      }

      let throwingReads = 0;
      await expect(
        runner.start({
          workflow: wf,
          workflowId,
          input: {},
          get securityContext(): RunSecurityContext {
            throwingReads++;
            throw new TypeError("hostile in-flight context getter");
          },
        }),
      ).rejects.toThrow("hostile in-flight context getter");
      expect(throwingReads).toBe(1);

      let statefulReads = 0;
      await expect(
        runner.start({
          workflow: wf,
          workflowId,
          input: {},
          get securityContext(): RunSecurityContext {
            statefulReads++;
            return statefulReads === 1
              ? ({
                  actor: { kind: "service-account", id: "bad\u200Bid" },
                } as unknown as RunSecurityContext)
              : actor1;
          },
        }),
      ).rejects.toBeInstanceOf(TypeError);
      expect(statefulReads).toBe(1);
    } finally {
      release.open();
    }
    expect(await first.result({ intervalMs: 1 })).toBe("secret");
  });

  it("waits for a lock winner's row and denies a mismatched pre-create join", async () => {
    const storage = new GatedCreateStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = joinWorkflow();
    const first = await runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-mismatch",
      input: {},
      securityContext: actor1,
    });
    await storage.createEntered;

    const loser = runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-mismatch",
      input: {},
      securityContext: actor2,
    });
    void loser.catch(() => undefined);
    await storage.loserLockAttempted;
    storage.allowCreate();

    await expect(loser).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
    expect(await first.result({ intervalMs: 1 })).toBe("ok");
  });

  it("waits for a lock winner's row and joins the same pre-create context", async () => {
    const storage = new GatedCreateStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = joinWorkflow();
    const first = await runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-same",
      input: {},
      securityContext: actor1,
    });
    await storage.createEntered;

    const joinedPromise = runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-same",
      input: {},
      securityContext: actor1,
    });
    await storage.loserLockAttempted;
    storage.allowCreate();

    const joined = await joinedPromise;
    expect(joined.workflowId).toBe(first.workflowId);
    expect(await first.result({ intervalMs: 1 })).toBe("ok");
    expect(await joined.result({ intervalMs: 1 })).toBe("ok");
  });

  it("fails a secured join closed when the lock winner never creates a row", async () => {
    const clock = FakeWallClock.create(1_000);
    const storage = new GatedCreateStorage(true);
    const runner = createWorkflowRunner({ storage, clock });
    const wf = joinWorkflow();
    await runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-no-row",
      input: {},
      securityContext: actor1,
    });
    await storage.createEntered;

    const loser = runner.start({
      workflow: wf,
      workflowId: "secured-pre-create-no-row",
      input: {},
      securityContext: actor1,
    });
    void loser.catch(() => undefined);
    await storage.loserLockAttempted;
    storage.allowCreate();

    let settled = false;
    void loser.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    for (let attempt = 0; attempt < 1_000 && !settled; attempt += 1) {
      await Promise.resolve();
      clock.advance(1);
    }
    expect(settled).toBe(true);

    await expect(loser).rejects.toMatchObject({ _tag: "RunSecurityContextConflictError" });
    expect(await storage.loadWorkflow("secured-pre-create-no-row")).toBeNull();
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
    expect(
      decodeRunSecurityContext({ actor: { kind: "service-account", id: "build agent" } }),
    ).toEqual({
      actor: { kind: "service-account", id: "build agent" },
    });
    for (const id of ["", " ", "\t", "line\nid", "nul\u0000id", "zero\u200Bid"]) {
      expect(() => decodeRunSecurityContext({ actor: { kind: "user", id } })).toThrow(TypeError);
    }
  });

  it("snapshots hostile context and principal getters exactly once", () => {
    let actorReads = 0;
    let effectiveReads = 0;
    let actorKindReads = 0;
    let actorIdReads = 0;
    let effectiveKindReads = 0;
    let effectiveIdReads = 0;
    const hostileActor = {
      get kind() {
        actorKindReads++;
        return actorKindReads === 1 ? "service-account" : "operator";
      },
      get id() {
        actorIdReads++;
        return actorIdReads === 1 ? "build agent" : "mutated";
      },
    };
    const hostileEffective = {
      get kind() {
        effectiveKindReads++;
        return effectiveKindReads === 1 ? "user" : "operator";
      },
      get id() {
        effectiveIdReads++;
        return effectiveIdReads === 1 ? "customer user" : "operator";
      },
    };
    const hostileContext = {
      get actor() {
        actorReads++;
        return hostileActor;
      },
      get effective() {
        effectiveReads++;
        return effectiveReads === 1 ? hostileEffective : { kind: "operator", id: "operator" };
      },
    } as unknown as RunSecurityContext;

    expect(normalizeRunSecurityContext(hostileContext)).toEqual({
      actor: { kind: "service-account", id: "build agent" },
      effective: { kind: "user", id: "customer user" },
    });
    expect({
      actorReads,
      effectiveReads,
      actorKindReads,
      actorIdReads,
      effectiveKindReads,
      effectiveIdReads,
    }).toEqual({
      actorReads: 1,
      effectiveReads: 1,
      actorKindReads: 1,
      actorIdReads: 1,
      effectiveKindReads: 1,
      effectiveIdReads: 1,
    });
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
