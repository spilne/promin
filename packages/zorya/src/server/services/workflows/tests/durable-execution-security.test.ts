import { describe, expect, it } from "bun:test";
import {
  InMemoryWorkflowStorage,
  SystemWallClock,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";
import { claimWorkflowStarts } from "../../../routes/workflow-starts.ts";
import {
  principalRef,
  type Principal,
  type PrincipalRef,
  type ZoryaSecurity,
} from "../../../security/index.ts";
import { LocalWorkflows } from "../local-workflows.ts";
import { QueuedWorkflows } from "../queued-workflows.ts";

const actor: Principal = {
  kind: "service-account",
  id: "runner",
  accessRoles: { team: ["member"] },
  canActFor: false,
};
const runContext = { actor: principalRef(actor) } as const;
const quietLogger = { error() {} };

function mutableSecurity(state: { allowed: boolean }): ZoryaSecurity {
  const resolve = async (ref: PrincipalRef) =>
    ref.kind === actor.kind && ref.id === actor.id ? actor : undefined;
  return {
    authenticate: async () => undefined,
    resolvePrincipal: resolve,
    resolveIdentity: async ({ actor: current }) => ({
      actor: current,
      effective: principalRef(current),
    }),
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async ({ context }) =>
      (await resolve(context.actor)) && state.allowed
        ? { allowed: true, reason: "access_role_allow" }
        : { allowed: false, reason: "permission_denied" },
    recordAudit: async () => {},
  };
}

async function until(done: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 100; i++) {
    if (await done()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition not reached");
}

describe("durable execution security paths", () => {
  it("LocalWorkflows blocks user code through the runner admission hook", async () => {
    const storage = new InMemoryWorkflowStorage();
    let bodies = 0;
    const secured = workflow<void>({ name: "secured-local" })
      .step("body", async () => {
        bodies++;
      })
      .build();
    const local = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: { [secured.name]: secured },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    local.configureExecutionSecurity({
      security: mutableSecurity({ allowed: false }),
      clock: SystemWallClock,
      logger: quietLogger,
    });

    const { workflowId } = await local.trigger(secured.name, undefined, {
      workflowId: "local-denied",
      namespace: "team",
      securityContext: runContext,
    });
    await until(async () => (await storage.loadWorkflow(workflowId))?.status === "failed");

    expect(bodies).toBe(0);
    expect(await storage.loadWorkflow(workflowId)).toMatchObject({
      status: "failed",
      errorTag: "RunSecurityBlockedError",
    });
  });

  it("a revoked queued run is consumed at claim and never exposed to a worker", async () => {
    const storage = new InMemoryWorkflowStorage();
    const starts = new InMemoryWorkflowStartQueue();
    const policy = { allowed: true };
    const queued = new QueuedWorkflows({
      storage,
      workflowStarts: starts,
      acceptAny: true,
      maintenance: false,
      wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
    });
    queued.configureExecutionSecurity({
      security: mutableSecurity(policy),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    await queued.trigger("remote", null, {
      workflowId: "queued-revoked",
      namespace: "team",
      securityContext: runContext,
    });
    expect(await starts.list()).toHaveLength(1);

    policy.allowed = false;
    const claim = claimWorkflowStarts(starts, undefined, (start) =>
      queued.authorizeWorkerStart(start.workflowId),
    );
    const response = await claim(
      new Request("http://zorya.test/api/worker-protocol/claim-starts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          workflowSpecs: [{ name: "remote", versions: [] }],
          workerId: "worker-1",
          limit: 10,
        }),
      }),
    );

    expect(await response.json()).toEqual({ starts: [] });
    expect(await starts.list()).toEqual([]);
    expect(await storage.loadWorkflow("queued-revoked")).toMatchObject({
      status: "failed",
      errorTag: "RunSecurityBlockedError",
    });
  });

  it("rechecks policy again at the remote worker lock boundary", async () => {
    const storage = new InMemoryWorkflowStorage();
    const starts = new InMemoryWorkflowStartQueue();
    const policy = { allowed: true };
    const queued = new QueuedWorkflows({
      storage,
      workflowStarts: starts,
      acceptAny: true,
      maintenance: false,
      wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
    });
    queued.configureExecutionSecurity({
      security: mutableSecurity(policy),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    await queued.trigger("remote", null, {
      workflowId: "lock-race",
      namespace: "team",
      securityContext: runContext,
    });

    expect(await queued.authorizeWorkerStart("lock-race")).toBe(true);
    policy.allowed = false;
    expect(await queued.authorizeWorkerLock("lock-race")).toBe(false);
    expect(await storage.loadWorkflow("lock-race")).toMatchObject({
      status: "failed",
      errorTag: "RunSecurityBlockedError",
    });
  });

  it("admits a remote child only with its parent's exact current identity", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queued = new QueuedWorkflows({
      storage,
      acceptAny: true,
      maintenance: false,
      wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
    });
    queued.configureExecutionSecurity({
      security: mutableSecurity({ allowed: true }),
      clock: SystemWallClock,
      logger: quietLogger,
    });
    await storage.createWorkflow({
      workflowId: "parent",
      workflowName: "remote",
      input: null,
      namespace: "team",
      securityContext: runContext,
    });

    await expect(
      queued.authorizeWorkerChild({
        workflowId: "child",
        parentWorkflowId: "parent",
        namespace: "team",
        securityContext: runContext,
      }),
    ).resolves.toBe(true);
    await expect(
      queued.authorizeWorkerChild({
        workflowId: "forged-child",
        parentWorkflowId: "parent",
        namespace: "team",
        securityContext: { actor: { kind: "service-account", id: "forged" } },
      }),
    ).resolves.toBe(false);
  });

  it("releases a claimed start when security infrastructure fails before denial persists", async () => {
    const starts = new InMemoryWorkflowStartQueue();
    await starts.enqueue({ workflowId: "retry-security", workflowName: "remote" });
    const claim = claimWorkflowStarts(starts, undefined, async () => {
      throw new Error("security store down");
    });

    const response = await claim(
      new Request("http://zorya.test/api/worker-protocol/claim-starts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ workflowSpecs: [{ name: "remote", versions: [] }], limit: 1 }),
      }),
    );

    expect(await response.json()).toEqual({ starts: [] });
    const [released] = await starts.list();
    expect(released).toMatchObject({ workflowId: "retry-security" });
    expect(released).not.toHaveProperty("claimToken");
  });
});
