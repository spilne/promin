import { randomUUID } from "node:crypto";
import {
  admitTurn,
  addUsage,
  threadTurnWorkflowId,
  THREAD_AGENT_METADATA_KEY,
  TurnInProgressError,
  type AgentBudgets,
  type BudgetUsage,
  type MemoryStore,
  type Message,
  type RegisteredAgent,
} from "@promin/agent";
import {
  hasCapability,
  runUsage,
  SystemWallClock,
  workflow,
  type RunSecurityContext,
  type UsageReport,
  type UsageTotals,
  type WallClock,
  type Workflow,
  type WorkflowBudget,
  type WorkflowState,
} from "@promin/workflow";
import { normalizeRunSecurityContext, sameRunSecurityContext } from "@promin/workflow/storage-kit";
import {
  snapshotAuthorizationDecision,
  snapshotSecurityContext,
  snapshotSecurityPermission,
  snapshotSecurityResource,
  type AuthorizationDecision,
  type AuthorizationRequest,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityContext,
  type ZoryaSecurity,
} from "../../security/index.ts";
import type { Logger } from "../../server-config.ts";
import type { LocalWorkflows } from "../workflows/local-workflows.ts";
import type { ZoryaAgents } from "./zorya-agents.ts";

export const RETAINED_AGENT_WORKFLOW = "retained-agent";

export type RetainedErrorCode = "not_found" | "forbidden" | "conflict" | "invalid";

/** Typed failure so routes map status codes without parsing messages. */
export class RetainedSubagentError extends Error {
  override readonly name = "RetainedSubagentError";
  constructor(
    readonly code: RetainedErrorCode,
    message: string,
  ) {
    super(message);
  }
}

const TERMINAL = ["completed", "failed", "cancelled"];

export interface RetainedParent {
  readonly namespaceId: string;
  readonly resourceId: string;
  readonly agentId: string;
  readonly threadId: string;
}

export interface RetainedChildHandle {
  readonly childId: string;
  readonly workflowId: string;
  readonly agentId: string;
  readonly threadId: string;
  readonly status: string;
  readonly depth: number;
  /** What the child's turns spent so far (omitted before its first turn settles, or when the store lists no step rows). */
  readonly usage?: UsageTotals;
}

type DeliveryMode = "queued" | "follow-up" | "steer";
interface MailCommand {
  readonly task: string;
  readonly mode: DeliveryMode;
  readonly messageId: string;
}
interface RetainedInput {
  readonly childId: string;
  readonly agentId: string;
  readonly agentVersion: string;
  readonly parent: RetainedParent;
  readonly depth: number;
  readonly task: string;
  /** Starter identity persisted with both the retained input and durable run. */
  readonly securityContext?: RunSecurityContext;
}

export interface RetainedExecutionSecurityConfig {
  readonly security: ZoryaSecurity;
  readonly clock?: Pick<WallClock, "now">;
  readonly logger?: Pick<Logger, "error">;
  /** Built-in key mode adopts contextless legacy/direct children as operator. */
  readonly legacySecurityContext?: RunSecurityContext;
}

export interface RetainedSubagentsConfig {
  readonly agents: ZoryaAgents;
  readonly workflows: LocalWorkflows;
  readonly memory: MemoryStore;
  /**
   * Explicit capability policy. A parent cannot delegate until the host grants it.
   * The third argument carries the caller's scope (and parent), so a policy can
   * authorize the child per tenant; two-argument policies keep working.
   */
  readonly canDelegate: (
    parentAgentId: string,
    childAgentId: string,
    options: {
      readonly parent: RetainedParent;
      readonly scope: { readonly namespaceId: string; readonly resourceId: string };
    },
  ) => boolean | Promise<boolean>;
  readonly maxDepth?: number;
  readonly maxChildren?: number;
  /**
   * Mailbox safety-net poll (default 60s). A wake signal is only a latency
   * hint: on every poll the worker re-reads the inbox by sequence, so a lost
   * signal delays a message by at most this long instead of stalling it.
   */
  readonly pollIntervalMs?: number;
  /** An idle child (no message for this long) completes (default 1h). */
  readonly idleTtlMs?: number;
  /**
   * Upper bound of journal iterations (messages + idle polls) per run
   * (default 1000). The child then completes, bounding the journal.
   */
  readonly maxIterations?: number;
  /**
   * How often a running child turn checks its inbox for a steer (default
   * 250ms). A steer aborts the running turn: see `messageAgent`.
   */
  readonly steerPollMs?: number;
  /**
   * The parent's budgets (the same `AgentBudgets` the host gives its agents:
   * one ledger, thread and namespace limits). Every child turn is admitted
   * against the parent thread's standing, as a turn of the parent would be,
   * and its spend is recorded in the ledger under the parent thread, so it
   * counts toward the parent's thread budget and the namespace window. A
   * refused turn is recorded as a failed outcome; the child keeps running.
   * Give child agents resolved by `agents.resolve` no ledger of their own,
   * or their spend counts twice in the namespace window.
   */
  readonly budgets?: AgentBudgets;
  /**
   * Engine budget of each child's run (`workflow({ budget })`): `maxTokens` /
   * `maxCostUsd` cap everything the child spends over its lifetime. Each turn's
   * spend is reported with `ctx.reportUsage`; crossing a cap fails the child
   * run (it stops accepting messages) with `WorkflowBudgetExceededError`.
   */
  readonly childBudget?: WorkflowBudget;
}

type RetainedRuntimeConfig = RetainedSubagentsConfig & {
  readonly authorizeTurn: (
    input: RetainedInput,
    operation: RetainedSecurityOperation,
  ) => Promise<{
    readonly securityContext?: RunSecurityContext;
    readonly principal?: Principal;
  }>;
};

type RetainedSecurityOperation =
  | "spawn"
  | "spawn.commit"
  | "message"
  | "message.commit"
  | "stop"
  | "stop.commit"
  | "turn.registry"
  | "turn.resolve"
  | "turn.thread"
  | "turn.send";

interface RetainedAuthorizationTarget {
  readonly childId: string;
  readonly agentId: string;
  readonly parent: RetainedParent;
  readonly securityContext?: RunSecurityContext;
}

/** Durable, retained child agents. One workflow holds each child's ordered inbox. */
export class RetainedSubagents {
  private readonly config: RetainedSubagentsConfig;
  private executionSecurity?: {
    readonly security: ZoryaSecurity;
    readonly clock: Pick<WallClock, "now">;
    readonly logger: Pick<Logger, "error">;
    readonly legacySecurityContext?: RunSecurityContext;
  };
  readonly definition: Workflow<RetainedInput, void>;

  constructor(config: RetainedSubagentsConfig) {
    this.config = config;
    const service: RetainedRuntimeConfig = {
      ...config,
      authorizeTurn: (input, operation) => this.authorizeStoredTurn(input, operation),
    };
    this.definition = workflow<RetainedInput>({
      name: RETAINED_AGENT_WORKFLOW,
      type: "retained-agent",
      ...(config.childBudget !== undefined && { budget: config.childBudget }),
    })
      .journaled("retained-agent", function* (ctx, storedInput) {
        const input = decodeRetainedInput(storedInput);
        const pollMs = service.pollIntervalMs ?? 60_000;
        const maxIdlePolls = Math.max(1, Math.ceil((service.idleTtlMs ?? 3_600_000) / pollMs));
        const maxIterations = service.maxIterations ?? 1000;
        // Journals written before spend reporting hold bare strings.
        const first = yield* ctx.activity(
          "initial",
          () => runAndRecord(service, input, input.task, 0),
          { idempotent: true },
        );
        yield* ctx.reportUsage(engineUsageOf(typeof first === "string" ? undefined : first.spend));
        // The inbox is consumed by sequence. `inbox:<seq>` only wakes the
        // worker early; each poll reads the next unconsumed message itself.
        // The workflow completes after idleTtlMs idle or maxIterations
        // iterations (journal bound); a completed child is re-created with
        // a new childId (its transcript stays readable until deleted).
        let seq = 1;
        let idle = 0;
        for (let i = 0; i < maxIterations; i++) {
          yield* ctx.signal(`inbox:${seq}`, { timeout: pollMs });
          const polled = yield* ctx.activity(`poll:${i}`, () => consume(service, input, seq), {
            idempotent: true,
          });
          const outcome =
            typeof polled === "string" ? { result: polled, spend: undefined } : polled;
          if (outcome.result === "empty") {
            if (++idle >= maxIdlePolls) return;
          } else {
            idle = 0;
            seq++;
            if (outcome.spend) yield* ctx.reportUsage(engineUsageOf(outcome.spend));
          }
        }
      })
      .build();
    config.workflows.define(this.definition as Workflow<unknown, unknown>, {
      internal: true,
    });
  }

  /** Installed by ZoryaServer once its pluggable security implementation is known. */
  configureExecutionSecurity(config: RetainedExecutionSecurityConfig): void {
    const legacySecurityContext =
      config.legacySecurityContext === undefined
        ? undefined
        : normalizeRunSecurityContext(config.legacySecurityContext);
    this.executionSecurity = Object.freeze({
      security: config.security,
      clock: config.clock ?? SystemWallClock,
      logger: config.logger ?? console,
      ...(legacySecurityContext !== undefined && { legacySecurityContext }),
    });
  }

  async start(): Promise<void> {
    await this.reconcileMailboxes();
  }
  async stop(): Promise<void> {}

  /**
   * Admission checks (child limit, child id) are serialized per parent thread
   * in this process; across processes they rely on `agents.turnGate`.
   */
  private readonly spawnChains = new Map<string, Promise<unknown>>();

  async spawnAgent(params: {
    readonly parent: RetainedParent;
    readonly agentId: string;
    readonly task: string;
    readonly childId?: string;
    /** Trusted caller identity; never read from model or HTTP request bodies. */
    readonly securityContext?: RunSecurityContext;
  }): Promise<RetainedChildHandle> {
    const parent = decodeRetainedParent(params.parent);
    const requestedSecurityContext = params.securityContext;
    const admitted = Object.freeze({
      parent,
      agentId: params.agentId,
      task: params.task,
      ...(params.childId !== undefined && { childId: params.childId }),
      ...(requestedSecurityContext !== undefined && {
        securityContext: normalizeRunSecurityContext(requestedSecurityContext),
      }),
    });
    const key = `${parent.namespaceId}\u0000${parent.threadId}`;
    const previous = this.spawnChains.get(key) ?? Promise.resolve();
    const run = previous.then(
      () => this.admit(admitted),
      () => this.admit(admitted),
    );
    const tail = run.catch(() => undefined);
    this.spawnChains.set(key, tail);
    void tail.then(() => {
      if (this.spawnChains.get(key) === tail) this.spawnChains.delete(key);
    });
    return run;
  }

  private async admit(params: {
    readonly parent: RetainedParent;
    readonly agentId: string;
    readonly task: string;
    readonly childId?: string;
    readonly securityContext?: RunSecurityContext;
  }): Promise<RetainedChildHandle> {
    const { parent } = params;
    const gate = this.config.agents.turnGate;
    let lease: Awaited<ReturnType<NonNullable<typeof gate>["acquire"]>> | undefined;
    for (let attempt = 0; gate && attempt < 20; attempt++) {
      try {
        lease = await gate.acquire({
          key: { namespaceId: parent.namespaceId, threadId: `retained-spawn:${parent.threadId}` },
          ownerId: randomUUID(),
          policy: "strict",
        });
        break;
      } catch (error) {
        if (!(error instanceof TurnInProgressError) || attempt === 19) throw error;
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    }
    try {
      if (!params.task.trim())
        throw new RetainedSubagentError("invalid", "Subagent task must not be empty");
      // Snapshot/validate the trusted starter identity before any registry,
      // thread or delegation-policy work can run.
      const securityContext = this.contextForSpawn(params.securityContext);
      const childId = params.childId ?? randomUUID();
      const target: RetainedAuthorizationTarget = {
        childId,
        agentId: params.agentId,
        parent,
        ...(securityContext !== undefined && { securityContext }),
      };
      if (this.executionSecurity && securityContext === undefined) {
        await this.auditMissingIdentity(target, "spawn");
        throw new RetainedSubagentError("forbidden", "Retained subagent identity is required");
      }
      await this.authorizeIdentity(target, "spawn");
      const parentThread = await this.ownedParent(parent);
      const allowed = await this.config.canDelegate(parent.agentId, params.agentId, {
        parent,
        scope: { namespaceId: parent.namespaceId, resourceId: parent.resourceId },
      });
      if (!allowed) throw new RetainedSubagentError("forbidden", "Subagent capability denied");
      const depthRaw = (await parentThread.metadata()).retainedDepth;
      const depth = (typeof depthRaw === "number" ? depthRaw : 0) + 1;
      if (depth > (this.config.maxDepth ?? 3))
        throw new RetainedSubagentError("conflict", "Subagent depth limit exceeded");
      const children = await this.listChildAgents(parent);
      if (
        children.filter((child) => !TERMINAL.includes(child.status)).length >=
        (this.config.maxChildren ?? 8)
      )
        throw new RetainedSubagentError("conflict", "Subagent child limit exceeded");
      const recipe = await this.config.agents.registry.get({ id: params.agentId });
      if (!recipe || !recipe.enabled)
        throw new RetainedSubagentError("not_found", "Child agent unavailable");
      const workflowId = `retained-agent:${childId}`;
      if (await this.config.workflows.storage.loadWorkflow(workflowId)) {
        throw new RetainedSubagentError("conflict", "Child id already exists");
      }
      const input: RetainedInput = Object.freeze({
        childId,
        agentId: params.agentId,
        agentVersion: recipe.version,
        parent: Object.freeze({ ...parent }),
        depth,
        task: params.task,
        ...(securityContext !== undefined && { securityContext }),
      });
      await this.authorizeIdentity(input, "spawn.commit");
      await this.config.workflows.trigger(RETAINED_AGENT_WORKFLOW, input, {
        workflowId,
        namespace: parent.namespaceId,
        internal: true,
        ...(securityContext !== undefined && { securityContext }),
        metadata: {
          childId,
          agentId: params.agentId,
          agentVersion: recipe.version,
          parentAgentId: parent.agentId,
          parentThreadId: parent.threadId,
          resourceId: parent.resourceId,
          depth,
        },
      });
      const admitted = await this.config.workflows.storage.loadWorkflow(workflowId);
      if (!admitted) {
        throw new RetainedSubagentError("conflict", "Child workflow was not persisted");
      }
      const stored = decodeRetainedState(admitted);
      if (!sameRetainedInput(stored, input)) {
        throw new RetainedSubagentError("conflict", "Child id already exists");
      }
      return {
        childId,
        workflowId,
        agentId: params.agentId,
        threadId: childThreadId(childId),
        status: "admitted",
        depth,
      };
    } finally {
      if (lease) await gate?.release(lease.leaseId);
    }
  }

  async listChildAgents(parent: RetainedParent): Promise<RetainedChildHandle[]> {
    parent = decodeRetainedParent(parent);
    await this.ownedParent(parent);
    const states = await this.config.workflows.storage.listWorkflows({
      name: RETAINED_AGENT_WORKFLOW,
      namespace: parent.namespaceId,
      metadata: {
        parentAgentId: parent.agentId,
        parentThreadId: parent.threadId,
        resourceId: parent.resourceId,
      },
    });
    return states.flatMap((state) => {
      try {
        const input = decodeRetainedState(state);
        const usage = state.steps ? runUsage(state.steps) : undefined;
        return [
          {
            childId: input.childId,
            workflowId: state.workflowId,
            agentId: input.agentId,
            threadId: childThreadId(input.childId),
            status: state.status,
            depth: input.depth,
            ...(usage && { usage }),
          },
        ];
      } catch {
        return [];
      }
    });
  }

  async messageAgent(params: {
    readonly parent: RetainedParent;
    readonly childId: string;
    readonly task: string;
    readonly mode?: DeliveryMode;
    readonly messageId?: string;
  }): Promise<{ readonly seq: number }> {
    const input = await this.ownedChild(params.parent, params.childId, "message");
    const state = await this.config.workflows.storage.loadWorkflow(workflowIdOf(params.childId));
    if (state && TERMINAL.includes(state.status)) {
      throw new RetainedSubagentError("conflict", "Child agent has stopped");
    }
    if (!params.task.trim())
      throw new RetainedSubagentError("invalid", "Subagent message must not be empty");
    const mode = params.mode ?? "queued";
    const messageId = params.messageId ?? randomUUID();
    await this.authorizeStoredTurn(input, "message.commit");
    const [stored] = await this.config.memory.appendMessages(
      inboxKey(input),
      [
        {
          role: "user",
          content: params.task,
          metadata: { task: params.task, mode, messageId },
        } as Message,
      ],
      { idempotencyKey: messageId },
    );
    if (!stored) throw new Error("Subagent mailbox append failed");
    if (
      stored.role !== "user" ||
      stored.content !== params.task ||
      stored.metadata?.mode !== mode
    ) {
      throw new RetainedSubagentError("conflict", "Subagent message id conflict");
    }
    // The wake is only a latency hint: the worker polls the inbox by
    // sequence, so a failed signal write must not fail an appended message
    // (the client's retry would otherwise just append a second copy).
    try {
      await this.config.workflows.storage.deliverSignal({
        workflowId: workflowIdOf(params.childId),
        signalName: `inbox:${stored.seq}`,
        payload: messageId,
      });
    } catch {
      // picked up by the worker's next poll or by reconcileMailboxes
    }
    return { seq: stored.seq };
  }

  async readAgentMessages(
    parent: RetainedParent,
    childId: string,
  ): Promise<{
    readonly inbox: Message[];
    readonly transcript: Message[];
    /** Per-message results: done, failed (with the error) or superseded by a steer. */
    readonly outcomes: Message[];
  }> {
    const input = await this.ownedChild(parent, childId);
    const inbox = await this.config.memory.getMessages(inboxKey(input));
    const transcript = await this.config.memory.getMessages(childKey(input));
    const outcomes = await this.config.memory.getMessages(outcomeKey(input));
    return { inbox, transcript, outcomes };
  }

  /** Authoritative recipe behind an owned retained child, for route admission. */
  async resolveOwnedChildAgentId(parent: RetainedParent, childId: string): Promise<string> {
    return (await this.ownedChild(parent, childId)).agentId;
  }

  async stopAgent(parent: RetainedParent, childId: string): Promise<void> {
    const input = await this.ownedChild(parent, childId, "stop");
    await this.authorizeStoredTurn(input, "stop.commit");
    await this.config.workflows.storage.cancelWorkflow({ workflowId: workflowIdOf(childId) });
  }

  async deleteChildAgent(parent: RetainedParent, childId: string): Promise<void> {
    const input = await this.ownedChild(parent, childId);
    await this.stopAgent(parent, childId);
    const workflowId = workflowIdOf(childId);
    // A cancelled worker holds the run lock until its current activity
    // returns (a turn's bookkeeping): wait briefly instead of failing at once.
    let lock = await this.config.workflows.storage.tryLock({ workflowId, lockDurationMs: 5_000 });
    for (let attempt = 0; !lock.acquired && attempt < 100; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 20));
      lock = await this.config.workflows.storage.tryLock({ workflowId, lockDurationMs: 5_000 });
    }
    if (!lock.acquired)
      throw new RetainedSubagentError("conflict", "Child agent busy; retry cleanup");
    await this.config.workflows.storage.releaseLock({
      workflowId,
      ...(lock.token !== undefined && { guard: { fenceToken: lock.token } }),
    });
    await this.config.memory.deleteThread(inboxKey(input));
    await this.config.memory.deleteThread(childKey(input));
    await this.config.memory.deleteThread(outcomeKey(input));
    // The workflow row stays (storage has no per-run delete); mark it so the
    // child is no longer listed or addressable.
    await this.config.workflows.storage.setWorkflowMetadata({
      workflowId,
      patch: { deleted: true },
    });
  }

  /** Re-deliver wakes after a crash between mailbox append and signal write. */
  async reconcileMailboxes(): Promise<void> {
    const states = await this.config.workflows.storage.listWorkflows({
      name: RETAINED_AGENT_WORKFLOW,
    });
    for (const state of states) {
      if (TERMINAL.includes(state.status)) continue;
      let input: RetainedInput;
      try {
        input = decodeRetainedState(state);
      } catch {
        continue;
      }
      const consumed = new Set(
        (await this.config.memory.getMessages(outcomeKey(input))).map((m) =>
          "metadata" in m ? m.metadata?.seq : undefined,
        ),
      );
      for (const message of await this.config.memory.getMessages(inboxKey(input))) {
        if (consumed.has(message.seq)) continue;
        await this.config.workflows.storage.deliverSignal({
          workflowId: state.workflowId,
          signalName: `inbox:${message.seq}`,
          payload: "metadata" in message ? (message.metadata?.messageId ?? null) : null,
        });
      }
    }
  }

  private async ownedParent(parent: RetainedParent) {
    parent = decodeRetainedParent(parent);
    const recipe = await this.config.agents.registry.get({ id: parent.agentId });
    if (!recipe) throw new RetainedSubagentError("not_found", "Parent agent unavailable");
    const agent = (await this.config.agents.resolve(recipe, parent)).withScope(parent);
    const thread = await agent.thread(parent.threadId, { createIfMissing: false });
    if ((await thread.metadata())[THREAD_AGENT_METADATA_KEY] !== parent.agentId) {
      throw new RetainedSubagentError("not_found", "Parent thread not owned by agent");
    }
    return thread;
  }

  private async ownedChild(
    parent: RetainedParent,
    childId: string,
    operation?: "message" | "stop",
  ): Promise<RetainedInput> {
    parent = decodeRetainedParent(parent);
    childId = requiredId(childId, "childId");
    const state = await this.config.workflows.storage.loadWorkflow(workflowIdOf(childId));
    if (!state) throw new RetainedSubagentError("not_found", "Child agent not found");
    let input: RetainedInput;
    try {
      input = decodeRetainedState(state);
    } catch {
      throw new RetainedSubagentError("not_found", "Child agent not found");
    }
    if (input.childId !== childId || !sameRetainedParent(input.parent, parent)) {
      throw new RetainedSubagentError("not_found", "Child agent not found");
    }
    if (operation !== undefined) await this.authorizeStoredTurn(input, operation);
    await this.ownedParent(parent);
    return input;
  }

  private contextForSpawn(
    requested: RunSecurityContext | undefined,
  ): RunSecurityContext | undefined {
    if (requested !== undefined) return normalizeRunSecurityContext(requested);
    if (!this.executionSecurity) return undefined;
    if (this.executionSecurity.legacySecurityContext !== undefined) {
      return this.executionSecurity.legacySecurityContext;
    }
    return undefined;
  }

  /** Reload and bind the workflow's durable input/context at every side-effect boundary. */
  private async authorizeStoredTurn(
    expected: RetainedInput,
    operation: RetainedSecurityOperation,
  ): Promise<{
    readonly securityContext?: RunSecurityContext;
    readonly principal?: Principal;
  }> {
    const state = await this.config.workflows.storage.loadWorkflow(workflowIdOf(expected.childId));
    if (!state) {
      await this.auditBindingDenial(expected, operation);
      throw new RetainedSubagentError("forbidden", "Retained child is unavailable");
    }
    let input: RetainedInput;
    try {
      input = decodeRetainedState(state);
    } catch {
      await this.auditBindingDenial(expected, operation);
      throw new RetainedSubagentError("forbidden", "Retained child identity mismatch");
    }
    if (!sameRetainedInput(input, expected)) {
      await this.auditBindingDenial(expected, operation);
      throw new RetainedSubagentError("forbidden", "Retained child identity mismatch");
    }
    return this.authorizeIdentity(input, operation);
  }

  private async auditBindingDenial(
    input: RetainedAuthorizationTarget,
    operation: RetainedSecurityOperation,
  ): Promise<void> {
    if (!this.executionSecurity) return;
    const runContext = this.contextForSpawn(input.securityContext);
    if (!runContext) {
      await this.auditMissingIdentity(input, operation);
      return;
    }
    const actorRef = toPrincipalRef(runContext.actor);
    await this.auditRetainedAuthorization(
      input,
      operation,
      snapshotSecurityContext({
        actor: unresolvedPrincipal(actorRef),
        effective: toPrincipalRef(runContext.effective ?? runContext.actor),
      }),
      { allowed: false, reason: "identity_unknown" },
    );
  }

  /** Resolve current actor/effective principal, authorize agent.invoke and immutably audit it. */
  private async authorizeIdentity(
    input: RetainedAuthorizationTarget,
    operation: RetainedSecurityOperation,
  ): Promise<{
    readonly securityContext?: RunSecurityContext;
    readonly principal?: Principal;
  }> {
    if (!this.executionSecurity) {
      return input.securityContext === undefined
        ? {}
        : { securityContext: normalizeRunSecurityContext(input.securityContext) };
    }
    const runContext = this.contextForSpawn(input.securityContext);
    if (!runContext) {
      await this.auditMissingIdentity(input, operation);
      throw new RetainedSubagentError("forbidden", "Retained subagent identity is required");
    }
    const actorRef = toPrincipalRef(runContext.actor);
    const effectiveRef = toPrincipalRef(runContext.effective ?? runContext.actor);
    const unresolved = snapshotSecurityContext({
      actor: unresolvedPrincipal(actorRef),
      effective: effectiveRef,
    });
    const actorRaw = await this.executionSecurity.security.resolvePrincipal(actorRef);
    if (!actorRaw) {
      await this.auditRetainedAuthorization(input, operation, unresolved, {
        allowed: false,
        reason: "identity_unknown",
      });
      throw new RetainedSubagentError("forbidden", "Retained starter unavailable");
    }
    const actorContext = snapshotSecurityContext({ actor: actorRaw, effective: effectiveRef });
    if (!samePrincipalRef(actorContext.actor, actorRef)) {
      await this.auditRetainedAuthorization(input, operation, unresolved, {
        allowed: false,
        reason: "identity_unknown",
      });
      throw new RetainedSubagentError("forbidden", "Retained starter unavailable");
    }
    const resolvedRaw = await this.executionSecurity.security.resolveIdentity({
      actor: actorContext.actor,
      asserted: effectiveRef,
    });
    const resolved = resolvedRaw === undefined ? undefined : snapshotSecurityContext(resolvedRaw);
    if (
      !resolved ||
      !samePrincipalRef(resolved.actor, actorRef) ||
      !samePrincipalRef(resolved.effective, effectiveRef)
    ) {
      await this.auditRetainedAuthorization(input, operation, actorContext, {
        allowed: false,
        reason: "identity_unknown",
      });
      throw new RetainedSubagentError("forbidden", "Retained starter unavailable");
    }
    let principal: Principal | undefined;
    if (samePrincipalRef(resolved.actor, resolved.effective)) {
      principal = resolved.actor;
    } else {
      const effectiveRaw = await this.executionSecurity.security.resolvePrincipal(
        resolved.effective,
      );
      // Key-only mode intentionally has no user directory. Its operator is
      // the resolver principal while the durable effective ref remains exact.
      if (!effectiveRaw && resolved.actor.kind === "operator") principal = resolved.actor;
      if (effectiveRaw) {
        const effectiveSnapshot = snapshotSecurityContext({
          actor: effectiveRaw,
          effective: resolved.effective,
        }).actor;
        if (samePrincipalRef(effectiveSnapshot, resolved.effective)) principal = effectiveSnapshot;
      }
    }
    if (!principal) {
      await this.auditRetainedAuthorization(input, operation, resolved, {
        allowed: false,
        reason: "identity_unknown",
      });
      throw new RetainedSubagentError("forbidden", "Retained subject unavailable");
    }

    const permission = snapshotSecurityPermission({ resource: "agent", action: "invoke" });
    const resource = snapshotSecurityResource({
      kind: "agent",
      namespaceId: input.parent.namespaceId,
      id: input.agentId,
    });
    const decision = snapshotAuthorizationDecision(
      await this.executionSecurity.security.authorize({
        context: resolved,
        permission,
        resource,
      } as AuthorizationRequest),
    );
    await this.auditRetainedAuthorization(input, operation, resolved, decision);
    if (!decision.allowed) {
      throw new RetainedSubagentError("forbidden", "Retained child authorization denied");
    }
    return { securityContext: runContext, principal };
  }

  private async auditRetainedAuthorization(
    input: RetainedAuthorizationTarget,
    operation: RetainedSecurityOperation,
    context: SecurityContext,
    decisionSource: AuthorizationDecision,
  ): Promise<void> {
    const executionSecurity = this.executionSecurity;
    if (!executionSecurity) return;
    const permission = snapshotSecurityPermission({ resource: "agent", action: "invoke" });
    const resource = snapshotSecurityResource({
      kind: "agent",
      namespaceId: input.parent.namespaceId,
      id: input.agentId,
    });
    const decision = snapshotAuthorizationDecision(decisionSource);
    const event: SecurityAuditEvent = Object.freeze({
      event: "retained-agent.authorization",
      occurredAt: executionSecurity.clock.now().toISOString(),
      context,
      namespaceId: input.parent.namespaceId,
      resource,
      permission,
      decision,
      metadata: Object.freeze({
        operation,
        childId: input.childId,
        parentAgentId: input.parent.agentId,
        parentThreadId: input.parent.threadId,
        parentResourceId: input.parent.resourceId,
        actorKind: context.actor.kind,
        actorId: context.actor.id,
        effectiveKind: context.effective.kind,
        effectiveId: context.effective.id,
      }),
    });
    try {
      await executionSecurity.security.recordAudit(event);
    } catch (error) {
      try {
        executionSecurity.logger.error(
          `[zorya] retained security audit failed for ${decision.allowed ? "allowed" : "denied"} ${operation}`,
          error,
        );
      } catch {
        // Observability never changes an already denied result.
      }
      if (decision.allowed) {
        throw new RetainedSubagentError("forbidden", "Retained security audit unavailable");
      }
    }
  }

  /** Missing durable identity has no honest SecurityContext; never invent one for audit. */
  private async auditMissingIdentity(
    input: RetainedAuthorizationTarget,
    operation: RetainedSecurityOperation,
  ): Promise<void> {
    const executionSecurity = this.executionSecurity;
    if (!executionSecurity) return;
    const permission = snapshotSecurityPermission({ resource: "agent", action: "invoke" });
    const resource = snapshotSecurityResource({
      kind: "agent",
      namespaceId: input.parent.namespaceId,
      id: input.agentId,
    });
    const decision = snapshotAuthorizationDecision({
      allowed: false,
      reason: "identity_unknown",
    });
    // SecurityAuditEvent normally carries a resolved context. This denial is
    // the one honest exception: absence is the fact being audited, so the
    // runtime record intentionally omits `context` instead of inventing one.
    const event = Object.freeze({
      event: "retained-agent.authorization",
      occurredAt: executionSecurity.clock.now().toISOString(),
      namespaceId: input.parent.namespaceId,
      resource,
      permission,
      decision,
      metadata: Object.freeze({
        operation,
        denialReason: "missing_identity",
        childId: input.childId,
        parentAgentId: input.parent.agentId,
        parentThreadId: input.parent.threadId,
        parentResourceId: input.parent.resourceId,
        actorKind: null,
        actorId: null,
        effectiveKind: null,
        effectiveId: null,
      }),
    });
    try {
      await executionSecurity.security.recordAudit(event as unknown as SecurityAuditEvent);
    } catch (error) {
      try {
        executionSecurity.logger.error(
          `[zorya] retained security audit failed for denied ${operation}`,
          error,
        );
      } catch {
        // Audit failure never changes the missing-identity denial.
      }
    }
  }
}

function toPrincipalRef(ref: { readonly kind: string; readonly id: string }): PrincipalRef {
  return Object.freeze({ kind: ref.kind as PrincipalRef["kind"], id: ref.id });
}

function samePrincipalRef(left: PrincipalRef, right: PrincipalRef): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function unresolvedPrincipal(ref: PrincipalRef): Principal {
  return Object.freeze({
    kind: ref.kind,
    id: ref.id,
    accessRoles: Object.freeze({}),
    canActFor: false,
  });
}

const INVALID_RETAINED_ID = /[\p{Cc}\p{Cf}]/u;

function requiredId(value: unknown, label: string): string {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    value.length > 1024 ||
    INVALID_RETAINED_ID.test(value)
  ) {
    throw new RetainedSubagentError("invalid", `Invalid retained ${label}`);
  }
  return value;
}

function decodeRetainedParent(value: unknown): RetainedParent {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new RetainedSubagentError("invalid", "Invalid retained parent");
  }
  const source = value as Record<string, unknown>;
  const namespaceId = source.namespaceId;
  const resourceId = source.resourceId;
  const agentId = source.agentId;
  const threadId = source.threadId;
  return Object.freeze({
    namespaceId: requiredId(namespaceId, "parent namespaceId"),
    resourceId: requiredId(resourceId, "parent resourceId"),
    agentId: requiredId(agentId, "parent agentId"),
    threadId: requiredId(threadId, "parent threadId"),
  });
}

function decodeOptionalSecurityContext(value: unknown): RunSecurityContext | undefined {
  return value === undefined ? undefined : normalizeRunSecurityContext(value as RunSecurityContext);
}

function decodeRetainedInput(value: unknown): RetainedInput {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new RetainedSubagentError("invalid", "Invalid retained input");
  }
  // Snapshot every getter once. All later authorization and execution uses
  // only this detached, deeply frozen value.
  const source = value as Record<string, unknown>;
  const childId = source.childId;
  const agentId = source.agentId;
  const agentVersion = source.agentVersion;
  const parent = source.parent;
  const depth = source.depth;
  const task = source.task;
  const securityContext = source.securityContext;
  if (!Number.isSafeInteger(depth) || (depth as number) < 1) {
    throw new RetainedSubagentError("invalid", "Invalid retained depth");
  }
  if (typeof task !== "string" || task.trim().length === 0) {
    throw new RetainedSubagentError("invalid", "Invalid retained task");
  }
  const decodedSecurityContext = decodeOptionalSecurityContext(securityContext);
  return Object.freeze({
    childId: requiredId(childId, "childId"),
    agentId: requiredId(agentId, "agentId"),
    agentVersion: requiredId(agentVersion, "agentVersion"),
    parent: decodeRetainedParent(parent),
    depth: depth as number,
    task,
    ...(decodedSecurityContext !== undefined && { securityContext: decodedSecurityContext }),
  });
}

function decodeRetainedState(state: WorkflowState): RetainedInput {
  // Snapshot the storage-owned fields once before decoding any nested data.
  const workflowId = state.workflowId;
  const workflowName = state.workflowName;
  const namespace = state.namespace;
  const inputValue = state.input;
  const securityContextValue = state.securityContext;
  const metadataValue = state.metadata;
  const input = decodeRetainedInput(inputValue);
  const securityContext = decodeOptionalSecurityContext(securityContextValue);
  if (!metadataValue || typeof metadataValue !== "object" || Array.isArray(metadataValue)) {
    throw new RetainedSubagentError("invalid", "Invalid retained metadata");
  }
  const metadata = metadataValue as Record<string, unknown>;
  const childId = metadata.childId;
  const agentId = metadata.agentId;
  const agentVersion = metadata.agentVersion;
  const parentAgentId = metadata.parentAgentId;
  const parentThreadId = metadata.parentThreadId;
  const resourceId = metadata.resourceId;
  const depth = metadata.depth;
  const deleted = metadata.deleted;
  if (
    workflowId !== workflowIdOf(input.childId) ||
    workflowName !== RETAINED_AGENT_WORKFLOW ||
    namespace !== input.parent.namespaceId ||
    childId !== input.childId ||
    agentId !== input.agentId ||
    agentVersion !== input.agentVersion ||
    parentAgentId !== input.parent.agentId ||
    parentThreadId !== input.parent.threadId ||
    resourceId !== input.parent.resourceId ||
    depth !== input.depth ||
    deleted === true ||
    !sameRunSecurityContext(securityContext, input.securityContext)
  ) {
    throw new RetainedSubagentError("invalid", "Invalid retained state binding");
  }
  return input;
}

function sameRetainedParent(left: RetainedParent, right: RetainedParent): boolean {
  return (
    left.namespaceId === right.namespaceId &&
    left.resourceId === right.resourceId &&
    left.agentId === right.agentId &&
    left.threadId === right.threadId
  );
}

function sameRetainedInput(left: RetainedInput, right: RetainedInput): boolean {
  return (
    left.childId === right.childId &&
    left.agentId === right.agentId &&
    left.agentVersion === right.agentVersion &&
    sameRetainedParent(left.parent, right.parent) &&
    left.depth === right.depth &&
    left.task === right.task &&
    sameRunSecurityContext(left.securityContext, right.securityContext)
  );
}

function workflowIdOf(childId: string): string {
  return `retained-agent:${childId}`;
}
function childThreadId(childId: string): string {
  return `retained-child:${childId}`;
}
function inboxKey(input: RetainedInput) {
  return {
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
    threadId: `retained-inbox:${input.childId}`,
  };
}
function outcomeKey(input: RetainedInput) {
  return {
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
    threadId: `retained-outcome:${input.childId}`,
  };
}
function childKey(input: RetainedInput) {
  return {
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
    threadId: childThreadId(input.childId),
  };
}

async function recordOutcome(params: {
  readonly config: RetainedSubagentsConfig;
  readonly input: RetainedInput;
  readonly seq: number;
  readonly status: "done" | "failed" | "superseded" | "interrupted";
  readonly detail?: string;
}): Promise<void> {
  const { config, input, seq, status, detail } = params;
  await config.memory.appendMessages(
    outcomeKey(input),
    [
      {
        role: "system",
        content: detail ? `${status}: ${detail}` : status,
        metadata: { seq, status },
      } as Message,
    ],
    { idempotencyKey: `outcome:${seq}` },
  );
}

/** One settled turn: how it ended, and what it spent (reported to the run's budget). */
interface TurnReport {
  readonly result: "done" | "failed" | "interrupted";
  readonly spend?: BudgetUsage;
}

/**
 * Run one turn; a failure is recorded and never fails the child workflow.
 * A steer that lands in the inbox after `seq` interrupts the turn. The turn
 * is admitted against the parent's budgets and its spend is charged to them.
 */
async function runAndRecord(
  config: RetainedRuntimeConfig,
  input: RetainedInput,
  task: string,
  seq: number,
): Promise<TurnReport> {
  const abort = new AbortController();
  const turnKey = `${input.childId}:${seq === 0 ? "initial" : `message:${seq}`}`;
  const watcher = setInterval(() => {
    void hasSteerAfter(config, input, seq).then(
      (steered) => {
        if (steered) abort.abort();
      },
      () => undefined,
    );
  }, config.steerPollMs ?? 250);
  let result: TurnReport["result"];
  try {
    if (config.budgets) {
      await admitTurn({
        budgets: config.budgets,
        namespaceId: input.parent.namespaceId,
        threadId: input.parent.threadId,
      });
    }
    const finish = await runChildTurn({ config, input, task, turnKey, signal: abort.signal });
    result = finish === "cancelled" ? "interrupted" : "done";
    await recordOutcome({ config, input, seq, status: result });
  } catch (error) {
    result = "failed";
    await recordOutcome({
      config,
      input,
      seq,
      status: "failed",
      detail: error instanceof Error ? error.message : String(error),
    });
  } finally {
    clearInterval(watcher);
  }
  const spend = await chargeParent({ config, input, turnKey });
  return { result, ...(spend && { spend }) };
}

/**
 * Read what the child's turn spent from its turn workflow (a failed or
 * refused turn may have spent nothing: no row) and record it in the parent
 * thread's ledger, once per turn.
 */
async function chargeParent(params: {
  readonly config: RetainedSubagentsConfig;
  readonly input: RetainedInput;
  readonly turnKey: string;
}): Promise<BudgetUsage | undefined> {
  const { config, input, turnKey } = params;
  const turnId = threadTurnWorkflowId({
    namespaceId: input.parent.namespaceId,
    threadId: childThreadId(input.childId),
    turnKey,
  });
  const spend = await turnSpend({ config, turnId });
  if (!spend) return undefined;
  await config.budgets?.ledger.record({
    turnId: `retained:${turnId}`,
    namespaceId: input.parent.namespaceId,
    threadId: input.parent.threadId,
    agentId: input.agentId,
    usage: spend,
  });
  return spend;
}

/**
 * What a child turn spent. A settled turn carries it in its result. A turn
 * that failed has no result, so the spend is summed from the model calls
 * (think and compaction steps) its journal recorded before it failed. Tool
 * and sub-agent spend of a failed turn is not in the journal values read here.
 */
async function turnSpend(params: {
  readonly config: RetainedSubagentsConfig;
  readonly turnId: string;
}): Promise<BudgetUsage | undefined> {
  const { config, turnId } = params;
  const state = await config.workflows.storage.loadWorkflow(turnId);
  const settled = (state?.result as { spend?: BudgetUsage } | undefined)?.spend;
  if (settled) return settled;
  const storage = config.workflows.storage;
  if (!state || !hasCapability(storage, "journal")) return undefined;
  const entries = await storage.loadJournal({
    workflowId: turnId,
    stepName: "agent",
  });
  let total: BudgetUsage | undefined;
  for (const entry of entries) {
    if (!/^(think|compact)-\d+$/.test(entry.activityName) || entry.exit?.tag !== "Success")
      continue;
    const value = (entry.exit.value as { value?: { spend?: BudgetUsage } } | null)?.value;
    if (value?.spend) total = total ? addUsage(total, value.spend) : value.spend;
  }
  return total;
}

/** One turn's spend as the workflow engine counts it: all prompt tokens, cache included. */
function engineUsageOf(spend: BudgetUsage | undefined): UsageReport {
  if (!spend) return {};
  return {
    inputTokens: spend.inputTokens + spend.cacheReadTokens + spend.cacheWriteTokens,
    outputTokens: spend.outputTokens,
    costUsd: spend.costUsd,
  };
}

async function hasSteerAfter(
  config: RetainedSubagentsConfig,
  input: RetainedInput,
  seq: number,
): Promise<boolean> {
  const later = await config.memory.getMessages(inboxKey(input), { fromSeq: seq + 1 });
  return later.some((item) => "metadata" in item && item.metadata?.mode === "steer");
}

/** Process the inbox message at `seq` if it exists. */
async function consume(
  config: RetainedRuntimeConfig,
  input: RetainedInput,
  seq: number,
): Promise<{
  readonly result: "empty" | "skipped" | "superseded" | TurnReport["result"];
  readonly spend?: BudgetUsage;
}> {
  const key = inboxKey(input);
  const [stored] = await config.memory.getMessages(key, { fromSeq: seq, toSeq: seq });
  if (!stored) return { result: "empty" };
  if (stored.role !== "user") return { result: "skipped" };
  const command = stored.metadata as unknown as MailCommand | undefined;
  if (!command || typeof command.task !== "string") return { result: "skipped" };
  // A steer supersedes queued work that has not started yet, and interrupts
  // a turn that is running (see runAndRecord).
  if (command.mode !== "steer") {
    const later = await config.memory.getMessages(key, { fromSeq: seq + 1 });
    if (later.some((item) => "metadata" in item && item.metadata?.mode === "steer")) {
      await recordOutcome({ config, input, seq, status: "superseded" });
      return { result: "superseded" };
    }
  }
  return runAndRecord(config, input, command.task, seq);
}

async function runChildTurn(params: {
  readonly config: RetainedRuntimeConfig;
  readonly input: RetainedInput;
  readonly task: string;
  readonly turnKey: string;
  readonly signal: AbortSignal;
}): Promise<string> {
  const { config, input, task, turnKey, signal } = params;
  // Reauthorize after every await that could race revocation. Each next
  // side-effecting layer sees only the principal resolved at its boundary.
  await config.authorizeTurn(input, "turn.registry");
  const recipe: RegisteredAgent | null = await config.agents.registry.get({
    id: input.agentId,
    version: input.agentVersion,
  });
  if (!recipe) throw new Error("Pinned child agent recipe unavailable");
  const resolverIdentity = await config.authorizeTurn(input, "turn.resolve");
  const resolvedAgent = await config.agents.resolve(recipe, {
    ...input.parent,
    ...(resolverIdentity.principal !== undefined && { principal: resolverIdentity.principal }),
    ...(resolverIdentity.securityContext !== undefined && {
      securityContext: resolverIdentity.securityContext,
    }),
  });
  await config.authorizeTurn(input, "turn.thread");
  const agent = resolvedAgent.withScope({
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
  });
  const thread = await agent.thread(childThreadId(input.childId), {
    ...(resolverIdentity.securityContext !== undefined && {
      securityContext: resolverIdentity.securityContext,
    }),
    metadata: {
      retainedParentAgentId: input.parent.agentId,
      retainedParentThreadId: input.parent.threadId,
      retainedDepth: input.depth,
    },
  });
  const sendIdentity = await config.authorizeTurn(input, "turn.send");
  const result = await thread.send(
    { task },
    {
      turnKey,
      signal,
      ...(sendIdentity.securityContext !== undefined && {
        securityContext: sendIdentity.securityContext,
      }),
    },
  );
  await result.text;
  return await result.finishReason;
}
