import { randomUUID } from "node:crypto";
import {
  THREAD_AGENT_METADATA_KEY,
  TurnInProgressError,
  type MemoryStore,
  type Message,
  type RegisteredAgent,
} from "@promin/agent";
import { workflow, type Workflow } from "@promin/workflow";
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
}

/** Durable, retained child agents. One workflow holds each child's ordered inbox. */
export class RetainedSubagents {
  private readonly config: RetainedSubagentsConfig;
  readonly definition: Workflow<RetainedInput, void>;

  constructor(config: RetainedSubagentsConfig) {
    this.config = config;
    this.definition = workflow<RetainedInput>({
      name: RETAINED_AGENT_WORKFLOW,
      type: "retained-agent",
    })
      .journaled("retained-agent", function* (ctx, input) {
        const service = config;
        const pollMs = service.pollIntervalMs ?? 60_000;
        const maxIdlePolls = Math.max(1, Math.ceil((service.idleTtlMs ?? 3_600_000) / pollMs));
        const maxIterations = service.maxIterations ?? 1000;
        yield* ctx.activity("initial", () => runAndRecord(service, input, input.task, 0), {
          idempotent: true,
        });
        // The inbox is consumed by sequence. `inbox:<seq>` only wakes the
        // worker early; each poll reads the next unconsumed message itself.
        // The workflow completes after idleTtlMs idle or maxIterations
        // iterations (journal bound); a completed child is re-created with
        // a new childId (its transcript stays readable until deleted).
        let seq = 1;
        let idle = 0;
        for (let i = 0; i < maxIterations; i++) {
          yield* ctx.signal(`inbox:${seq}`, { timeout: pollMs });
          const outcome = yield* ctx.activity(`poll:${i}`, () => consume(service, input, seq), {
            idempotent: true,
          });
          if (outcome === "empty") {
            if (++idle >= maxIdlePolls) return;
          } else {
            idle = 0;
            seq++;
          }
        }
      })
      .build();
    config.workflows.define(this.definition as Workflow<unknown, unknown>, { internal: true });
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
  }): Promise<RetainedChildHandle> {
    const key = `${params.parent.namespaceId}\u0000${params.parent.threadId}`;
    const previous = this.spawnChains.get(key) ?? Promise.resolve();
    const run = previous.then(
      () => this.admit(params),
      () => this.admit(params),
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
      const childId = params.childId ?? randomUUID();
      const workflowId = `retained-agent:${childId}`;
      if (await this.config.workflows.storage.loadWorkflow(workflowId)) {
        throw new RetainedSubagentError("conflict", "Child id already exists");
      }
      const input: RetainedInput = {
        childId,
        agentId: params.agentId,
        agentVersion: recipe.version,
        parent,
        depth,
        task: params.task,
      };
      await this.config.workflows.trigger(RETAINED_AGENT_WORKFLOW, input, {
        workflowId,
        namespace: parent.namespaceId,
        internal: true,
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
      // jsonb does not preserve key order: compare identity fields only.
      const stored = admitted?.input as Partial<RetainedInput> | undefined;
      if (
        stored &&
        (stored.childId !== input.childId ||
          stored.agentId !== input.agentId ||
          stored.agentVersion !== input.agentVersion ||
          stored.task !== input.task ||
          stored.parent?.threadId !== parent.threadId)
      ) {
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
    return states
      .filter((state) => state.metadata?.deleted !== true)
      .map((state) => {
        const input = state.input as RetainedInput;
        return {
          childId: input.childId,
          workflowId: state.workflowId,
          agentId: input.agentId,
          threadId: childThreadId(input.childId),
          status: state.status,
          depth: input.depth,
        };
      });
  }

  async messageAgent(params: {
    readonly parent: RetainedParent;
    readonly childId: string;
    readonly task: string;
    readonly mode?: DeliveryMode;
    readonly messageId?: string;
  }): Promise<{ readonly seq: number }> {
    const input = await this.ownedChild(params.parent, params.childId);
    const state = await this.config.workflows.storage.loadWorkflow(workflowIdOf(params.childId));
    if (state && TERMINAL.includes(state.status)) {
      throw new RetainedSubagentError("conflict", "Child agent has stopped");
    }
    if (!params.task.trim())
      throw new RetainedSubagentError("invalid", "Subagent message must not be empty");
    const mode = params.mode ?? "queued";
    const messageId = params.messageId ?? randomUUID();
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

  async stopAgent(parent: RetainedParent, childId: string): Promise<void> {
    await this.ownedChild(parent, childId);
    await this.config.workflows.storage.cancelWorkflow({ workflowId: workflowIdOf(childId) });
  }

  async deleteChildAgent(parent: RetainedParent, childId: string): Promise<void> {
    const input = await this.ownedChild(parent, childId);
    await this.stopAgent(parent, childId);
    const workflowId = workflowIdOf(childId);
    const lock = await this.config.workflows.storage.tryLock({ workflowId, lockDurationMs: 5_000 });
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
      if (TERMINAL.includes(state.status) || state.metadata?.deleted === true) continue;
      const input = state.input as RetainedInput;
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
    const recipe = await this.config.agents.registry.get({ id: parent.agentId });
    if (!recipe) throw new RetainedSubagentError("not_found", "Parent agent unavailable");
    const agent = (await this.config.agents.resolve(recipe, parent)).withScope(parent);
    const thread = await agent.thread(parent.threadId, { createIfMissing: false });
    if ((await thread.metadata())[THREAD_AGENT_METADATA_KEY] !== parent.agentId) {
      throw new RetainedSubagentError("not_found", "Parent thread not owned by agent");
    }
    return thread;
  }

  private async ownedChild(parent: RetainedParent, childId: string): Promise<RetainedInput> {
    await this.ownedParent(parent);
    const state = await this.config.workflows.storage.loadWorkflow(workflowIdOf(childId));
    if (
      !state ||
      state.workflowName !== RETAINED_AGENT_WORKFLOW ||
      state.namespace !== parent.namespaceId ||
      state.metadata?.parentAgentId !== parent.agentId ||
      state.metadata?.parentThreadId !== parent.threadId ||
      state.metadata?.resourceId !== parent.resourceId ||
      state.metadata?.deleted === true
    )
      throw new RetainedSubagentError("not_found", "Child agent not found");
    return state.input as RetainedInput;
  }
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
  readonly status: "done" | "failed" | "superseded";
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

/** Run one turn; a failure is recorded and never fails the child workflow. */
async function runAndRecord(
  config: RetainedSubagentsConfig,
  input: RetainedInput,
  task: string,
  seq: number,
): Promise<"done" | "failed"> {
  try {
    await runChildTurn(config, input, task, seq === 0 ? "initial" : `message:${seq}`);
    await recordOutcome({ config, input, seq, status: "done" });
    return "done";
  } catch (error) {
    await recordOutcome({
      config,
      input,
      seq,
      status: "failed",
      detail: error instanceof Error ? error.message : String(error),
    });
    return "failed";
  }
}

/** Process the inbox message at `seq` if it exists. */
async function consume(
  config: RetainedSubagentsConfig,
  input: RetainedInput,
  seq: number,
): Promise<"empty" | "skipped" | "superseded" | "done" | "failed"> {
  const key = inboxKey(input);
  const [stored] = await config.memory.getMessages(key, { fromSeq: seq, toSeq: seq });
  if (!stored) return "empty";
  if (stored.role !== "user") return "skipped";
  const command = stored.metadata as unknown as MailCommand | undefined;
  if (!command || typeof command.task !== "string") return "skipped";
  // A steer supersedes queued work that has not started yet. It takes effect
  // at the next turn boundary, never inside a tool call.
  if (command.mode !== "steer") {
    const later = await config.memory.getMessages(key, { fromSeq: seq + 1 });
    if (later.some((item) => "metadata" in item && item.metadata?.mode === "steer")) {
      await recordOutcome({ config, input, seq, status: "superseded" });
      return "superseded";
    }
  }
  return runAndRecord(config, input, command.task, seq);
}

async function runChildTurn(
  config: RetainedSubagentsConfig,
  input: RetainedInput,
  task: string,
  turnKey: string,
): Promise<void> {
  const recipe: RegisteredAgent | null = await config.agents.registry.get({
    id: input.agentId,
    version: input.agentVersion,
  });
  if (!recipe) throw new Error("Pinned child agent recipe unavailable");
  const agent = (await config.agents.resolve(recipe, input.parent)).withScope({
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
  });
  const thread = await agent.thread(childThreadId(input.childId), {
    metadata: {
      retainedParentAgentId: input.parent.agentId,
      retainedParentThreadId: input.parent.threadId,
      retainedDepth: input.depth,
    },
  });
  const result = await thread.send({ task }, { turnKey: `${input.childId}:${turnKey}` });
  await result.text;
}
