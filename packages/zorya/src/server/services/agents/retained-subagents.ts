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
  /** Explicit capability policy. A parent cannot delegate until the host grants it. */
  readonly canDelegate: (parentAgentId: string, childAgentId: string) => boolean | Promise<boolean>;
  readonly maxDepth?: number;
  readonly maxChildren?: number;
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
        yield* ctx.activity("initial", () => runChildTurn(service, input, input.task, "initial"), {
          idempotent: true,
        });
        for (let seq = 1; ; seq++) {
          yield* ctx.signal(`inbox:${seq}`);
          yield* ctx.activity(
            `message:${seq}`,
            async () => {
              const key = inboxKey(input);
              const [stored] = await service.memory.getMessages(key, { fromSeq: seq, toSeq: seq });
              if (!stored || stored.role !== "user") return;
              const command = stored.metadata as unknown as MailCommand | undefined;
              if (!command || typeof command.task !== "string") return;
              // A steer supersedes queued work that has not started yet. It
              // takes effect at the next turn boundary, never inside a tool call.
              if (command.mode !== "steer") {
                const later = await service.memory.getMessages(key, { fromSeq: seq + 1 });
                if (later.some((item) => "metadata" in item && item.metadata?.mode === "steer"))
                  return;
              }
              await runChildTurn(service, input, command.task, `message:${seq}`);
            },
            { idempotent: true },
          );
        }
      })
      .build();
    config.workflows.define(this.definition as Workflow<unknown, unknown>, { internal: true });
  }

  async start(): Promise<void> {
    await this.reconcileMailboxes();
  }
  async stop(): Promise<void> {}

  async spawnAgent(params: {
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
      if (!params.task.trim()) throw new Error("Subagent task must not be empty");
      const parentThread = await this.ownedParent(parent);
      if (!(await this.config.canDelegate(parent.agentId, params.agentId))) {
        throw new Error("Subagent capability denied");
      }
      const depthRaw = (await parentThread.metadata()).retainedDepth;
      const depth = (typeof depthRaw === "number" ? depthRaw : 0) + 1;
      if (depth > (this.config.maxDepth ?? 3)) throw new Error("Subagent depth limit exceeded");
      const children = await this.listChildAgents(parent);
      if (
        children.filter((child) => !["completed", "failed", "cancelled"].includes(child.status))
          .length >= (this.config.maxChildren ?? 8)
      )
        throw new Error("Subagent child limit exceeded");
      const recipe = await this.config.agents.registry.get({ id: params.agentId });
      if (!recipe || !recipe.enabled) throw new Error("Child agent unavailable");
      const childId = params.childId ?? randomUUID();
      const workflowId = `retained-agent:${childId}`;
      if (await this.config.workflows.storage.loadWorkflow(workflowId)) {
        throw new Error("Child id already exists");
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
      if (admitted?.input && JSON.stringify(admitted.input) !== JSON.stringify(input)) {
        throw new Error("Child id already exists");
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
    return states.map((state) => {
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
    if (state && ["completed", "failed", "cancelled"].includes(state.status)) {
      throw new Error("Child agent has stopped");
    }
    if (!params.task.trim()) throw new Error("Subagent message must not be empty");
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
      throw new Error("Subagent message id conflict");
    }
    await this.config.workflows.storage.deliverSignal({
      workflowId: workflowIdOf(params.childId),
      signalName: `inbox:${stored.seq}`,
      payload: messageId,
    });
    return { seq: stored.seq };
  }

  async readAgentMessages(
    parent: RetainedParent,
    childId: string,
  ): Promise<{
    readonly inbox: Message[];
    readonly transcript: Message[];
  }> {
    const input = await this.ownedChild(parent, childId);
    const inbox = await this.config.memory.getMessages(inboxKey(input));
    const transcript = await this.config.memory.getMessages(childKey(input));
    return { inbox, transcript };
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
    if (!lock.acquired) throw new Error("Child agent busy; retry cleanup");
    await this.config.workflows.storage.releaseLock({
      workflowId,
      ...(lock.token !== undefined && { guard: { fenceToken: lock.token } }),
    });
    await this.config.memory.deleteThread(inboxKey(input));
    await this.config.memory.deleteThread(childKey(input));
  }

  /** Re-deliver wakes after a crash between mailbox append and signal write. */
  async reconcileMailboxes(): Promise<void> {
    const states = await this.config.workflows.storage.listWorkflows({
      name: RETAINED_AGENT_WORKFLOW,
    });
    for (const state of states) {
      if (["completed", "failed", "cancelled"].includes(state.status)) continue;
      const input = state.input as RetainedInput;
      for (const message of await this.config.memory.getMessages(inboxKey(input))) {
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
    if (!recipe) throw new Error("Parent agent unavailable");
    const agent = (await this.config.agents.resolve(recipe, parent)).withScope(parent);
    const thread = await agent.thread(parent.threadId, { createIfMissing: false });
    if ((await thread.metadata())[THREAD_AGENT_METADATA_KEY] !== parent.agentId) {
      throw new Error("Parent thread not owned by agent");
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
      state.metadata?.resourceId !== parent.resourceId
    )
      throw new Error("Child agent not found");
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
function childKey(input: RetainedInput) {
  return {
    namespaceId: input.parent.namespaceId,
    resourceId: input.parent.resourceId,
    threadId: childThreadId(input.childId),
  };
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
