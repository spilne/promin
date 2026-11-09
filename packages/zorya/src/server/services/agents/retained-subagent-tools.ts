import { tool, type AgentTool, type Message, type ToolExecuteContext } from "@promin/agent";
import { z } from "zod";
import {
  RetainedSubagentError,
  type RetainedParent,
  type RetainedSubagents,
} from "./retained-subagents.ts";

/** Names of the tools `retainedSubagentTools` returns, for a recipe's tool list. */
export const RETAINED_SUBAGENT_TOOL_NAMES = [
  "spawn_subagent",
  "message_subagent",
  "list_subagents",
  "read_subagent",
  "stop_subagent",
] as const;

/** Longest message body, in characters, `read_subagent` returns to the model. */
const MAX_READ_CHARS = 4_000;
const DEFAULT_READ_MESSAGES = 6;

/** The calling turn's thread is the parent. The model cannot name another one. */
function parentOf(ctx: ToolExecuteContext | undefined): RetainedParent {
  const scope = ctx?.scope;
  if (!scope?.namespaceId || !scope.resourceId || !scope.threadId || !scope.agentId) {
    throw new RetainedSubagentError(
      "invalid",
      "Subagent tools need a complete caller scope (namespace, resource, thread, agent).",
    );
  }
  return {
    namespaceId: scope.namespaceId,
    resourceId: scope.resourceId,
    threadId: scope.threadId,
    agentId: scope.agentId,
  };
}

function render(message: Message): { role: string; content: string } {
  const raw =
    typeof message.content === "string" ? message.content : JSON.stringify(message.content);
  const content =
    raw.length > MAX_READ_CHARS ? `${raw.slice(0, MAX_READ_CHARS)}... [truncated]` : raw;
  return { role: message.role, content };
}

/**
 * Tools that let a model run retained subagents of its own thread: spawn,
 * message, list, read and stop. Every call acts on the calling turn's
 * thread (`ctx.scope`), so a model reaches only its own children. Spawning
 * goes through `RetainedSubagents.spawnAgent`, so `canDelegate`, the depth
 * and child limits and the parent's budgets all apply. A refusal is thrown
 * as `RetainedSubagentError` and reaches the model as the tool's error.
 *
 * Add the tools to the host's `resolveLocalAgent({ tools })` and list the
 * names (`RETAINED_SUBAGENT_TOOL_NAMES`) in the recipes that may delegate.
 */
export function retainedSubagentTools(
  retained: RetainedSubagents,
): Record<(typeof RETAINED_SUBAGENT_TOOL_NAMES)[number], AgentTool<any, any>> {
  return {
    spawn_subagent: tool({
      name: "spawn_subagent",
      description:
        "Start a long-lived subagent that works on a task in the background. Returns its " +
        "childId at once; use message_subagent to send more work and read_subagent to see results.",
      parameters: z.object({
        agentId: z.string().min(1).describe("Id of the agent to run as the subagent."),
        task: z.string().min(1).max(100_000).describe("The subagent's first task."),
      }),
      // A crash after the child starts leaves the outcome unknown: the model
      // gets that error and checks list_subagents before spawning again.
      execute: async (input, ctx) =>
        retained.spawnAgent({ parent: parentOf(ctx), agentId: input.agentId, task: input.task }),
    }),
    message_subagent: tool({
      name: "message_subagent",
      description:
        "Send a message to one of your subagents. mode 'queued' (default) and 'follow-up' run " +
        "after its current work; 'steer' drops queued messages and interrupts the turn it is running.",
      parameters: z.object({
        childId: z.string().min(1),
        task: z.string().min(1).max(100_000),
        mode: z.enum(["queued", "follow-up", "steer"]).optional(),
      }),
      // The tool call id dedupes the mailbox append, so a re-run sends once.
      idempotent: true,
      execute: async (input, ctx) =>
        retained.messageAgent({
          parent: parentOf(ctx),
          childId: input.childId,
          task: input.task,
          ...(input.mode && { mode: input.mode }),
          ...(ctx?.toolCallId && { messageId: ctx.toolCallId }),
        }),
    }),
    list_subagents: tool({
      name: "list_subagents",
      description: "List your subagents with their status and what they have spent.",
      parameters: z.object({}),
      idempotent: true,
      execute: async (_input, ctx) => ({
        children: await retained.listChildAgents(parentOf(ctx)),
      }),
    }),
    read_subagent: tool({
      name: "read_subagent",
      description:
        "Read a subagent's most recent transcript messages and the outcome (done, failed, " +
        "interrupted, superseded) of each message you sent it.",
      parameters: z.object({
        childId: z.string().min(1),
        lastMessages: z.number().int().min(1).max(50).optional(),
      }),
      idempotent: true,
      execute: async (input, ctx) => {
        const read = await retained.readAgentMessages(parentOf(ctx), input.childId);
        return {
          outcomes: read.outcomes.map(render),
          transcript: read.transcript
            .slice(-(input.lastMessages ?? DEFAULT_READ_MESSAGES))
            .map(render),
        };
      },
    }),
    stop_subagent: tool({
      name: "stop_subagent",
      description: "Stop one of your subagents. It stops accepting messages.",
      parameters: z.object({ childId: z.string().min(1) }),
      idempotent: true,
      execute: async (input, ctx) => {
        await retained.stopAgent(parentOf(ctx), input.childId);
        return { stopped: true };
      },
    }),
  };
}
