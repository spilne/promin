import { z } from "zod";
import { tool } from "../tool.ts";
import type { Scheduler, ScheduleTick } from "@promin/workflow";

export interface SchedulerToolsConfig {
  /**
   * The scheduler instance — typically `new InMemoryScheduler()` for single-process use,
   * or `createDurableScheduler({ storage })` for persistent multi-process deployments.
   */
  scheduler: Scheduler;
  /**
   * Called each time a scheduled task fires. Wire this to re-enter the agent:
   *
   *   onTick: (task) => session.send(task)
   *
   * The `task` string is whatever the LLM passed when it called `scheduleTask`.
   */
  onTick: (task: string, tick: ScheduleTick) => void;
}

/**
 * Creates three tools that let the LLM manage a durable task scheduler.
 *
 * Internally subscribes to `scheduler.subscribe()` in the background so ticks
 * are delivered to `onTick` without the caller needing to manage the stream.
 *
 * Usage:
 *
 *   const scheduler = new InMemoryScheduler();
 *
 *   agentLoop({
 *     tools: {
 *       ...createSchedulerTools({
 *         scheduler,
 *         onTick: (task) => session.send(task),
 *       }),
 *     },
 *   });
 */
export function createSchedulerTools(config: SchedulerToolsConfig) {
  // Subscribe once — the scheduler delivers ticks for all schedules, including those
  // registered after this call (InMemoryScheduler uses a poll loop; DurableScheduler polls storage).
  config.scheduler.subscribe().forEach((tick) => {
    const task = tick.metadata?.task;
    if (typeof task === "string") config.onTick(task, tick);
  });

  return {
    scheduleTask: tool({
      name: "scheduleTask",
      description:
        "Schedule a recurring or one-shot task. " +
        "Use a cron expression for calendar-based schedules (e.g. '0 9 * * *' for daily at 9am), " +
        "or intervalMs for fixed intervals (e.g. 60000 for every minute). " +
        "The task string is replayed back to you when the schedule fires.",
      parameters: z
        .object({
          id: z
            .string()
            .describe("Unique schedule ID — use a short descriptive name, e.g. 'daily-report'"),
          task: z
            .string()
            .describe("The task to run when the schedule fires (replayed as a new message)"),
          cron: z.string().optional().describe("Cron expression, e.g. '0 9 * * *' (daily 9am UTC)"),
          intervalMs: z
            .number()
            .int()
            .positive()
            .optional()
            .describe("Fixed interval in milliseconds"),
          timezone: z
            .string()
            .optional()
            .describe("IANA timezone for cron evaluation, e.g. 'America/New_York'"),
        })
        .refine((v) => v.cron !== undefined || v.intervalMs !== undefined, {
          message: "Provide either cron or intervalMs",
        }),
      execute: async ({ id, task, cron, intervalMs, timezone }) => {
        config.scheduler.register({
          id,
          ...(cron ? { cron } : {}),
          ...(intervalMs ? { intervalMs } : {}),
          ...(timezone ? { timezone } : {}),
          metadata: { task },
        });
        const schedule = cron
          ? `cron(${cron}${timezone ? ` ${timezone}` : ""})`
          : `every ${intervalMs}ms`;
        return `Scheduled "${id}": ${schedule}`;
      },
    }),

    listSchedules: tool({
      name: "listSchedules",
      description: "List all registered schedules and their configurations.",
      parameters: z.object({}),
      execute: async () => {
        const schedules = config.scheduler.list();
        if (!schedules.length) return "No schedules registered.";
        return schedules
          .map((s) => {
            const trigger = s.cron ?? (s.intervalMs ? `every ${s.intervalMs}ms` : "unknown");
            const status = s.enabled === false ? " [paused]" : "";
            const task = s.metadata?.task ?? "(no task)";
            return `${s.id}${status}: ${trigger} → "${task}"`;
          })
          .join("\n");
      },
    }),

    cancelSchedule: tool({
      name: "cancelSchedule",
      description: "Cancel and remove a scheduled task by its ID.",
      parameters: z.object({
        id: z.string().describe("The schedule ID to cancel"),
      }),
      execute: async ({ id }) => {
        config.scheduler.unregister(id);
        return `Cancelled schedule "${id}".`;
      },
    }),
  };
}
