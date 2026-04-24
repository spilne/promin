// ---------------------------------------------------------------------------
// RunPollWatcher — polls workflow storage and publishes diffs to RunEventBus.
//
// Lives for the duration of an active SSE subscription. Polls at a fixed
// cadence, compares step snapshots, and emits { type: "step" } when a step
// changes or is added, and { type: "status" } on workflow status change.
// Shuts down when the workflow reaches a terminal status.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import type { Clock } from "@promin/core";
import { SystemClock } from "@promin/core";
import { runToDto, stepToDto } from "./serialize.ts";
import type { RunEventBus } from "./run-event-bus.ts";
import type { StepDto } from "./api-types.ts";

const TERMINAL_STATUSES = new Set(["completed", "failed"]);

export interface RunPollWatcherConfig {
  storage: WorkflowStorage;
  bus: RunEventBus;
  workflowId: string;
  /** Poll interval in ms. Default 1000. */
  intervalMs?: number;
  clock?: Clock;
}

export class RunPollWatcher {
  private readonly storage: WorkflowStorage;
  private readonly bus: RunEventBus;
  private readonly workflowId: string;
  private readonly intervalMs: number;
  private readonly clock: Clock;
  private handle?: ReturnType<Clock["setInterval"]>;
  private lastSteps = new Map<string, StepDto>();
  private lastStatus?: string;

  constructor(config: RunPollWatcherConfig) {
    this.storage = config.storage;
    this.bus = config.bus;
    this.workflowId = config.workflowId;
    this.intervalMs = config.intervalMs ?? 1000;
    this.clock = config.clock ?? SystemClock;
  }

  /** Emit an initial snapshot and start polling. Returns initial RunDto. */
  async start(): Promise<void> {
    const state = await this.storage.loadWorkflow(this.workflowId);
    if (!state) return;
    const dto = runToDto(state);
    this.bus.publish(this.workflowId, { type: "snapshot", run: dto });
    for (const s of dto.steps) this.lastSteps.set(s.stepName, s);
    this.lastStatus = dto.status;
    if (TERMINAL_STATUSES.has(dto.status)) {
      this.bus.publish(this.workflowId, { type: "end" });
      return;
    }
    this.handle = this.clock.setInterval(() => {
      void this.tick();
    }, this.intervalMs);
  }

  stop(): void {
    this.handle?.clear();
    this.handle = undefined;
  }

  private async tick(): Promise<void> {
    if (!this.bus.hasListeners(this.workflowId)) {
      this.stop();
      return;
    }
    const state = await this.storage.loadWorkflow(this.workflowId);
    if (!state) return;

    // Emit step diffs.
    for (const s of Object.values(state.steps)) {
      const dto = stepToDto(s);
      const prev = this.lastSteps.get(dto.stepName);
      if (!prev || !stepEqual(prev, dto)) {
        this.bus.publish(this.workflowId, { type: "step", stepName: dto.stepName, step: dto });
        this.lastSteps.set(dto.stepName, dto);
      }
    }

    // Emit status change.
    if (state.status !== this.lastStatus) {
      this.lastStatus = state.status;
      this.bus.publish(this.workflowId, { type: "status", status: state.status });
    }

    if (TERMINAL_STATUSES.has(state.status)) {
      this.bus.publish(this.workflowId, { type: "end" });
      this.stop();
    }
  }
}

function stepEqual(a: StepDto, b: StepDto): boolean {
  return (
    a.status === b.status &&
    a.attempt === b.attempt &&
    a.durationMs === b.durationMs &&
    a.completedAt === b.completedAt &&
    a.wakeAt === b.wakeAt &&
    a.signalName === b.signalName
  );
}

export function isTerminalStatus(status: string): boolean {
  return TERMINAL_STATUSES.has(status);
}
