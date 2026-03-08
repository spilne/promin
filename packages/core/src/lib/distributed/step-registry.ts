// ---------------------------------------------------------------------------
// StepRegistry — maps step names to Pipeline implementations on workers
//
// Workers register the steps they can execute. The coordinator dispatches
// steps by name; workers resolve them from their local registry.
// ---------------------------------------------------------------------------

import type { Pipeline, TaggedError } from "../pipeline.ts";

export interface StepContext {
  readonly input: unknown;
  readonly prev: unknown;
  readonly deps: Record<string, unknown>;
  readonly workflowId: string;
  readonly stepName: string;
  readonly attempt: number;
}

export type StepHandler = (ctx: StepContext) => Pipeline<unknown, TaggedError> | Promise<unknown>;

export interface StepRegistry {
  register(stepName: string, handler: StepHandler): void;
  resolve(stepName: string): StepHandler | undefined;
  has(stepName: string): boolean;
  list(): string[];
}

export class MapStepRegistry implements StepRegistry {
  private readonly steps = new Map<string, StepHandler>();

  register(stepName: string, handler: StepHandler): void {
    this.steps.set(stepName, handler);
  }

  resolve(stepName: string): StepHandler | undefined {
    return this.steps.get(stepName);
  }

  has(stepName: string): boolean {
    return this.steps.has(stepName);
  }

  list(): string[] {
    return [...this.steps.keys()];
  }
}
