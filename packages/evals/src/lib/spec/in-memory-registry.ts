// ---------------------------------------------------------------------------
// InMemoryEvalSpecRegistry — the default, process-local EvalSpecRegistry.
//
// Version-keyed (one entry per (id, version)); `register` upserts and
// preserves `createdAt` across version re-writes.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  DEFAULT_EVAL_SPEC_VERSION,
  type EvalSpec,
  type EvalSpecRegistry,
  type RegisterEvalSpecInput,
} from "./types.ts";

export interface InMemoryEvalSpecRegistryConfig {
  readonly clock?: Clock;
}

export class InMemoryEvalSpecRegistry implements EvalSpecRegistry {
  private readonly specs = new Map<string, EvalSpec>();
  private readonly clock: Clock;

  constructor(config: InMemoryEvalSpecRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  async register(input: RegisterEvalSpecInput): Promise<EvalSpec> {
    const version = input.version ?? DEFAULT_EVAL_SPEC_VERSION;
    const key = `${input.id}::${version}`;
    const now = this.clock.currentTimeMs();
    const existing = this.specs.get(key);
    const spec: EvalSpec = {
      id: input.id,
      version,
      ...(input.description !== undefined && { description: input.description }),
      dataset: input.dataset,
      targets: input.targets,
      scorers: input.scorers,
      ...(input.samplesPerCase !== undefined && { samplesPerCase: input.samplesPerCase }),
      ...(input.concurrency !== undefined && { concurrency: input.concurrency }),
      ...(input.passThreshold !== undefined && { passThreshold: input.passThreshold }),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.specs.set(key, spec);
    return spec;
  }

  async get(id: string, version?: string): Promise<EvalSpec | null> {
    if (version !== undefined) {
      return this.specs.get(`${id}::${version}`) ?? null;
    }
    let latest: EvalSpec | null = null;
    for (const spec of this.specs.values()) {
      if (spec.id === id && (latest === null || isNewer(spec, latest))) {
        latest = spec;
      }
    }
    return latest;
  }

  async list(): Promise<EvalSpec[]> {
    const latest = new Map<string, EvalSpec>();
    for (const spec of this.specs.values()) {
      const current = latest.get(spec.id);
      if (current === undefined || isNewer(spec, current)) {
        latest.set(spec.id, spec);
      }
    }
    return [...latest.values()].sort((a, b) => a.createdAt - b.createdAt);
  }

  async versions(id: string): Promise<EvalSpec[]> {
    return [...this.specs.values()]
      .filter((spec) => spec.id === id)
      .sort((a, b) => a.createdAt - b.createdAt);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      this.specs.delete(`${id}::${version}`);
      return;
    }
    for (const [key, spec] of this.specs) {
      if (spec.id === id) this.specs.delete(key);
    }
  }
}

/**
 * Which of two specs is the more recent. Primary key is `updatedAt`; ties
 * (same-millisecond writes) break on `version` descending, so "latest" is
 * deterministic rather than dependent on map iteration order.
 */
function isNewer(a: EvalSpec, b: EvalSpec): boolean {
  if (a.updatedAt !== b.updatedAt) return a.updatedAt > b.updatedAt;
  return a.version > b.version;
}
