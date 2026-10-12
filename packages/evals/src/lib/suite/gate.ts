// ---------------------------------------------------------------------------
// Promotion gates — "a version of this agent can be made active only once
// eval suite X scored at least T on that very version".
//
// A gate is stored per agent id in a `VersionedRegistry` of kind
// `EVAL_GATE_KIND` (so policy changes keep a history like everything else
// operators edit). `checkEvalGate` answers whether a version passes.
//
// Which runs count: only runs of ONE suite version — the gate's pinned
// `suiteVersion`, else the suite's version that is active when the gate is
// checked (a draft suite version, never promoted, never satisfies a gate).
// Of those runs on the agent version, the NEWEST completed one decides: it
// must score at least `minScore`. An older passing run does not count once
// a newer one fails, so a flaky suite re-run until it passes is visible in
// the run history — but re-running is not prevented; make suites
// deterministic enough to gate on (`samplesPerCase`, `passThreshold`).
// ---------------------------------------------------------------------------

import {
  RegistryInputError,
  type RegistryKind,
  type Versioned,
  type VersionedRegistry,
} from "@promin/agent";
import type { EvalRunRecord, EvalRunStore } from "../storage/types.ts";

/** A gate's policy; the registry id is the agent id it guards. */
export interface EvalGate {
  readonly suiteId: string;
  /**
   * Only runs of this suite version count. Default: the suite's version
   * that is active when the gate is checked.
   */
  readonly suiteVersion?: string;
  /**
   * Informational: the suite's active version when an unpinned gate was
   * set. The gate is still evaluated against the active version at check
   * time.
   */
  readonly suiteVersionAtSet?: string;
  /** Lowest passing score, 0..1. */
  readonly minScore: number;
  /**
   * Score a run by this scorer's mean value. Default: the run's case
   * pass-rate (fraction of cases that passed).
   */
  readonly scorerId?: string;
}

export interface EvalGateFilter {
  readonly suiteId?: string;
}

export type RegisteredEvalGate = Versioned<EvalGate>;

/** A `VersionedRegistry` of promotion gates, keyed by agent id. */
export type EvalGateRegistry = VersionedRegistry<EvalGate, EvalGateFilter, EvalGate>;

export const EVAL_GATE_KIND: RegistryKind<EvalGate, EvalGateFilter, EvalGate> = {
  name: "eval-gate",
  prepare: ({ id, input }) => {
    const fail = (message: string): never => {
      throw new RegistryInputError(`eval gate "${id}": ${message}`);
    };
    if (typeof input !== "object" || input === null) fail("body must be an object");
    if (typeof input.suiteId !== "string" || input.suiteId.length === 0) {
      fail("suiteId must be a non-empty string");
    }
    if (
      typeof input.minScore !== "number" ||
      !Number.isFinite(input.minScore) ||
      input.minScore < 0 ||
      input.minScore > 1
    ) {
      fail("minScore must be in 0..1");
    }
    for (const field of ["suiteVersion", "suiteVersionAtSet", "scorerId"] as const) {
      const value = input[field];
      if (value !== undefined && (typeof value !== "string" || value.length === 0)) {
        fail(`${field} must be a non-empty string`);
      }
    }
    return {
      suiteId: input.suiteId,
      ...(input.suiteVersion !== undefined && { suiteVersion: input.suiteVersion }),
      ...(input.suiteVersionAtSet !== undefined && { suiteVersionAtSet: input.suiteVersionAtSet }),
      minScore: input.minScore,
      ...(input.scorerId !== undefined && { scorerId: input.scorerId }),
    };
  },
  facets: ({ body }) => [`suite:${body.suiteId}`],
  query: (filter) => ({
    include: filter.suiteId !== undefined ? [`suite:${filter.suiteId}`] : [],
    exclude: [],
  }),
};

/** The outcome of checking one agent version against its gate. */
export interface EvalGateVerdict {
  readonly passed: boolean;
  /** The newest completed run of the gate's suite on the version, if any. */
  readonly latest: EvalRunRecord | null;
  /** That run's score under the gate's metric. */
  readonly score: number | null;
  /** Why a version failed, for operators. */
  readonly reason?: string;
}

/** A run's score under a gate: the scorer's mean value, or the case pass-rate. */
export function evalGateScore(params: {
  readonly gate: EvalGate;
  readonly run: EvalRunRecord;
}): number | null {
  const { gate, run } = params;
  if (run.totals === undefined) return null;
  if (gate.scorerId === undefined) return run.totals.passRate;
  return run.totals.perScorer[gate.scorerId]?.meanValue ?? null;
}

/**
 * Whether `agentId@version` passes `gate`: the newest completed run of the
 * gate's suite version (pinned, else `activeSuiteVersion`) on that agent
 * version must score at least `minScore`. Without either suite version
 * (the suite has no active version) nothing passes.
 */
export async function checkEvalGate(params: {
  readonly gate: EvalGate;
  readonly runs: EvalRunStore;
  readonly agentId: string;
  readonly version: string;
  /** The suite's active version now; used when the gate pins none. */
  readonly activeSuiteVersion: string | null;
}): Promise<EvalGateVerdict> {
  const { gate, agentId, version } = params;
  const suiteVersion = gate.suiteVersion ?? params.activeSuiteVersion;
  if (suiteVersion === null) {
    return {
      passed: false,
      latest: null,
      score: null,
      reason: `eval suite "${gate.suiteId}" has no active version to judge ${agentId}@${version} by`,
    };
  }
  const [latest] = await params.runs.list({
    targetId: agentId,
    targetVersion: version,
    suiteId: gate.suiteId,
    suiteVersion,
    status: "completed",
    limit: 1,
  });
  const suite = `${gate.suiteId}@${suiteVersion}`;
  if (latest === undefined) {
    return {
      passed: false,
      latest: null,
      score: null,
      reason: `${agentId}@${version} has no completed run of eval suite "${suite}"`,
    };
  }
  const score = evalGateScore({ gate, run: latest });
  const metric = gate.scorerId !== undefined ? `scorer "${gate.scorerId}"` : "pass rate";
  if (score === null) {
    return {
      passed: false,
      latest,
      score: null,
      reason: `run ${latest.runId} has no ${metric} score`,
    };
  }
  if (score < gate.minScore) {
    return {
      passed: false,
      latest,
      score,
      reason: `run ${latest.runId} of "${suite}" scored ${metric} ${score.toFixed(3)} < ${gate.minScore}`,
    };
  }
  return { passed: true, latest, score };
}
