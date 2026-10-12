// ---------------------------------------------------------------------------
// EVAL_SUITE_KIND — eval suites on the generic `VersionedRegistry`.
//
// `prepare` normalises (metadata defaults, undefined fields dropped so the
// body hashes the same however it was written) and validates: a malformed
// dataset, an unknown scorer kind or an out-of-range knob never enters the
// store (`RegistryInputError`, a 400 over HTTP).
// ---------------------------------------------------------------------------

import { RegistryInputError, type RegistryKind } from "@promin/agent";
import { resolveEvalOptions } from "../runner.ts";
import {
  SCORER_SPEC_KINDS,
  type DatasetSpec,
  type EvalSuite,
  type EvalSuiteFilter,
  type EvalSuiteInput,
  type ScorerSpec,
} from "./types.ts";

export const EVAL_SUITE_KIND: RegistryKind<EvalSuite, EvalSuiteFilter, EvalSuiteInput> = {
  name: "eval-suite",
  prepare: ({ id, input }) => {
    const fail = (message: string): never => {
      throw new RegistryInputError(`eval suite "${id}": ${message}`);
    };
    if (typeof input !== "object" || input === null) fail("body must be an object");
    const dataset = prepareDataset({ raw: input.dataset, fail });
    if (!Array.isArray(input.scorers) || input.scorers.length === 0) {
      fail("scorers must be a non-empty array");
    }
    const scorers = input.scorers.map((raw, i) =>
      prepareScorer({ raw, fail, at: `scorers[${i}]` }),
    );
    const scorerIds = scorers.map((s) => s.id ?? s.kind);
    const duplicate = scorerIds.find((sid, i) => scorerIds.indexOf(sid) !== i);
    if (duplicate !== undefined) fail(`two scorers share the id "${duplicate}"; set "id" on one`);
    try {
      resolveEvalOptions(input);
    } catch (err) {
      fail(err instanceof Error ? err.message : String(err));
    }
    if (input.idempotentCases !== undefined && typeof input.idempotentCases !== "boolean") {
      fail("idempotentCases must be a boolean");
    }
    const allowedTools =
      input.allowedTools !== undefined
        ? stringList({ raw: input.allowedTools, fail, at: "allowedTools" })
        : undefined;
    const description = input.description ?? null;
    if (description !== null && typeof description !== "string") {
      fail("description must be a string");
    }
    return {
      description,
      dataset,
      scorers,
      ...(input.samplesPerCase !== undefined && { samplesPerCase: input.samplesPerCase }),
      ...(input.concurrency !== undefined && { concurrency: input.concurrency }),
      ...(input.passThreshold !== undefined && { passThreshold: input.passThreshold }),
      ...(input.caseTimeoutMs !== undefined && { caseTimeoutMs: input.caseTimeoutMs }),
      ...(input.idempotentCases !== undefined && { idempotentCases: input.idempotentCases }),
      ...(allowedTools !== undefined && { allowedTools }),
      metadata: {
        tags: stringList({ raw: input.metadata?.tags, fail, at: "metadata.tags" }),
        agentIds: stringList({ raw: input.metadata?.agentIds, fail, at: "metadata.agentIds" }),
      },
    };
  },
  facets: ({ body }) => [
    ...body.metadata.tags.map((t) => `tag:${t}`),
    ...body.metadata.agentIds.map((a) => `agent:${a}`),
  ],
  query: (filter) => ({
    include: [
      ...(filter.tag !== undefined ? [`tag:${filter.tag}`] : []),
      ...(filter.agentId !== undefined ? [`agent:${filter.agentId}`] : []),
    ],
    exclude: [],
  }),
};

type Fail = (message: string) => never;

function prepareDataset(params: { raw: unknown; fail: Fail }): DatasetSpec {
  const { fail } = params;
  const raw = params.raw as Record<string, unknown> | null | undefined;
  if (typeof raw !== "object" || raw === null) return fail("dataset must be an object");
  switch (raw.kind) {
    case "inline": {
      if (!isNonEmptyString(raw.id)) return fail("an inline dataset needs an id");
      if (!Array.isArray(raw.cases) || raw.cases.length === 0) {
        return fail("an inline dataset needs a non-empty cases array");
      }
      const ids = new Set<string>();
      const cases = raw.cases.map((c: unknown, i: number) => {
        const evalCase = c as Record<string, unknown> | null;
        if (typeof evalCase !== "object" || evalCase === null || !isNonEmptyString(evalCase.id)) {
          return fail(`dataset.cases[${i}] needs a string id`);
        }
        if (!("input" in evalCase)) return fail(`dataset.cases[${i}] needs an input`);
        if (ids.has(evalCase.id)) return fail(`two cases share the id "${evalCase.id}"`);
        ids.add(evalCase.id);
        return {
          id: evalCase.id,
          input: evalCase.input,
          ...(evalCase.expected !== undefined && { expected: evalCase.expected }),
          ...(evalCase.tags !== undefined && {
            tags: stringList({ raw: evalCase.tags, fail, at: `dataset.cases[${i}].tags` }),
          }),
          ...(evalCase.metadata !== undefined && {
            metadata: evalCase.metadata as Readonly<Record<string, unknown>>,
          }),
        };
      });
      return { kind: "inline", id: raw.id, cases };
    }
    case "stored":
      if (!isNonEmptyString(raw.datasetId)) return fail("a stored dataset needs a datasetId");
      return { kind: "stored", datasetId: raw.datasetId };
    case "jsonl":
      if (!isNonEmptyString(raw.path)) return fail("a jsonl dataset needs a path");
      return {
        kind: "jsonl",
        path: raw.path,
        ...(isNonEmptyString(raw.id) && { id: raw.id }),
      };
    default:
      return fail(`unknown dataset kind ${JSON.stringify(raw.kind)}`);
  }
}

function prepareScorer(params: { raw: unknown; fail: Fail; at: string }): ScorerSpec {
  const { at } = params;
  const fail = (message: string): never => params.fail(`${at}: ${message}`);
  const raw = params.raw as Record<string, unknown> | null;
  if (typeof raw !== "object" || raw === null) return fail("must be an object");
  const kind = raw.kind as ScorerSpec["kind"];
  if (!SCORER_SPEC_KINDS.includes(kind)) return fail(`unknown scorer kind ${JSON.stringify(kind)}`);
  if (raw.id !== undefined && !isNonEmptyString(raw.id)) fail("id must be a non-empty string");
  if (raw.threshold !== undefined && !isUnit(raw.threshold)) fail("threshold must be in 0..1");
  if (raw.required !== undefined && typeof raw.required !== "boolean") {
    fail("required must be a boolean");
  }
  const base = {
    ...(raw.id !== undefined && { id: raw.id as string }),
    ...(raw.threshold !== undefined && { threshold: raw.threshold as number }),
    ...(raw.required !== undefined && { required: raw.required as boolean }),
  };
  switch (kind) {
    case "exactMatch":
      return { kind, ...base };
    case "regexMatch": {
      if (!isNonEmptyString(raw.pattern)) return fail("regexMatch needs a pattern");
      if (raw.flags !== undefined && typeof raw.flags !== "string") fail("flags must be a string");
      try {
        new RegExp(raw.pattern, raw.flags as string | undefined);
      } catch (err) {
        fail(`invalid pattern: ${err instanceof Error ? err.message : String(err)}`);
      }
      return {
        kind,
        ...base,
        pattern: raw.pattern,
        ...(raw.flags !== undefined && { flags: raw.flags as string }),
      };
    }
    case "jsonMatch":
      if (raw.exact !== undefined && typeof raw.exact !== "boolean")
        fail("exact must be a boolean");
      return { kind, ...base, ...(raw.exact !== undefined && { exact: raw.exact as boolean }) };
    case "llmJudge":
      if (!isNonEmptyString(raw.judgeRef)) return fail("llmJudge needs a judgeRef");
      if (!isNonEmptyString(raw.rubric)) return fail("llmJudge needs a rubric");
      return { kind, ...base, judgeRef: raw.judgeRef, rubric: raw.rubric };
    case "toolTrajectory": {
      const expected = stringList({ raw: raw.expected, fail, at: "expected" });
      if (raw.mode !== undefined && !["exact", "ordered", "set"].includes(raw.mode as string)) {
        fail('mode must be "exact", "ordered" or "set"');
      }
      return {
        kind,
        ...base,
        expected,
        ...(raw.mode !== undefined && { mode: raw.mode as "exact" | "ordered" | "set" }),
      };
    }
    case "budget": {
      const limits: Record<string, number> = {};
      for (const field of ["maxLatencyMs", "maxCostUsd", "maxTotalTokens"] as const) {
        const value = raw[field];
        if (value === undefined) continue;
        if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
          fail(`${field} must be a non-negative number`);
        }
        limits[field] = value as number;
      }
      if (Object.keys(limits).length === 0) fail("budget needs at least one limit");
      return { kind, ...base, ...limits };
    }
    case "retrievalRecall":
      return {
        kind,
        ...base,
        ...(raw.toolNames !== undefined && {
          toolNames: stringList({ raw: raw.toolNames, fail, at: "toolNames" }),
        }),
      };
    case "setMatch":
      if (
        raw.metric !== undefined &&
        !["precision", "recall", "f1"].includes(raw.metric as string)
      ) {
        fail('metric must be "precision", "recall" or "f1"');
      }
      return {
        kind,
        ...base,
        ...(raw.metric !== undefined && { metric: raw.metric as "precision" | "recall" | "f1" }),
      };
  }
}

function stringList(params: { raw: unknown; fail: Fail; at: string }): string[] {
  if (params.raw === undefined) return [];
  if (!Array.isArray(params.raw) || !params.raw.every(isNonEmptyString)) {
    return params.fail(`${params.at} must be an array of non-empty strings`);
  }
  return [...params.raw];
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isUnit(value: unknown): boolean {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1;
}
