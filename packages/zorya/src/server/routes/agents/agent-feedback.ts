import { buildAgentTrace } from "@promin/agent";
import type { EvalDatasetStore } from "@promin/evals";
import { z } from "zod";
import { json, jsonError, readJson } from "../../router.ts";
import type { AgentGatewayDeps } from "./agent-gateway-deps.ts";
import {
  loadScopedAgent,
  openThreadFromBody,
  openThreadFromQuery,
  scopeFromQuery,
} from "./agent-scope.ts";

const FeedbackSchema = z.object({
  namespaceId: z.string(),
  resourceId: z.string().optional(),
  ownerId: z.string().optional(),
  workflowId: z.string().min(1).max(200),
  rating: z.enum(["positive", "negative"]),
  comment: z.string().max(2000).optional(),
  expected: z.string().min(1).max(20_000).optional(),
});

/** A stable dataset per tenant and agent, usable by a stored eval suite. */
export function feedbackDatasetId(
  namespaceId: string,
  agentId: string,
  resourceId: string,
): string {
  return `agent-feedback:${encodeURIComponent(namespaceId)}:${encodeURIComponent(agentId)}:${encodeURIComponent(resourceId)}`;
}

export function regressionDatasetId(
  namespaceId: string,
  agentId: string,
  resourceId: string,
): string {
  return `agent-regression:${encodeURIComponent(namespaceId)}:${encodeURIComponent(agentId)}:${encodeURIComponent(resourceId)}`;
}

export function postAgentFeedback(deps: AgentGatewayDeps, datasets: EvalDatasetStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const raw = await readJson<unknown>(req);
    const parsed = FeedbackSchema.safeParse(raw);
    if (!parsed.success) return jsonError(400, "invalid_feedback");
    const opened = await openThreadFromBody({ deps, req, params, body: parsed.data });
    if (opened instanceof Response) return opened;
    const { thread, scoped } = opened;
    const messages = await thread.messages({ limit: 10_000 });
    const turn = buildAgentTrace(messages).turns.find(
      (item) => item.timing?.workflowId === parsed.data.workflowId,
    );
    if (!turn) return jsonError(404, "turn_not_found");
    const input = turn.children.find((item) => item.kind === "user");
    if (!input || input.kind !== "user") return jsonError(409, "turn_has_no_input");
    const datasetId = feedbackDatasetId(scoped.namespaceId, scoped.recipe.id, scoped.resourceId);
    const regressionId = regressionDatasetId(
      scoped.namespaceId,
      scoped.recipe.id,
      scoped.resourceId,
    );
    const caseId = `${thread.id}:${parsed.data.workflowId}`;
    await datasets.appendCase(datasetId, {
      id: caseId,
      input: input.content,
      ...(parsed.data.expected !== undefined && { expected: parsed.data.expected }),
      tags: [parsed.data.rating, ...(parsed.data.expected ? ["regression-case"] : [])],
      metadata: {
        agentId: scoped.recipe.id,
        agentVersion: scoped.recipe.version,
        namespaceId: scoped.namespaceId,
        resourceId: scoped.resourceId,
        threadId: thread.id,
        workflowId: parsed.data.workflowId,
        rating: parsed.data.rating,
        ...(parsed.data.comment !== undefined && { comment: parsed.data.comment }),
        ratedAt: Date.now(),
      },
    });
    if (parsed.data.rating === "negative" && parsed.data.expected) {
      await datasets.appendCase(regressionId, {
        id: caseId,
        input: input.content,
        expected: parsed.data.expected,
        tags: ["operator-regression"],
        metadata: {
          agentId: scoped.recipe.id,
          agentVersion: scoped.recipe.version,
          namespaceId: scoped.namespaceId,
          resourceId: scoped.resourceId,
          threadId: thread.id,
          workflowId: parsed.data.workflowId,
        },
      });
    }
    return json(201, { caseId, datasetId, regressionDatasetId: regressionId });
  };
}

export function getAgentFeedback(deps: AgentGatewayDeps, datasets: EvalDatasetStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const datasetId = feedbackDatasetId(
      opened.scoped.namespaceId,
      opened.scoped.recipe.id,
      opened.scoped.resourceId,
    );
    const cases = (await datasets.get(datasetId)) ?? [];
    const feedback = cases.filter((item) => item.metadata?.threadId === opened.thread.id);
    return json(200, { datasetId, feedback });
  };
}

/** Resource-scoped operator feedback trend, newest day first. */
export function getAgentFeedbackTrend(deps: AgentGatewayDeps, datasets: EvalDatasetStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    if (!params.id) return jsonError(400, "missing_id");
    const scope = scopeFromQuery(new URL(req.url));
    if ("error" in scope) return jsonError(400, scope.error);
    const scoped = await loadScopedAgent({ deps, req, id: params.id, scope, invoking: false });
    if (scoped instanceof Response) return scoped;
    const datasetId = feedbackDatasetId(scoped.namespaceId, scoped.recipe.id, scoped.resourceId);
    const cases = (await datasets.get(datasetId)) ?? [];
    const days = new Map<string, { positive: number; negative: number }>();
    for (const item of cases) {
      const at = item.metadata?.ratedAt;
      const day = new Date(typeof at === "number" ? at : 0).toISOString().slice(0, 10);
      const row = days.get(day) ?? { positive: 0, negative: 0 };
      if (item.metadata?.rating === "positive") row.positive++;
      if (item.metadata?.rating === "negative") row.negative++;
      days.set(day, row);
    }
    const trend = [...days]
      .sort(([a], [b]) => b.localeCompare(a))
      .map(([day, counts]) => ({
        day,
        ...counts,
        negativeRate: counts.negative / Math.max(1, counts.positive + counts.negative),
      }));
    const now = Date.now();
    const window = (minAge: number, maxAge: number) => {
      const selected = cases.filter((item) => {
        const at = item.metadata?.ratedAt;
        return typeof at === "number" && now - at >= minAge && now - at < maxAge;
      });
      return {
        count: selected.length,
        negativeRate:
          selected.filter((item) => item.metadata?.rating === "negative").length /
          Math.max(1, selected.length),
      };
    };
    const recent = window(0, 7 * 86_400_000);
    const previous = window(7 * 86_400_000, 14 * 86_400_000);
    const alert =
      recent.count >= 5 &&
      previous.count >= 5 &&
      recent.negativeRate - previous.negativeRate >= 0.2;
    return json(200, {
      datasetId,
      total: cases.length,
      trend,
      alert: alert ? { kind: "negative-rate-rise", recent, previous } : null,
    });
  };
}
