import { buildAgentTrace, type Message } from "@promin/agent";
import type { EvalCase, EvalDatasetStore } from "@promin/evals";
import { z } from "zod";
import { json, jsonError, readJson } from "../../router.ts";
import { feedbackDatasetId, regressionDatasetId } from "../../services/evals/feedback-datasets.ts";
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

export { feedbackDatasetId, regressionDatasetId };

/** Most earlier messages stored with a case, and their combined text size. */
const MAX_HISTORY_MESSAGES = 40;
const MAX_HISTORY_CHARS = 60_000;

/**
 * The conversation before a turn, in the shape `agentTarget` replays
 * (`metadata.messages`): user and assistant messages and tool results of
 * earlier turns, without engine metadata, system messages or thinking
 * blocks. A long history keeps its most recent turns, starting at a user
 * message so tool calls stay paired with their results.
 */
function replayableHistory(params: { messages: ReadonlyArray<Message>; beforeSeq: number }): {
  messages: Message[];
  truncated: boolean;
} {
  const earlier = params.messages
    .filter((message, index) => ((message as { seq?: number }).seq ?? index) < params.beforeSeq)
    .filter((message) => message.role !== "system")
    .map((message): Message => {
      if (message.role === "user") return { role: "user", content: message.content };
      if (message.role === "tool") {
        return {
          role: "tool",
          toolCallId: message.toolCallId,
          content: message.content,
          ...(message.isError && { isError: true }),
        };
      }
      return {
        role: "assistant",
        content: message.content,
        ...(message.toolCalls && { toolCalls: message.toolCalls }),
      };
    });
  const size = (items: ReadonlyArray<Message>) =>
    items.reduce((sum, item) => sum + (item.content?.length ?? 0), 0);
  let start = Math.max(0, earlier.length - MAX_HISTORY_MESSAGES);
  const firstUserFrom = (from: number) => {
    const found = earlier.findIndex((item, index) => index >= from && item.role === "user");
    return found === -1 ? earlier.length : found;
  };
  start = firstUserFrom(start);
  while (start < earlier.length && size(earlier.slice(start)) > MAX_HISTORY_CHARS) {
    start = firstUserFrom(start + 1);
  }
  return { messages: earlier.slice(start), truncated: start > 0 };
}

/** The operator check for writing or deleting feedback (403 `scope_forbidden`). */
async function requireFeedbackOperator(params: {
  deps: AgentGatewayDeps;
  req: Request;
}): Promise<Response | undefined> {
  const authorize = params.deps.authorizeScope;
  if (!authorize) return undefined;
  const allowed = await authorize({
    req: params.req,
    scope: { kind: "operator", resource: "agent-feedback" },
  });
  return allowed
    ? undefined
    : jsonError(403, "scope_forbidden", "Writing feedback needs operator access.");
}

/** Insert the case, or replace the one already stored under its id. */
async function upsertCase(params: {
  datasets: EvalDatasetStore;
  datasetId: string;
  evalCase: EvalCase;
}): Promise<void> {
  const { datasets, datasetId, evalCase } = params;
  if (await datasets.appendCase(datasetId, evalCase)) return;
  if (await datasets.replaceCase(datasetId, evalCase)) return;
  // The case vanished between the two calls (a concurrent delete): add it back.
  await datasets.appendCase(datasetId, evalCase);
}

/**
 * Rate a turn. One case per turn: rating again replaces the earlier rating
 * in the feedback dataset, and the regression dataset follows the latest
 * rating (a case while it is negative with an `expected` answer, none
 * otherwise). The two writes are separate, so a failure between them is
 * healed by repeating the request.
 */
export function postAgentFeedback(deps: AgentGatewayDeps, datasets: EvalDatasetStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const forbidden = await requireFeedbackOperator({ deps, req });
    if (forbidden) return forbidden;
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
    const history = replayableHistory({ messages, beforeSeq: turn.fromSeq });
    const datasetId = feedbackDatasetId(scoped.namespaceId, scoped.recipe.id, scoped.resourceId);
    const regressionId = regressionDatasetId(
      scoped.namespaceId,
      scoped.recipe.id,
      scoped.resourceId,
    );
    const caseId = feedbackCaseId({ threadId: thread.id, workflowId: parsed.data.workflowId });
    const regression = parsed.data.rating === "negative" && parsed.data.expected !== undefined;
    const base = {
      agentId: scoped.recipe.id,
      agentVersion: scoped.recipe.version,
      namespaceId: scoped.namespaceId,
      resourceId: scoped.resourceId,
      threadId: thread.id,
      workflowId: parsed.data.workflowId,
      // Replayed before the input by the eval runner (`agentTarget`).
      ...(history.messages.length > 0 && {
        messages: history.messages,
        historyTruncated: history.truncated,
      }),
    };
    const historyTags = history.messages.length > 0 ? ["depends-on-history"] : [];
    await upsertCase({
      datasets,
      datasetId,
      evalCase: {
        id: caseId,
        input: input.content,
        ...(parsed.data.expected !== undefined && { expected: parsed.data.expected }),
        tags: [parsed.data.rating, ...(regression ? ["regression-case"] : []), ...historyTags],
        metadata: {
          ...base,
          rating: parsed.data.rating,
          ...(parsed.data.comment !== undefined && { comment: parsed.data.comment }),
          ratedAt: Date.now(),
        },
      },
    });
    if (regression) {
      await upsertCase({
        datasets,
        datasetId: regressionId,
        evalCase: {
          id: caseId,
          input: input.content,
          expected: parsed.data.expected,
          tags: ["operator-regression", ...historyTags],
          metadata: base,
        },
      });
    } else {
      await datasets.deleteCase(regressionId, caseId);
    }
    return json(201, { caseId, datasetId, regressionDatasetId: regressionId });
  };
}

function feedbackCaseId(params: { threadId: string; workflowId: string }): string {
  return `${params.threadId}:${params.workflowId}`;
}

/**
 * Remove a turn's rating and regression case, so user content copied into
 * the datasets does not outlive what the operator wants kept. Idempotent.
 */
export function deleteAgentFeedback(deps: AgentGatewayDeps, datasets: EvalDatasetStore) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const forbidden = await requireFeedbackOperator({ deps, req });
    if (forbidden) return forbidden;
    const workflowId = new URL(req.url).searchParams.get("workflowId");
    if (!workflowId) return jsonError(400, "missing_workflowId");
    const opened = await openThreadFromQuery({ deps, req, params });
    if (opened instanceof Response) return opened;
    const { thread, scoped } = opened;
    const caseId = feedbackCaseId({ threadId: thread.id, workflowId });
    const rated = await datasets.deleteCase(
      feedbackDatasetId(scoped.namespaceId, scoped.recipe.id, scoped.resourceId),
      caseId,
    );
    const sampled = await datasets.deleteCase(
      regressionDatasetId(scoped.namespaceId, scoped.recipe.id, scoped.resourceId),
      caseId,
    );
    return json(200, { caseId, deleted: rated || sampled });
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
