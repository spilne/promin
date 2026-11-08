import type { ToolScope } from "./tool.ts";
import type { TurnSpan, TurnTrace } from "./trace.ts";

/** Stable identifiers that let a trace backend group turns without seeing messages. */
export interface AgentTraceContext {
  readonly agentName: string;
  readonly workflowId: string;
  readonly scope?: ToolScope;
}

export interface AgentTraceExporter {
  /** May be called again after a crash during export; deduplicate by workflowId. */
  exportTurn(trace: TurnTrace, context: AgentTraceContext): void | Promise<void>;
  /** Export failures are advisory and never fail the agent turn. */
  onError?(error: unknown, context: AgentTraceContext): void | Promise<void>;
}

type AttributeValue = string | number | boolean;
type Attributes = Record<string, AttributeValue>;

/** The small OpenTelemetry API surface used by this adapter. */
export interface OpenTelemetrySpanLike {
  end(endTime?: Date): void;
}

/** An `@opentelemetry/api` Tracer satisfies this interface. */
export interface OpenTelemetryTracerLike {
  startActiveSpan<T>(
    name: string,
    options: { readonly startTime: Date; readonly attributes: Attributes },
    fn: (span: OpenTelemetrySpanLike) => T,
  ): T;
}

export interface OpenTelemetryAgentTraceOptions {
  readonly tracer: OpenTelemetryTracerLike;
  /** Return undefined to remove an attribute before it reaches the tracer. */
  readonly filterAttribute?: (key: string, value: AttributeValue) => AttributeValue | undefined;
  readonly onError?: AgentTraceExporter["onError"];
}

/**
 * Export the engine's journaled turn timing as a root span with one child per
 * think, tool, approval and compaction. Only allowlisted identifiers, timings,
 * counts and tool names are emitted; prompts, answers, tool inputs and results
 * never enter an attribute. The optional filter can redact even these ids.
 */
export function createOpenTelemetryAgentTraceExporter(
  options: OpenTelemetryAgentTraceOptions,
): AgentTraceExporter {
  const attributes = (raw: Attributes): Attributes => {
    if (!options.filterAttribute) return raw;
    return Object.fromEntries(
      Object.entries(raw)
        .map(([key, value]) => [key, options.filterAttribute!(key, value)] as const)
        .filter((entry): entry is readonly [string, AttributeValue] => entry[1] !== undefined),
    );
  };
  return {
    ...(options.onError && { onError: options.onError }),
    exportTurn(trace, context) {
      const scope = context.scope;
      const rootAttributes: Attributes = {
        "agent.name": context.agentName,
        "agent.workflow_id": context.workflowId,
        "agent.finish_reason": trace.finishReason,
        "agent.steps": trace.steps,
        "agent.usage.input_tokens": trace.usage.inputTokens,
        "agent.usage.output_tokens": trace.usage.outputTokens,
      };
      if (scope?.namespaceId) rootAttributes["agent.namespace_id"] = scope.namespaceId;
      if (scope?.resourceId) rootAttributes["agent.resource_id"] = scope.resourceId;
      if (scope?.threadId) rootAttributes["agent.thread_id"] = scope.threadId;
      if (scope?.agentId) rootAttributes["agent.recipe_id"] = scope.agentId;
      if (trace.usage.cacheReadTokens !== undefined)
        rootAttributes["agent.usage.cache_read_tokens"] = trace.usage.cacheReadTokens;
      if (trace.usage.cacheWriteTokens !== undefined)
        rootAttributes["agent.usage.cache_write_tokens"] = trace.usage.cacheWriteTokens;

      options.tracer.startActiveSpan(
        "agent.turn",
        { startTime: new Date(trace.startedAt), attributes: attributes(rootAttributes) },
        (root) => {
          try {
            for (const child of trace.spans) {
              if (child.startedAt === undefined || child.durationMs === undefined) continue;
              options.tracer.startActiveSpan(
                `agent.${child.kind}`,
                {
                  startTime: new Date(child.startedAt),
                  attributes: attributes(spanAttributes(child)),
                },
                (span) => span.end(new Date(child.startedAt! + child.durationMs!)),
              );
            }
          } finally {
            root.end(new Date(trace.endedAt));
          }
        },
      );
    },
  };
}

function spanAttributes(span: TurnSpan): Attributes {
  const attrs: Attributes = { "agent.step": span.step };
  if (span.kind === "think") {
    if (span.usage) {
      attrs["agent.usage.input_tokens"] = span.usage.inputTokens;
      attrs["agent.usage.output_tokens"] = span.usage.outputTokens;
    }
  } else if (span.kind === "tool") {
    attrs["agent.tool.name"] = span.name;
    attrs["agent.tool.call_id"] = span.toolCallId;
    attrs["agent.tool.failed"] = span.failed;
    if (span.delegated) attrs["agent.tool.delegated"] = true;
  } else if (span.kind === "approval") {
    attrs["agent.tool.name"] = span.toolName;
    attrs["agent.tool.call_id"] = span.toolCallId;
    attrs["agent.approval.approved"] = span.approved;
  } else {
    attrs["agent.compaction.compacted"] = span.compacted;
    attrs["agent.compaction.summarized"] = span.summarized;
  }
  return attrs;
}
