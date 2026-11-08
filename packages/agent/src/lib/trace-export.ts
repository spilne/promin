import { createHash } from "node:crypto";
import type { ToolScope } from "./tool.ts";
import type { TurnSpan, TurnTrace } from "./trace.ts";

/** Stable identifiers that let a trace backend group turns without seeing messages. */
export interface AgentTraceContext {
  readonly agentName: string;
  readonly workflowId: string;
  readonly scope?: ToolScope;
  /** W3C `traceparent` of the caller's request, when the agent config supplies one. */
  readonly traceparent?: string;
}

/** Default budget for one `exportTurn` call before it is routed to `onError`. */
export const DEFAULT_TRACE_EXPORT_TIMEOUT_MS = 5000;

export interface AgentTraceExporter {
  /** Longest `exportTurn` may run before the turn moves on and `onError` gets a timeout. */
  readonly timeoutMs?: number;
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
  /** Code 2 is `SpanStatusCode.ERROR`. */
  setStatus?(status: { code: number; message?: string }): void;
}

/** An `@opentelemetry/api` Tracer satisfies this interface. */
export interface OpenTelemetryTracerLike {
  startActiveSpan<T>(
    name: string,
    options: { readonly startTime: Date; readonly attributes: Attributes },
    fn: (span: OpenTelemetrySpanLike) => T,
  ): T;
  startActiveSpan<T>(
    name: string,
    options: { readonly startTime: Date; readonly attributes: Attributes },
    context: unknown,
    fn: (span: OpenTelemetrySpanLike) => T,
  ): T;
}

const SPAN_STATUS_ERROR = 2;

export interface OpenTelemetryAgentTraceOptions {
  readonly tracer: OpenTelemetryTracerLike;
  /** Return undefined to remove an attribute before it reaches the tracer. */
  readonly filterAttribute?: (key: string, value: AttributeValue) => AttributeValue | undefined;
  readonly onError?: AgentTraceExporter["onError"];
  /** Export deadline, default 5000 ms. A timeout is reported to `onError`. */
  readonly timeoutMs?: number;
  /**
   * Export the raw `agent.resource_id` and `agent.thread_id`. By default they are
   * replaced by a stable SHA-256 prefix so turns still group without naming anyone.
   */
  readonly includeIdentifiers?: boolean;
  /** Mixed into the identifier hash so hashes cannot be correlated across deployments. */
  readonly identifierSalt?: string;
  /**
   * Parent for the root span, an `@opentelemetry/api` Context. Typically
   * `propagation.extract(ROOT_CONTEXT, { traceparent: context.traceparent })`.
   * Return undefined to start a new trace.
   */
  readonly parentContext?: (context: AgentTraceContext) => unknown;
}

function hashIdentifier(value: string, salt: string | undefined): string {
  return createHash("sha256")
    .update(salt ? `${salt}\0${value}` : value)
    .digest("hex")
    .slice(0, 16);
}

/** Run an export under a deadline; throws on timeout, rejection or a synchronous throw. */
export async function runTraceExport(params: {
  readonly exporter: AgentTraceExporter;
  readonly trace: TurnTrace;
  readonly context: AgentTraceContext;
}): Promise<void> {
  const timeoutMs = params.exporter.timeoutMs ?? DEFAULT_TRACE_EXPORT_TIMEOUT_MS;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Agent trace export timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  const exported = (async () => params.exporter.exportTurn(params.trace, params.context))();
  exported.catch(() => {}); // a late failure after the timeout is already reported
  try {
    await Promise.race([exported, timeout]);
  } finally {
    clearTimeout(timer);
  }
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
    ...(options.timeoutMs !== undefined && { timeoutMs: options.timeoutMs }),
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
      const identifier = (value: string) =>
        options.includeIdentifiers ? value : hashIdentifier(value, options.identifierSalt);
      if (scope?.resourceId) rootAttributes["agent.resource_id"] = identifier(scope.resourceId);
      if (scope?.threadId) rootAttributes["agent.thread_id"] = identifier(scope.threadId);
      if (scope?.agentId) rootAttributes["agent.recipe_id"] = scope.agentId;
      if (trace.usage.cacheReadTokens !== undefined)
        rootAttributes["agent.usage.cache_read_tokens"] = trace.usage.cacheReadTokens;
      if (trace.usage.cacheWriteTokens !== undefined)
        rootAttributes["agent.usage.cache_write_tokens"] = trace.usage.cacheWriteTokens;

      const rootOptions = {
        startTime: new Date(trace.startedAt),
        attributes: attributes(rootAttributes),
      };
      const parent = options.parentContext?.(context);
      const body = (root: OpenTelemetrySpanLike) => {
        try {
          for (const child of trace.spans) {
            if (child.startedAt === undefined || child.durationMs === undefined) continue;
            options.tracer.startActiveSpan(
              `agent.${child.kind}`,
              {
                startTime: new Date(child.startedAt),
                attributes: attributes(spanAttributes(child)),
              },
              (span) => {
                if (child.kind === "tool" && child.failed) {
                  span.setStatus?.({ code: SPAN_STATUS_ERROR, message: "tool failed" });
                }
                span.end(new Date(child.startedAt! + child.durationMs!));
              },
            );
          }
        } finally {
          root.end(new Date(trace.endedAt));
        }
      };
      if (parent === undefined) options.tracer.startActiveSpan("agent.turn", rootOptions, body);
      else options.tracer.startActiveSpan("agent.turn", rootOptions, parent, body);
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
