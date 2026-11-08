// ---------------------------------------------------------------------------
// @promin/workflow/otel — OpenTelemetry adapter for `WorkflowTracer`.
//
// Structural types only: no `@opentelemetry/*` import, so the package has no
// OpenTelemetry dependency. Pass `@opentelemetry/api`'s tracer and namespace.
// ---------------------------------------------------------------------------

export {
  createOpenTelemetryWorkflowTracer,
  type OpenTelemetryWorkflowTracerOptions,
  type OtelApiLike,
  type OtelAttributeValue,
  type OtelSpanLike,
  type OtelTracerLike,
} from "./lib/tracing/otel-workflow-tracer.ts";
