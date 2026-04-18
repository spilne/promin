export { DataFrame } from "./dataframe.ts";
export { GroupedDataFrame } from "./grouped-dataframe.ts";
export { ArrayExecutor } from "./array-executor.ts";
export { StringAccessor, DateAccessor } from "./accessors.ts";
export {
  Expr,
  WhenExpr,
  ListExpr,
  col,
  lit,
  when,
  isCompilable,
  astToSql,
  type ExprAst,
} from "./expr.ts";
export { type DataFrameExecutor, type ExecutionCost } from "./executor.ts";
export {
  type LogicalPlan,
  type AggFn,
  type CustomAgg,
  type ExprAgg,
  type WindowFn,
  type RollingFn,
  type CumulativeFn,
  percentile,
  reduce,
  exprAgg,
} from "./logical-plan.ts";
export { optimizePlan } from "./plan-optimizer.ts";
export {
  CsvFile,
  ParquetFile,
  JsonFile,
  TsvFile,
  isFileSource,
  type FileSourceDescriptor,
} from "./file-source.ts";
export { type DataFrameSink, CsvSink, JsonlSink } from "./sink.ts";
export { classifyPlan, type PlanStreamability } from "./plan-classifier.ts";
export { executeChunked } from "./chunked-executor.ts";
export { type GroupAccumulator, createAccumulator, accumulate, finalize } from "./streaming-agg.ts";
export {
  IncrementalAggregation,
  type IncrementalAggregationConfig,
} from "./incremental-aggregation.ts";
