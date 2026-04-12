// DataFrame
export {
  DataFrame,
  GroupedDataFrame,
  ArrayExecutor,
  StringAccessor,
  DateAccessor,
  type DataFrameExecutor,
  type ExecutionCost,
  type LogicalPlan,
  type AggFn,
  type CustomAgg,
  type WindowFn,
  type RollingFn,
  type CumulativeFn,
  percentile,
  reduce,
  CsvFile,
  ParquetFile,
  JsonFile,
  TsvFile,
  isFileSource,
  type FileSourceDescriptor,
  Expr,
  WhenExpr,
  col,
  lit,
  when,
  isCompilable,
  astToSql,
  type ExprAst,
  type DataFrameSink,
  CsvSink,
  JsonlSink,
  optimizePlan,
  classifyPlan,
  type PlanStreamability,
  executeChunked,
  type GroupAccumulator,
  createAccumulator,
  accumulate,
  finalize,
  IncrementalAggregation,
  type IncrementalAggregationConfig,
} from "./lib/dataframe/index.ts";

// Data profiling
export {
  type ProfileReport,
  type ColumnProfile,
  type NumericProfile,
  type StringProfile,
  type BooleanProfile,
  type DateProfile,
  type CorrelationPair,
  type ProfileWarning,
  profileData,
  type ProfileOptions,
} from "./lib/data-profiler/index.ts";

// Data diff
export {
  dataDiff,
  schemaDiff,
  type DataDiffResult,
  type DiffOptions,
  type Modification,
  type SchemaDiffResult,
} from "./lib/data-diff/index.ts";

// Data quality
export {
  type Expectation,
  type ExpectationResult,
  type ValidationResult,
  ExpectationSuite,
} from "./lib/data-quality/index.ts";

// Data contracts
export {
  type DataContract,
  type ContractValidationResult,
  type ValidatableContract,
  defineContract,
} from "./lib/data-contracts/index.ts";

// Data testing
export {
  generators,
  generateRows,
  type Generator,
  dataframeProperties,
  streamProperties,
} from "./lib/data-testing/index.ts";
