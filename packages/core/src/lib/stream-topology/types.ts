// ---------------------------------------------------------------------------
// StreamTopology types — stateful stream processing with windows, joins, checkpointing
// ---------------------------------------------------------------------------

export interface TimeWindow {
  start: number;
  end: number;
}

export type WindowType =
  | { type: "tumbling"; windowMs: number }
  | { type: "sliding"; windowMs: number; slideMs: number }
  | { type: "session"; gapMs: number };

export interface AggregateSpec<S, T, U> {
  init: () => S;
  add: (state: S, value: T) => S;
  emit: (key: string, window: TimeWindow, state: S) => U;
}

export interface ProcessSpec<S, T, U> {
  init: () => S;
  process: (state: S, value: T) => { state: S; emit?: U };
}

export interface JoinConfig {
  windowMs: number;
}

export interface TopologyConfig {
  group: string;
  stateBackend?: unknown; // StateBackend<string, unknown>
  checkpointIntervalMs?: number;
  /** Max items buffered between stages before backpressure kicks in. Default: unbounded. */
  maxBufferSize?: number;
  /** Max items emitted per second across the topology. Default: unlimited. */
  maxItemsPerSecond?: number;
  /** Max entries in the dedup seen-set before oldest are evicted. Default: 100_000. */
  maxDedupeSize?: number;
  /** Called when backpressure is applied (buffer full). */
  onBackpressure?: (stats: BackpressureStats) => void;
}

export interface BackpressureStats {
  /** Current buffer fill level (0-1). */
  fillRatio: number;
  /** Number of items in the buffer. */
  bufferedItems: number;
  /** Max buffer capacity. */
  maxBuffer: number;
  /** Timestamp of the event. */
  timestamp: number;
}

export interface TopologyHandle {
  shutdown(): Promise<void>;
  isRunning(): boolean;
  /** Get current topology metrics. */
  metrics(): TopologyMetrics;
}

export interface TopologyMetrics {
  /** Total items processed since start. */
  itemsProcessed: number;
  /** Items processed per second (rolling average). */
  itemsPerSecond: number;
  /** Current buffer fill levels by operator. */
  bufferStats: { operator: string; buffered: number; capacity: number }[];
  /** Number of keys in dedup set. */
  dedupeSize: number;
  /** Number of active windows. */
  activeWindows: number;
  /** Number of buffered join items (left + right). */
  joinBufferSize: number;
}

// ---------------------------------------------------------------------------
// Topology plan nodes — the logical plan for the processing DAG
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TopologyNode<_T = any> =
  | SourceNode<any>
  | MapNode<any>
  | FilterNode<any>
  | MapAsyncNode<any>
  | KeyByNode<any>
  | WindowNode<any>
  | AggregateNode<any>
  | ProcessNode<any>
  | DedupeNode<any>
  | JoinNode<any>
  | SinkNode<any>;

export interface SourceNode<T> {
  type: "source";
  source: unknown; // Streamable & Acknowledgeable
}

export interface MapNode<T> {
  type: "map";
  parent: TopologyNode;
  fn: (value: unknown) => T;
}

export interface FilterNode<T> {
  type: "filter";
  parent: TopologyNode;
  fn: (value: T) => boolean;
}

export interface MapAsyncNode<T> {
  type: "mapAsync";
  parent: TopologyNode;
  concurrency: number;
  fn: (value: unknown) => Promise<T>;
}

export interface KeyByNode<T> {
  type: "keyBy";
  parent: TopologyNode;
  keyFn: (value: T) => string;
}

export interface WindowNode<T> {
  type: "window";
  parent: TopologyNode;
  windowType: WindowType;
}

export interface AggregateNode<T> {
  type: "aggregate";
  parent: TopologyNode;
  spec: AggregateSpec<unknown, unknown, T>;
}

export interface ProcessNode<T> {
  type: "process";
  parent: TopologyNode;
  spec: ProcessSpec<unknown, unknown, T>;
}

export interface DedupeNode<T> {
  type: "dedupe";
  parent: TopologyNode;
  keyFn: (value: T) => string;
}

export interface JoinNode<T> {
  type: "join";
  left: TopologyNode;
  right: TopologyNode;
  config: JoinConfig;
}

export interface SinkNode<T> {
  type: "sink";
  parent: TopologyNode;
  sink: unknown; // Sinkable
}

export interface CompiledTopology {
  nodes: TopologyNode[];
  sinks: SinkNode<unknown>[];
}
