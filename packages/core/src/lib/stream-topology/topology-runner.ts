// ---------------------------------------------------------------------------
// TopologyRunner — compiles and executes a StreamTopology
//
// Walks the logical plan (topology nodes) and compiles them into a
// StreamPipeline that runs within a single process. Kafka consumer groups
// handle partition assignment across instances — the runner processes
// whatever partitions are assigned to it.
//
// State is keyed and checkpointed so it can be restored on crash/rebalance.
// Backpressure is applied via bounded buffers and rate limiting.
// ---------------------------------------------------------------------------

import { StreamPipeline } from "../stream-pipeline.ts";
import { type FusibleOp, fuseOpsToStream } from "../fusion.ts";
import type { Streamable, Acknowledgeable, Envelope, Sinkable } from "../typeclasses/streamable.ts";
import type { StateBackend } from "../typeclasses/state-backend.ts";
import { InMemoryState } from "../adapters/memory/in-memory-state.ts";
import { BuiltTopology } from "./stream-topology.ts";
import { WindowManager } from "./window-manager.ts";
import { JoinBuffer } from "./join-buffer.ts";
import type {
  TopologyNode,
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  AggregateSpec,
  ProcessSpec,
  WindowType,
} from "./types.ts";

export class TopologyRunner {
  /** Compile and run a topology. Returns a handle for shutdown. */
  static async run(topology: BuiltTopology, config: TopologyConfig): Promise<TopologyHandle> {
    const runner = new TopologyRunnerInstance(topology, config);
    return runner.start();
  }
}

// ---------------------------------------------------------------------------
// LRU set — bounded dedup with eviction
// ---------------------------------------------------------------------------

class LruSet {
  private set = new Set<string>();
  private order: string[] = [];

  constructor(private readonly maxSize: number) {}

  has(key: string): boolean {
    return this.set.has(key);
  }

  add(key: string): void {
    if (this.set.has(key)) return;
    this.set.add(key);
    this.order.push(key);
    while (this.set.size > this.maxSize) {
      const oldest = this.order.shift()!;
      this.set.delete(oldest);
    }
  }

  get size(): number {
    return this.set.size;
  }

  toArray(): string[] {
    return [...this.set];
  }

  restore(keys: string[]): void {
    this.set.clear();
    this.order = [];
    for (const key of keys) {
      this.set.add(key);
      this.order.push(key);
    }
  }
}

// ---------------------------------------------------------------------------
// Rate limiter — token bucket
// ---------------------------------------------------------------------------

class RateLimiter {
  private tokens: number;
  private lastRefill: number;

  constructor(private readonly maxPerSecond: number) {
    this.tokens = maxPerSecond;
    this.lastRefill = Date.now();
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = Date.now();
      const elapsed = (now - this.lastRefill) / 1000;
      this.tokens = Math.min(this.maxPerSecond, this.tokens + elapsed * this.maxPerSecond);
      this.lastRefill = now;

      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }

      // Wait for token to become available
      const waitMs = Math.ceil(((1 - this.tokens) / this.maxPerSecond) * 1000);
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
}

// ---------------------------------------------------------------------------
// TopologyRunnerInstance
// ---------------------------------------------------------------------------

class TopologyRunnerInstance {
  private running = true;
  private stateBackend: StateBackend<string, unknown>;
  private checkpointInterval: ReturnType<typeof setInterval> | null = null;
  private abortController = new AbortController();

  // Stateful operators tracked for checkpointing
  private windowManagers: WindowManager<unknown, unknown, unknown>[] = [];
  private joinBuffers: JoinBuffer<unknown, unknown>[] = [];
  private dedupSets: LruSet[] = [];
  private processStates: Map<string, unknown>[] = [];

  // Batch ack — flush pending on shutdown
  private pendingAckEnvelope: Envelope<unknown> | null = null;

  // Metrics
  private itemsProcessed = 0;
  private metricsStartTime = Date.now();
  private rateLimiter: RateLimiter | null = null;

  constructor(
    private readonly topology: BuiltTopology,
    private readonly config: TopologyConfig,
  ) {
    this.stateBackend =
      (config.stateBackend as StateBackend<string, unknown>) ?? new InMemoryState();

    if (config.maxItemsPerSecond) {
      this.rateLimiter = new RateLimiter(config.maxItemsPerSecond);
    }
  }

  async start(): Promise<TopologyHandle> {
    // Restore all state from last checkpoint
    await this.restoreAllState();

    // Set up periodic checkpointing
    if (this.config.checkpointIntervalMs) {
      this.checkpointInterval = setInterval(
        () => this.checkpointAllState(),
        this.config.checkpointIntervalMs,
      );
    }

    // Compile and run each sink branch
    const promises: Promise<void>[] = [];
    if (this.topology.compiled.sinks.length > 0) {
      for (const sink of this.topology.compiled.sinks) {
        let pipeline = this.compile(sink.parent);
        const sinkTarget = sink.sink as Sinkable<unknown>;

        // Apply backpressure buffer
        if (this.config.maxBufferSize) {
          pipeline = pipeline.buffer(this.config.maxBufferSize);
        }

        promises.push(
          pipeline
            .interruptOn(this.abortController.signal)
            .tapAsync(async (value) => {
              if (this.rateLimiter) await this.rateLimiter.acquire();
              await sinkTarget.publish(value);
              this.itemsProcessed++;
            })
            .drain(),
        );
      }
    } else {
      const terminal = this.topology.compiled.nodes[this.topology.compiled.nodes.length - 1]!;
      let pipeline = this.compile(terminal);

      if (this.config.maxBufferSize) {
        pipeline = pipeline.buffer(this.config.maxBufferSize);
      }

      promises.push(
        pipeline
          .interruptOn(this.abortController.signal)
          .tap(() => {
            this.itemsProcessed++;
          })
          .drain(),
      );
    }

    const drainPromise = Promise.all(promises);

    const self = this;
    const handle: TopologyHandle = {
      shutdown: async () => {
        self.running = false;
        self.abortController.abort();
        if (self.checkpointInterval) clearInterval(self.checkpointInterval);
        // Flush pending ack before checkpoint
        if (self.pendingAckEnvelope) {
          await self.pendingAckEnvelope.ack();
          self.pendingAckEnvelope = null;
        }
        await self.checkpointAllState();
        await drainPromise.catch(() => {});
      },
      isRunning: () => self.running,
      metrics: () => self.getMetrics(),
    };

    return handle;
  }

  // -------------------------------------------------------------------------
  // Metrics
  // -------------------------------------------------------------------------

  private getMetrics(): TopologyMetrics {
    const elapsed = (Date.now() - this.metricsStartTime) / 1000;
    return {
      itemsProcessed: this.itemsProcessed,
      itemsPerSecond: elapsed > 0 ? this.itemsProcessed / elapsed : 0,
      bufferStats: [], // Could be wired to Effect's buffer internals in future
      dedupeSize: this.dedupSets.reduce((sum, s) => sum + s.size, 0),
      activeWindows: this.windowManagers.reduce((sum, wm) => sum + wm.snapshot().length, 0),
      joinBufferSize: this.joinBuffers.reduce((sum, jb) => {
        const s = jb.stats();
        return sum + s.leftItems + s.rightItems;
      }, 0),
    };
  }

  // -------------------------------------------------------------------------
  // State checkpoint / restore
  // -------------------------------------------------------------------------

  private async checkpointAllState(): Promise<void> {
    // Save window manager state
    for (let i = 0; i < this.windowManagers.length; i++) {
      const snapshot = this.windowManagers[i]!.snapshot();
      await this.stateBackend.put(`window:${i}`, snapshot);
    }

    // Save dedup sets
    for (let i = 0; i < this.dedupSets.length; i++) {
      await this.stateBackend.put(`dedupe:${i}`, this.dedupSets[i]!.toArray());
    }

    // Save join buffer state — left and right buffers
    for (let i = 0; i < this.joinBuffers.length; i++) {
      await this.stateBackend.put(`join:${i}`, this.joinBuffers[i]!.snapshot());
    }

    // Process states are saved inline during processing, but flush here too
    for (let i = 0; i < this.processStates.length; i++) {
      const entries = [...this.processStates[i]!.entries()];
      await this.stateBackend.put(`process-map:${i}`, entries);
    }

    await this.stateBackend.checkpoint({ name: `topology:${this.config.group}` });
  }

  private async restoreAllState(): Promise<void> {
    await this.stateBackend.restore({ name: `topology:${this.config.group}` });

    // Note: actual restoration happens in compile* methods when they
    // create their stateful operators — they check the backend for
    // existing state. This method just loads the checkpoint into the
    // backend's live state.
  }

  // -------------------------------------------------------------------------
  // Compilation
  // -------------------------------------------------------------------------

  private compile(node: TopologyNode): StreamPipeline<unknown, never> {
    // Collect adjacent pure ops (map/filter) and fuse them into a single mapChunks call.
    const { pureOps, baseNode } = this.collectPureOps(node);
    if (pureOps.length > 0) {
      const base = this.compile(baseNode);
      return this.applyFused(base, pureOps);
    }

    switch (node.type) {
      case "source":
        return this.compileSource(node);
      case "map":
      case "filter":
        // Should not reach here — collectPureOps handles these
        return this.compile(node.parent).map(node.fn);
      case "mapAsync":
        return this.compile(node.parent).parAsyncMap(node.concurrency, node.fn);
      case "keyBy":
        return this.compile(node.parent);
      case "window":
        return this.compile(node.parent);
      case "aggregate":
        return this.compileAggregate(node);
      case "process":
        return this.compileProcess(node);
      case "dedupe":
        return this.compileDedupe(node);
      case "join":
        return this.compileJoin(node);
      case "sink":
        return this.compile(node.parent);
    }
  }

  /**
   * Walk backwards from a node collecting adjacent map/filter ops.
   * Returns the ops in execution order (innermost parent first) and the
   * base node where the pure chain ends.
   */
  private collectPureOps(node: TopologyNode): {
    pureOps: Array<
      { type: "map"; fn: (v: unknown) => unknown } | { type: "filter"; fn: (v: unknown) => boolean }
    >;
    baseNode: TopologyNode;
  } {
    const ops: Array<
      { type: "map"; fn: (v: unknown) => unknown } | { type: "filter"; fn: (v: unknown) => boolean }
    > = [];
    let current = node;

    while (current.type === "map" || current.type === "filter") {
      if (current.type === "map") {
        ops.push({ type: "map", fn: current.fn });
      } else {
        ops.push({ type: "filter", fn: current.fn as (v: unknown) => boolean });
      }
      current = current.parent;
    }

    // Ops are collected outermost-first, reverse for execution order
    ops.reverse();
    return { pureOps: ops, baseNode: current };
  }

  /** Apply fused pure ops as a single mapChunks call. */
  private applyFused(
    pipeline: StreamPipeline<unknown, never>,
    ops: Array<
      { type: "map"; fn: (v: unknown) => unknown } | { type: "filter"; fn: (v: unknown) => boolean }
    >,
  ): StreamPipeline<unknown, never> {
    if (ops.length === 1) {
      const op = ops[0]!;
      return op.type === "map" ? pipeline.map(op.fn) : pipeline.filter(op.fn);
    }

    // Convert to FusibleOp format and use shared fusion
    const fusibleOps: FusibleOp[] = ops.map((op) =>
      op.type === "map"
        ? { tag: "map" as const, fn: op.fn }
        : { tag: "filter" as const, fn: op.fn },
    );

    return new StreamPipeline(fuseOpsToStream(pipeline.stream, fusibleOps));
  }

  private compileSource(node: { source: unknown }): StreamPipeline<unknown, never> {
    const source = node.source as Streamable<unknown> & Acknowledgeable<unknown>;
    const batchSize = this.config.ackBatchSize ?? 100;
    let count = 0;

    // Use subscribe() (no ack) when ack is disabled, subscribeAck() otherwise.
    // Batch ack: sync-unwrap values, ack only the last envelope per batch.
    // For Kafka, acking offset N implicitly acks all offsets < N.
    if (batchSize <= 0) {
      // No ack — fastest path for sources that don't need acknowledgement
      return source.subscribe();
    }

    // Single mapChunks: unwrap + batch ack in one pass per chunk (~4096 items).
    // Avoids per-item Effect overhead from chained .map().tap().
    return source
      .subscribeAck({ group: this.config.group })
      .mapChunks((chunk: Envelope<unknown>[]) => {
        const values: unknown[] = new Array(chunk.length);
        for (let i = 0; i < chunk.length; i++) {
          const env = chunk[i]!;
          values[i] = env.value;
          this.pendingAckEnvelope = env;
          count++;
          if (count >= batchSize) {
            env.ack();
            this.pendingAckEnvelope = null;
            count = 0;
          }
        }
        return values;
      });
  }

  private compileAggregate(node: {
    parent: TopologyNode;
    spec: AggregateSpec<unknown, unknown, unknown>;
  }): StreamPipeline<unknown, never> {
    const { windowType, keyFn } = this.findWindowAndKey(node.parent);
    const manager = new WindowManager(windowType, node.spec);
    const idx = this.windowManagers.length;
    this.windowManagers.push(manager);

    // Restore from checkpoint (fire-and-forget — completes before first item arrives)
    this.stateBackend.get(`window:${idx}`).then((saved) => {
      if (saved && Array.isArray(saved)) {
        manager.restore(saved as any);
      }
    });

    return this.compile(node.parent).flatMap((value: unknown) => {
      const key = keyFn(value);
      const now = this.extractTimestamp(value);
      const emitted = manager.add(key, value, now);
      const flushed = manager.flush(key, now);

      const all = [...emitted, ...flushed];
      return StreamPipeline.fromIterable(all) as StreamPipeline<unknown, never>;
    });
  }

  private compileProcess(node: {
    parent: TopologyNode;
    spec: ProcessSpec<unknown, unknown, unknown>;
  }): StreamPipeline<unknown, never> {
    const { keyFn } = this.findKeyBy(node.parent);
    const keyStates = new Map<string, unknown>();
    const idx = this.processStates.length;
    this.processStates.push(keyStates);

    // Restore from checkpoint
    this.stateBackend.get(`process-map:${idx}`).then((saved) => {
      if (saved && Array.isArray(saved)) {
        for (const [k, v] of saved as [string, unknown][]) {
          keyStates.set(k, v);
        }
      }
    });

    return this.compile(node.parent).filterMap((value: unknown) => {
      const key = keyFn(value);
      const currentState = keyStates.get(key) ?? node.spec.init();
      const result = node.spec.process(currentState, value);
      keyStates.set(key, result.state);
      return result.emit;
    });
  }

  private compileDedupe(node: {
    parent: TopologyNode;
    keyFn: (value: unknown) => string;
  }): StreamPipeline<unknown, never> {
    const maxSize = this.config.maxDedupeSize ?? 100_000;
    const seen = new LruSet(maxSize);
    const idx = this.dedupSets.length;
    this.dedupSets.push(seen);

    // Restore from checkpoint
    this.stateBackend.get(`dedupe:${idx}`).then((saved) => {
      if (saved && Array.isArray(saved)) {
        seen.restore(saved as string[]);
      }
    });

    return this.compile(node.parent).filter((value: unknown) => {
      const key = node.keyFn(value);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  private compileJoin(node: {
    left: TopologyNode;
    right: TopologyNode;
    config: { windowMs: number };
  }): StreamPipeline<unknown, never> {
    const buffer = new JoinBuffer(node.config.windowMs);
    const idx = this.joinBuffers.length;
    this.joinBuffers.push(buffer);

    // Restore from checkpoint
    this.stateBackend.get(`join:${idx}`).then((saved) => {
      if (saved) {
        buffer.restore(saved as any);
      }
    });

    const leftKeyFn = this.findKeyBy(node.left).keyFn;
    const rightKeyFn = this.findKeyBy(node.right).keyFn;

    type Tagged = { side: "left" | "right"; key: string; value: unknown; ts: number };

    const leftStream: StreamPipeline<Tagged, never> = this.compile(node.left).map((value) => ({
      side: "left" as const,
      key: leftKeyFn(value),
      value,
      ts: this.extractTimestamp(value),
    }));

    const rightStream: StreamPipeline<Tagged, never> = this.compile(node.right).map((value) => ({
      side: "right" as const,
      key: rightKeyFn(value),
      value,
      ts: this.extractTimestamp(value),
    }));

    return leftStream.merge(rightStream).flatMap((tagged) => {
      const matches =
        tagged.side === "left"
          ? buffer.addLeft(tagged.key, tagged.value, tagged.ts)
          : buffer.addRight(tagged.key, tagged.value, tagged.ts);

      return StreamPipeline.fromIterable(matches) as StreamPipeline<unknown, never>;
    });
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private findWindowAndKey(node: TopologyNode): {
    windowType: WindowType;
    keyFn: (value: unknown) => string;
  } {
    let windowType: WindowType | undefined;
    let keyFn: ((value: unknown) => string) | undefined;
    let current: TopologyNode | undefined = node;

    while (current) {
      if (current.type === "window" && !windowType) {
        windowType = current.windowType;
      }
      if (current.type === "keyBy" && !keyFn) {
        keyFn = current.keyFn as (value: unknown) => string;
      }
      if (windowType && keyFn) break;
      current = "parent" in current ? (current as any).parent : undefined;
    }

    if (!windowType) throw new Error("aggregate requires a window (tumbling/sliding/session)");
    if (!keyFn) throw new Error("windowed aggregate requires keyBy");

    return { windowType, keyFn };
  }

  private findKeyBy(node: TopologyNode): { keyFn: (value: unknown) => string } {
    let current: TopologyNode | undefined = node;
    while (current) {
      if (current.type === "keyBy") {
        return { keyFn: current.keyFn as (value: unknown) => string };
      }
      current = "parent" in current ? (current as any).parent : undefined;
    }
    throw new Error("stateful operator requires keyBy");
  }

  private extractTimestamp(value: unknown): number {
    if (value && typeof value === "object") {
      const v = value as Record<string, unknown>;
      if (typeof v.ts === "number") return v.ts;
      if (typeof v.timestamp === "number") return v.timestamp;
      if (typeof v.eventTime === "number") return v.eventTime;
      if (typeof v.createdAt === "string") return new Date(v.createdAt).getTime();
    }
    return Date.now();
  }
}
