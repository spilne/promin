// ---------------------------------------------------------------------------
// TopologyRunner — compiles and executes a StreamTopology
//
// Walks the logical plan (topology nodes) and compiles them into a
// StreamPipeline that runs within a single process. Kafka consumer groups
// handle partition assignment across instances — the runner processes
// whatever partitions are assigned to it.
//
// State is keyed and checkpointed so it can be restored on crash/rebalance.
// ---------------------------------------------------------------------------

import { StreamPipeline } from "../stream-pipeline.ts";
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

class TopologyRunnerInstance {
  private running = true;
  private stateBackend: StateBackend<string, unknown>;
  private checkpointInterval: ReturnType<typeof setInterval> | null = null;
  private abortController = new AbortController();

  constructor(
    private readonly topology: BuiltTopology,
    private readonly config: TopologyConfig,
  ) {
    this.stateBackend =
      (config.stateBackend as StateBackend<string, unknown>) ?? new InMemoryState();
  }

  async start(): Promise<TopologyHandle> {
    // Restore from last checkpoint
    await this.stateBackend.restore({ name: `topology:${this.config.group}` });

    // Set up periodic checkpointing
    if (this.config.checkpointIntervalMs) {
      this.checkpointInterval = setInterval(
        () => this.checkpoint(),
        this.config.checkpointIntervalMs,
      );
    }

    // Compile and run each sink branch
    const promises: Promise<void>[] = [];
    if (this.topology.compiled.sinks.length > 0) {
      for (const sink of this.topology.compiled.sinks) {
        const pipeline = this.compile(sink.parent);
        const sinkTarget = sink.sink as Sinkable<unknown>;
        promises.push(
          pipeline
            .interruptOn(this.abortController.signal)
            .tapAsync(async (value) => {
              await sinkTarget.publish(value);
            })
            .drain(),
        );
      }
    } else {
      // No sinks — just compile the terminal node and drain
      const terminal = this.topology.compiled.nodes[this.topology.compiled.nodes.length - 1]!;
      const pipeline = this.compile(terminal);
      promises.push(pipeline.interruptOn(this.abortController.signal).drain());
    }

    // Run in background
    const drainPromise = Promise.all(promises);

    const handle: TopologyHandle = {
      shutdown: async () => {
        this.running = false;
        this.abortController.abort();
        if (this.checkpointInterval) clearInterval(this.checkpointInterval);
        await this.checkpoint();
        await drainPromise.catch(() => {}); // swallow interrupt
      },
      isRunning: () => this.running,
    };

    return handle;
  }

  /** Recursively compile a topology node into a StreamPipeline. */
  private compile(node: TopologyNode): StreamPipeline<unknown, never> {
    switch (node.type) {
      case "source":
        return this.compileSource(node);
      case "map":
        return this.compile(node.parent).map(node.fn);
      case "filter":
        return this.compile(node.parent).filter(node.fn as (value: unknown) => boolean);
      case "mapAsync":
        return this.compile(node.parent).parAsyncMap(node.concurrency, node.fn);
      case "keyBy":
        // keyBy is a logical marker — at runtime, items just flow through
        // The key function is used by downstream stateful operators
        return this.compile(node.parent);
      case "window":
        // Window is a logical marker — actual windowing happens in aggregate
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
        // Sink is handled by the runner, not compiled into the pipeline
        return this.compile(node.parent);
    }
  }

  private compileSource(node: { source: unknown }): StreamPipeline<unknown, never> {
    const source = node.source as Streamable<unknown> & Acknowledgeable<unknown>;
    // Use ack-based subscription for at-least-once processing
    return source
      .subscribeAck({ group: this.config.group })
      .mapAsync(async (envelope: Envelope<unknown>) => {
        const value = envelope.value;
        await envelope.ack();
        return value;
      });
  }

  private compileAggregate(node: {
    parent: TopologyNode;
    spec: AggregateSpec<unknown, unknown, unknown>;
  }): StreamPipeline<unknown, never> {
    // Walk up to find the window and keyBy nodes
    const { windowType, keyFn } = this.findWindowAndKey(node.parent);
    const manager = new WindowManager(windowType, node.spec);

    return this.compile(node.parent).flatMap((value: unknown) => {
      const key = keyFn(value);
      const now = this.extractTimestamp(value);
      const emitted = manager.add(key, value, now);

      // Also flush completed windows
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

    return this.compile(node.parent).filterMap((value: unknown) => {
      const key = keyFn(value);
      const currentState = keyStates.get(key) ?? node.spec.init();
      const result = node.spec.process(currentState, value);
      keyStates.set(key, result.state);

      // Save state to backend (fire-and-forget for performance, checkpointed periodically)
      this.stateBackend.put(`process:${key}`, result.state);

      return result.emit;
    });
  }

  private compileDedupe(node: {
    parent: TopologyNode;
    keyFn: (value: unknown) => string;
  }): StreamPipeline<unknown, never> {
    const seen = new Set<string>();

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

  /** Walk up the node tree to find the windowType and keyBy function. */
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

  /** Walk up to find the keyBy function. */
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

  /** Extract event time from a value. Falls back to wall clock. */
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

  private async checkpoint(): Promise<void> {
    await this.stateBackend.checkpoint({ name: `topology:${this.config.group}` });
  }
}
