// ---------------------------------------------------------------------------
// commitBatchWithin — fs2-kafka-style batched offset commit pipe
//
// Usage:
//   topic.subscribeAck()
//     .parAsyncMap(25, async (env) => {
//       await process(env.value);
//       return env;
//     })
//     .through(commitBatchWithin(500, 15_000))
//     .drain();
//
// Batches ack'd envelopes by count or time, commits offsets as a group.
// Uses OffsetTracker internally for parallel-safe contiguous commits.
// ---------------------------------------------------------------------------

import type { StreamPipeline } from "@promin/core";
import type { Envelope } from "@promin/core";
import { OffsetTracker } from "./offset-tracker.ts";
import type { KafkaConsumer, KafkaOffsetCommit } from "./kafka-types.ts";

export interface CommitBatchWithinConfig {
  /** Commit after this many messages. */
  maxBatchSize: number;
  /** Commit after this many ms, even if batch isn't full. */
  maxWaitMs: number;
  /** Kafka consumer to commit offsets on. */
  consumer: KafkaConsumer;
  /** Topic name (for offset commits). */
  topic: string;
}

/**
 * Pipe that batches envelope acks and commits offsets to Kafka.
 *
 * Collects processed envelopes, tracks contiguous offsets via OffsetTracker,
 * and flushes commits when either `maxBatchSize` or `maxWaitMs` is reached.
 *
 * The pipe unwraps `Envelope<T>` → `T`, so downstream sees plain values.
 *
 * @example
 * ```ts
 * import { commitBatchWithin } from "@promin/kafka";
 *
 * topic.subscribeAck()
 *   .parAsyncMap(25, async (env) => {
 *     await processOrder(env.value);
 *     return env;
 *   })
 *   .through(commitBatchWithin({
 *     maxBatchSize: 500,
 *     maxWaitMs: 15_000,
 *     consumer: myConsumer,
 *     topic: "orders",
 *   }))
 *   .drain();
 * ```
 */
export function commitBatchWithin<T>(
  config: CommitBatchWithinConfig,
): (stream: StreamPipeline<Envelope<T>, never>) => StreamPipeline<T, never> {
  return (stream) => {
    const tracker = new OffsetTracker();
    let pendingCount = 0;
    let lastFlush = Date.now();
    let flushTimer: ReturnType<typeof setInterval> | undefined;

    const flush = async () => {
      const committable = tracker.committable();
      if (committable.size === 0) return;

      const offsets: KafkaOffsetCommit[] = [...committable.entries()].map(
        ([partition, offset]) => ({
          topic: config.topic,
          partition,
          offset: offset.toString(),
        }),
      );

      try {
        await config.consumer.commitOffsets(offsets);
        pendingCount = 0;
        lastFlush = Date.now();
      } catch {
        // Commit failed — will retry on next flush
      }
    };

    // Start periodic flush timer
    flushTimer = setInterval(
      async () => {
        if (Date.now() - lastFlush >= config.maxWaitMs) {
          await flush();
        }
      },
      Math.min(config.maxWaitMs, 1000),
    );

    return stream
      .mapAsync(async (env) => {
        const partition = (env.metadata.partition as number) ?? 0;
        const offset = Number(env.metadata.offset ?? 0);

        tracker.complete(partition, offset);
        pendingCount++;

        // Flush on batch size
        if (pendingCount >= config.maxBatchSize) {
          await flush();
        }

        return env.value;
      })
      .onFinalize(async () => {
        // Final flush on stream end
        if (flushTimer) clearInterval(flushTimer);
        await flush();
      });
  };
}

/**
 * Simplified commitBatchWithin that works with the KafkaTopic directly.
 * Extracts consumer and topic from the envelope metadata.
 *
 * @example
 * ```ts
 * topic.subscribeAck()
 *   .parAsyncMap(25, async (env) => {
 *     await process(env.value);
 *     return env;
 *   })
 *   .through(autoCommitBatchWithin(500, 15_000))
 *   .drain();
 * ```
 */
export function autoCommitBatchWithin<T>(
  maxBatchSize: number,
  maxWaitMs: number,
): (stream: StreamPipeline<Envelope<T>, never>) => StreamPipeline<T, never> {
  return (stream) => {
    let pendingCount = 0;
    let lastFlush = Date.now();
    let flushTimer: ReturnType<typeof setInterval> | undefined;

    // Capture ack functions and batch them
    const pendingAcks: (() => Promise<void>)[] = [];

    const flush = async () => {
      // Ack all pending envelopes — the tracker ensures contiguous commit
      for (const ack of pendingAcks) {
        await ack();
      }
      pendingAcks.length = 0;
      pendingCount = 0;
      lastFlush = Date.now();
    };

    flushTimer = setInterval(
      async () => {
        if (Date.now() - lastFlush >= maxWaitMs && pendingCount > 0) {
          await flush();
        }
      },
      Math.min(maxWaitMs, 1000),
    );

    return stream
      .mapAsync(async (env) => {
        pendingAcks.push(() => env.ack());
        pendingCount++;

        if (pendingCount >= maxBatchSize) {
          await flush();
        }

        return env.value;
      })
      .onFinalize(async () => {
        if (flushTimer) clearInterval(flushTimer);
        await flush();
      });
  };
}
