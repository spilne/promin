/**
 * fs2-kafka / kafka4s patterns translated to @promin/kafka
 *
 * fs2-kafka is the gold standard for functional Kafka in Scala.
 * This file shows each core pattern and its equivalent in our API.
 *
 * Key concepts that map directly:
 *   commitBatchWithin   → OffsetTracker + commitIntervalMs
 *   mapAsync(n)         → parAsyncMap(n)
 *   ProducerRecords     → publish / publishBatch
 *   through(pipe)       → through(fn) / parAsyncMap
 */

import { KafkaTopic, type KafkaClient } from "@promin/kafka";

declare const kafka: KafkaClient;

// ---------------------------------------------------------------------------
// Pattern 1: Consume → Process → Commit (batched)
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("orders")
//     .records
//     .mapAsync(25)(record => processOrder(record.value).as(record.offset))
//     .through(commitBatchWithin(500, 15.seconds))
//
// Ours:
async function consumeProcessCommit() {
  const orders = new KafkaTopic<{ orderId: string; amount: number }>({
    kafka,
    topic: "orders",
    groupId: "order-processor",
  });

  await orders
    .subscribeAck({ commitIntervalMs: 15_000 }) // batch commits every 15s
    .parAsyncMap(25, async (envelope) => {
      await processOrder(envelope.value);
      await envelope.ack(); // tracked by OffsetTracker, flushed on interval
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 2: Consume → Transform → Produce (pipe through)
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("raw-events")
//     .records
//     .map(record => ProducerRecord("enriched-events", enrichEvent(record.value)))
//     .through(KafkaProducer.pipe(producerSettings))
//     .through(commitBatchWithin(500, 5.seconds))
//
// Ours:
async function consumeTransformProduce() {
  const rawEvents = new KafkaTopic<{ type: string; data: unknown }>({
    kafka,
    topic: "raw-events",
    groupId: "enricher",
  });

  const enrichedEvents = new KafkaTopic<{ type: string; data: unknown; enrichedAt: string }>({
    kafka,
    topic: "enriched-events",
    groupId: "enriched-consumer",
  });

  await rawEvents
    .subscribeAck({ commitIntervalMs: 5_000 })
    .parAsyncMap(10, async (envelope) => {
      const enriched = {
        ...envelope.value,
        enrichedAt: new Date().toISOString(),
      };

      // Produce to output topic
      await enrichedEvents.publish(enriched);

      // Ack input — offset tracked, committed in batch
      await envelope.ack();
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 3: Fan-out — one input, multiple outputs
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("events")
//     .records
//     .evalMap { record =>
//       record.value.eventType match {
//         case "order" => orderProducer.produce(...)
//         case "user"  => userProducer.produce(...)
//       }
//     }
//
// Ours:
async function fanOut() {
  const events = new KafkaTopic<{ eventType: string; payload: unknown }>({
    kafka,
    topic: "events",
    groupId: "router",
  });

  const orderTopic = new KafkaTopic<unknown>({ kafka, topic: "orders", groupId: "x" });
  const userTopic = new KafkaTopic<unknown>({ kafka, topic: "users", groupId: "x" });

  await events
    .subscribeAck()
    .parAsyncMap(10, async (envelope) => {
      const { eventType, payload } = envelope.value;

      switch (eventType) {
        case "order":
          await orderTopic.publish(payload);
          break;
        case "user":
          await userTopic.publish(payload);
          break;
      }

      await envelope.ack();
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 4: Windowed aggregation — batch by time
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("clicks")
//     .records
//     .groupWithin(1000, 10.seconds)
//     .evalMap(batch => writeBatch(batch.toList.map(_.value)))
//     .through(commitBatchWithin(100, 5.seconds))
//
// Ours:
async function windowedAggregation() {
  const clicks = new KafkaTopic<{ userId: string; page: string; ts: number }>({
    kafka,
    topic: "clicks",
    groupId: "analytics",
  });

  await clicks
    .subscribeAck({ commitIntervalMs: 5_000 })
    .groupWithin(1000, 10_000) // batch: 1000 items or 10s
    .parAsyncMap(3, async (batch) => {
      // Bulk write to analytics DB
      const values = batch.map((env) => env.value);
      await writeBatch(values);

      // Ack all messages in the batch
      for (const env of batch) {
        await env.ack();
      }
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 5: Stateful deduplication
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("payments")
//     .records
//     .evalMapAccumulate(Set.empty[String]) { (seen, record) =>
//       if (seen.contains(record.value.txId)) (seen, None)
//       else (seen + record.value.txId, Some(record))
//     }
//     .unNone
//
// Ours:
async function statefulDedup() {
  const payments = new KafkaTopic<{ txId: string; amount: number }>({
    kafka,
    topic: "payments",
    groupId: "dedup-processor",
  });

  await payments
    .subscribeAck()
    .distinctBy((env) => env.value.txId) // dedup by transaction ID
    .parAsyncMap(5, async (envelope) => {
      await processPayment(envelope.value);
      await envelope.ack();
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 6: Dead letter + retry
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("jobs")
//     .records
//     .evalMap { record =>
//       processJob(record.value)
//         .handleErrorWith(e => dlqProducer.produce(record.value, e))
//     }
//
// Ours:
async function deadLetterRetry() {
  const jobs = new KafkaTopic<{ jobId: string; payload: unknown }>({
    kafka,
    topic: "jobs",
    groupId: "worker",
  });

  const dlq = new KafkaTopic<{ jobId: string; error: string; original: unknown }>({
    kafka,
    topic: "jobs-dlq",
    groupId: "dlq-consumer",
  });

  await jobs
    .subscribeAck()
    .parAsyncMap(10, async (envelope) => {
      try {
        await processJob(envelope.value);
      } catch (err) {
        // Send to DLQ instead of crashing
        await dlq.publish({
          jobId: envelope.value.jobId,
          error: err instanceof Error ? err.message : String(err),
          original: envelope.value,
        });
      }
      await envelope.ack();
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Pattern 7: Keyed processing — partition-local state
// ---------------------------------------------------------------------------
// fs2-kafka:
//   consumer.stream
//     .subscribeTo("user-actions")
//     .partitionedRecords
//     .map(_.evalMap(record => updateUserState(record.key, record.value)))
//     .parJoinUnbounded
//
// Ours:
async function keyedProcessing() {
  const actions = new KafkaTopic<{ action: string; data: unknown }>({
    kafka,
    topic: "user-actions",
    groupId: "state-manager",
  });

  // Each partition processes independently — same user always on same partition
  await actions
    .subscribeAck()
    .parAsyncMap(12, async (envelope) => {
      const userId = (envelope.metadata as any).key;
      await updateUserState(userId, envelope.value);
      await envelope.ack();
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Stubs for compilation
// ---------------------------------------------------------------------------

async function processOrder(_order: unknown): Promise<void> {}
async function writeBatch(_values: unknown[]): Promise<void> {}
async function processPayment(_payment: unknown): Promise<void> {}
async function processJob(_job: unknown): Promise<void> {}
async function updateUserState(_userId: string, _action: unknown): Promise<void> {}

export {
  consumeProcessCommit,
  consumeTransformProduce,
  fanOut,
  windowedAggregation,
  statefulDedup,
  deadLetterRetry,
  keyedProcessing,
};
