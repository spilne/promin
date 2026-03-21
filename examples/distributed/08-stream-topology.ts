/**
 * StreamTopology — distributed stream processing primitives
 *
 * StreamPipeline is single-process pull-based. StreamTopology is its
 * distributed counterpart: a declarative processing DAG that runs
 * across partitions with co-located state and automatic checkpointing.
 *
 * Think: Kafka Streams / Flink, with our API style.
 *
 * Key concepts:
 *   StreamTopology.source()  → declare input from any Streamable
 *   .keyBy()                 → repartition by key (keyed processing)
 *   .tumbling() / .sliding() → windowed aggregation
 *   .join()                  → stream-stream join by key
 *   .to()                    → sink output to any Sinkable
 *   .build() + runner.run()  → compile and execute the topology
 */

import {
  StreamTopology,
  TopologyRunner,
  type TimeWindow,
} from "@promin/core";
import { KafkaTopic, type KafkaClient } from "@promin/kafka";
import { InMemoryState } from "@promin/core";

declare const kafka: KafkaClient;

// ---------------------------------------------------------------------------
// Scenario 1: Real-time click analytics
//
// Business flow: Users browse an e-commerce site. Every click is published
// to Kafka. We aggregate clicks per user in 1-minute tumbling windows to
// build a real-time "user engagement score" that powers personalized
// recommendations.
// ---------------------------------------------------------------------------

async function clickAnalytics() {
  const clicks = new KafkaTopic<{ userId: string; page: string; ts: number }>({
    kafka,
    topic: "user.clicks",
    groupId: "click-analytics",
  });

  const engagement = new KafkaTopic<{
    userId: string;
    windowStart: number;
    windowEnd: number;
    clickCount: number;
    uniquePages: number;
  }>({
    kafka,
    topic: "user.engagement",
    groupId: "engagement-consumer",
  });

  // Declarative topology — nothing runs until .run()
  const topology = StreamTopology
    .source(clicks)
    .keyBy((click) => click.userId)
    .tumbling(60_000) // 1-minute windows
    .aggregate({
      init: () => ({ count: 0, pages: new Set<string>() }),
      add: (state, click) => ({
        count: state.count + 1,
        pages: state.pages.add(click.page),
      }),
      emit: (key, window, state) => ({
        userId: key,
        windowStart: window.start,
        windowEnd: window.end,
        clickCount: state.count,
        uniquePages: state.pages.size,
      }),
    })
    .to(engagement)
    .build();

  // Run with automatic checkpointing
  const handle = await TopologyRunner.run(topology, {
    group: "click-analytics",
    checkpointIntervalMs: 30_000,
    stateBackend: new InMemoryState(),
  });

  // Graceful shutdown
  process.on("SIGTERM", () => handle.shutdown());
}

// ---------------------------------------------------------------------------
// Scenario 2: Order enrichment — join orders with customer profiles
//
// Business flow: The orders service publishes raw orders. The CRM publishes
// customer profile updates. We join them by customerId within a 5-minute
// window so downstream services get orders enriched with customer tier,
// name, and loyalty status — without querying the CRM database.
// ---------------------------------------------------------------------------

async function orderEnrichment() {
  const orders = new KafkaTopic<{
    orderId: string;
    customerId: string;
    amount: number;
    items: string[];
  }>({
    kafka,
    topic: "shop.orders",
    groupId: "order-enricher",
  });

  const customers = new KafkaTopic<{
    customerId: string;
    name: string;
    tier: "bronze" | "silver" | "gold";
    loyaltyPoints: number;
  }>({
    kafka,
    topic: "crm.customers",
    groupId: "order-enricher",
  });

  const enrichedOrders = new KafkaTopic<{
    orderId: string;
    customerId: string;
    customerName: string;
    customerTier: string;
    amount: number;
    discount: number;
  }>({
    kafka,
    topic: "shop.enriched-orders",
    groupId: "enriched-consumer",
  });

  const topology = StreamTopology
    .source(orders)
    .keyBy((o) => o.customerId)
    .join(
      StreamTopology.source(customers).keyBy((c) => c.customerId),
      { windowMs: 300_000 }, // 5-minute join window
    )
    .map(({ left: order, right: customer }) => ({
      orderId: order.orderId,
      customerId: order.customerId,
      customerName: customer.name,
      customerTier: customer.tier,
      amount: order.amount,
      discount: customer.tier === "gold" ? 0.15 : customer.tier === "silver" ? 0.1 : 0,
    }))
    .to(enrichedOrders)
    .build();

  await TopologyRunner.run(topology, {
    group: "order-enricher",
    checkpointIntervalMs: 10_000,
  });
}

// ---------------------------------------------------------------------------
// Scenario 3: Fraud detection — sliding window anomaly detection
//
// Business flow: Payment transactions stream in. For each card, we maintain
// a 10-minute sliding window (advancing every minute). If the transaction
// count exceeds 20 or total amount exceeds $5000 in any window, we emit
// a fraud alert. The stateful processor remembers per-card running totals.
// ---------------------------------------------------------------------------

async function fraudDetection() {
  const transactions = new KafkaTopic<{
    txId: string;
    cardId: string;
    amount: number;
    merchant: string;
    ts: number;
  }>({
    kafka,
    topic: "payments.transactions",
    groupId: "fraud-detector",
  });

  const alerts = new KafkaTopic<{
    cardId: string;
    windowStart: number;
    windowEnd: number;
    txCount: number;
    totalAmount: number;
    reason: string;
  }>({
    kafka,
    topic: "fraud.alerts",
    groupId: "alert-consumer",
  });

  const topology = StreamTopology
    .source(transactions)
    .keyBy((tx) => tx.cardId)
    .sliding({ windowMs: 600_000, slideMs: 60_000 }) // 10min window, 1min slide
    .aggregate({
      init: () => ({ count: 0, total: 0 }),
      add: (state, tx) => ({
        count: state.count + 1,
        total: state.total + tx.amount,
      }),
      emit: (cardId, window, state) => ({
        cardId,
        windowStart: window.start,
        windowEnd: window.end,
        txCount: state.count,
        totalAmount: state.total,
        reason:
          state.count > 20
            ? "high_frequency"
            : state.total > 5000
              ? "high_amount"
              : "none",
      }),
    })
    .filter((alert) => alert.reason !== "none")
    .to(alerts)
    .build();

  await TopologyRunner.run(topology, {
    group: "fraud-detector",
    checkpointIntervalMs: 5_000,
  });
}

// ---------------------------------------------------------------------------
// Scenario 4: Stateful dedup + enrichment with per-key state
//
// Business flow: IoT sensors publish readings every second. Many readings
// are duplicates (sensor retries). We deduplicate by sensorId + timestamp,
// then compute a running average temperature per sensor (exponential moving
// average). Downstream services see clean, enriched data.
// ---------------------------------------------------------------------------

async function iotProcessing() {
  const readings = new KafkaTopic<{
    sensorId: string;
    temperature: number;
    humidity: number;
    ts: number;
  }>({
    kafka,
    topic: "iot.raw-readings",
    groupId: "iot-processor",
  });

  const processed = new KafkaTopic<{
    sensorId: string;
    temperature: number;
    movingAvg: number;
    humidity: number;
    ts: number;
  }>({
    kafka,
    topic: "iot.processed",
    groupId: "processed-consumer",
  });

  const topology = StreamTopology
    .source(readings)
    .keyBy((r) => r.sensorId)
    .dedupe((r) => `${r.sensorId}:${r.ts}`) // drop duplicate readings
    .process<{ movingAvg: number }, {
      sensorId: string;
      temperature: number;
      movingAvg: number;
      humidity: number;
      ts: number;
    }>({
      init: () => ({ movingAvg: 0 }),
      process: (state, reading) => {
        // Exponential moving average (α = 0.3)
        const alpha = 0.3;
        const newAvg = state.movingAvg === 0
          ? reading.temperature
          : alpha * reading.temperature + (1 - alpha) * state.movingAvg;

        return {
          state: { movingAvg: newAvg },
          emit: {
            sensorId: reading.sensorId,
            temperature: reading.temperature,
            movingAvg: Math.round(newAvg * 100) / 100,
            humidity: reading.humidity,
            ts: reading.ts,
          },
        };
      },
    })
    .to(processed)
    .build();

  await TopologyRunner.run(topology, {
    group: "iot-processor",
    checkpointIntervalMs: 15_000,
    stateBackend: new InMemoryState(),
  });
}

// ---------------------------------------------------------------------------
// Scenario 5: Session windows — user session analytics
//
// Business flow: A web app publishes page view events. We group them into
// "sessions" — a session ends when the user is inactive for 30 minutes.
// Each completed session is emitted with duration, page count, and the
// entry/exit pages. Marketing uses this for funnel analysis.
// ---------------------------------------------------------------------------

async function sessionAnalytics() {
  const pageViews = new KafkaTopic<{
    userId: string;
    page: string;
    referrer: string;
    ts: number;
  }>({
    kafka,
    topic: "web.pageviews",
    groupId: "session-builder",
  });

  const sessions = new KafkaTopic<{
    userId: string;
    sessionStart: number;
    sessionEnd: number;
    durationMs: number;
    pageCount: number;
    entryPage: string;
    exitPage: string;
  }>({
    kafka,
    topic: "analytics.sessions",
    groupId: "session-consumer",
  });

  const topology = StreamTopology
    .source(pageViews)
    .keyBy((pv) => pv.userId)
    .session(1_800_000) // 30-minute inactivity gap
    .aggregate({
      init: () => ({ pages: [] as { page: string; ts: number }[] }),
      add: (state, pv) => ({
        pages: [...state.pages, { page: pv.page, ts: pv.ts }].sort((a, b) => a.ts - b.ts),
      }),
      emit: (userId, window, state) => ({
        userId,
        sessionStart: window.start,
        sessionEnd: window.end,
        durationMs: window.end - window.start,
        pageCount: state.pages.length,
        entryPage: state.pages[0]!.page,
        exitPage: state.pages[state.pages.length - 1]!.page,
      }),
    })
    .to(sessions)
    .build();

  await TopologyRunner.run(topology, {
    group: "session-builder",
    checkpointIntervalMs: 60_000,
  });
}

// ---------------------------------------------------------------------------
// Scenario 6: Multi-stage topology — ETL pipeline
//
// Business flow: Raw events come from multiple microservices. Stage 1
// validates and normalizes the schema. Stage 2 enriches with geo-IP data.
// Stage 3 aggregates per-region metrics in 5-minute windows. Each stage
// is independently scalable — just run more instances.
// ---------------------------------------------------------------------------

async function multiStageEtl() {
  const rawEvents = new KafkaTopic<{ type: string; payload: string; ip: string }>({
    kafka,
    topic: "ingest.raw",
    groupId: "etl-pipeline",
  });

  const regionMetrics = new KafkaTopic<{
    region: string;
    windowStart: number;
    eventCount: number;
    topEventType: string;
  }>({
    kafka,
    topic: "metrics.by-region",
    groupId: "metrics-consumer",
  });

  const topology = StreamTopology
    .source(rawEvents)
    // Stage 1: validate & normalize
    .filter((e) => e.type !== "" && e.payload !== "")
    .mapAsync(10, async (event) => ({
      type: event.type.toLowerCase(),
      data: JSON.parse(event.payload),
      ip: event.ip,
    }))
    // Stage 2: geo-IP enrichment
    .mapAsync(20, async (event) => ({
      ...event,
      region: await geoIpLookup(event.ip),
    }))
    // Stage 3: aggregate by region in 5-minute windows
    .keyBy((e) => e.region)
    .tumbling(300_000)
    .aggregate({
      init: () => ({ count: 0, types: {} as Record<string, number> }),
      add: (state, event) => ({
        count: state.count + 1,
        types: {
          ...state.types,
          [event.type]: (state.types[event.type] ?? 0) + 1,
        },
      }),
      emit: (region, window, state) => {
        const topType = Object.entries(state.types).sort(([, a], [, b]) => b - a)[0];
        return {
          region,
          windowStart: window.start,
          eventCount: state.count,
          topEventType: topType?.[0] ?? "unknown",
        };
      },
    })
    .to(regionMetrics)
    .build();

  await TopologyRunner.run(topology, {
    group: "etl-pipeline",
    checkpointIntervalMs: 30_000,
  });
}

// ---------------------------------------------------------------------------
// Stubs for compilation
// ---------------------------------------------------------------------------

async function geoIpLookup(_ip: string): Promise<string> {
  return "US";
}

export {
  clickAnalytics,
  orderEnrichment,
  fraudDetection,
  iotProcessing,
  sessionAnalytics,
  multiStageEtl,
};
