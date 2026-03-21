/**
 * Kafka event processing — consume, transform, produce with exactly-once semantics
 *
 * Business flow:
 * 1. Payment service publishes transaction events to a Kafka topic
 * 2. Fraud detection consumer reads each transaction with manual ack
 * 3. Each transaction is scored against fraud rules (amount, frequency, location)
 * 4. Legitimate transactions are forwarded to the "approved" topic
 * 5. Suspicious transactions are routed to the "review" topic for human investigation
 * 6. Offsets are committed only after successful processing — no lost events
 * 7. If a consumer crashes, uncommitted messages are redelivered to another consumer in the group
 *
 * Kafka handles the partitioning — transactions for the same user always go to the same partition,
 * ensuring ordering per-user while allowing parallel processing across users.
 */

import { KafkaTopic, type KafkaClient } from "@promin/kafka";

// Use any kafkajs-compatible client:
// import { Kafka } from "@confluentinc/kafka-javascript/kafkajs";
// import { Kafka } from "kafkajs";
// const kafka = new Kafka({ brokers: [...] });
declare const kafka: KafkaClient;

// ---------------------------------------------------------------------------
// Topics — typed, with streaming typeclasses
// ---------------------------------------------------------------------------

interface Transaction {
  txId: string;
  userId: string;
  amount: number;
  currency: string;
  merchantId: string;
  country: string;
  timestamp: string;
}

interface FraudDecision {
  txId: string;
  userId: string;
  amount: number;
  score: number;
  decision: "approved" | "review" | "blocked";
  reasons: string[];
}

// Source: raw transaction events from payment service
const transactions = new KafkaTopic<Transaction>({
  kafka,
  topic: "payment.transactions",
  groupId: "fraud-detection",
});

// Sink: approved transactions
const approved = new KafkaTopic<FraudDecision>({
  kafka,
  topic: "fraud.approved",
  groupId: "fraud-approved-consumer",
});

// Sink: flagged for review
const review = new KafkaTopic<FraudDecision>({
  kafka,
  topic: "fraud.review",
  groupId: "fraud-review-consumer",
});

// ---------------------------------------------------------------------------
// Fraud scoring logic
// ---------------------------------------------------------------------------

function scoreFraud(tx: Transaction): FraudDecision {
  const reasons: string[] = [];
  let score = 0;

  // High amount
  if (tx.amount > 5000) {
    score += 30;
    reasons.push(`high amount: $${tx.amount}`);
  }

  // International transaction
  if (tx.country !== "US") {
    score += 20;
    reasons.push(`international: ${tx.country}`);
  }

  // Round amount (common in fraud)
  if (tx.amount % 100 === 0 && tx.amount > 500) {
    score += 15;
    reasons.push("round amount");
  }

  const decision =
    score >= 50 ? "blocked" :
    score >= 25 ? "review" :
    "approved";

  return {
    txId: tx.txId,
    userId: tx.userId,
    amount: tx.amount,
    score,
    decision,
    reasons,
  };
}

// ---------------------------------------------------------------------------
// Consumer: manual ack with offset commit
// ---------------------------------------------------------------------------

async function startFraudDetector() {
  console.log("Starting fraud detection consumer...\n");

  // subscribeAck gives us manual control over when offsets are committed.
  // If we crash before ack(), the message is redelivered.
  await transactions.subscribeAck()
    .forEach(async (envelope) => {
      const tx = envelope.value;
      const decision = scoreFraud(tx);

      // Route based on decision
      if (decision.decision === "approved") {
        // Keyed publish — same userId always goes to same partition
        await approved.publish(decision, { key: tx.userId });
      } else {
        await review.publish(decision, { key: tx.userId });
        console.log(`FLAGGED: tx=${tx.txId} score=${decision.score} reasons=${decision.reasons.join(", ")}`);
      }

      // Commit offset AFTER processing — exactly-once semantics
      // If we crash here, this message will be reprocessed (at-least-once)
      await envelope.ack();
    });
}

// ---------------------------------------------------------------------------
// Producer: simulate payment events
// ---------------------------------------------------------------------------

async function simulatePayments() {
  const txs: Transaction[] = [
    { txId: "tx_001", userId: "u_1", amount: 49.99, currency: "USD", merchantId: "m_coffee", country: "US", timestamp: new Date().toISOString() },
    { txId: "tx_002", userId: "u_2", amount: 8500, currency: "USD", merchantId: "m_electronics", country: "US", timestamp: new Date().toISOString() },
    { txId: "tx_003", userId: "u_3", amount: 200, currency: "EUR", merchantId: "m_online", country: "DE", timestamp: new Date().toISOString() },
    { txId: "tx_004", userId: "u_1", amount: 5000, currency: "USD", merchantId: "m_unknown", country: "NG", timestamp: new Date().toISOString() },
    { txId: "tx_005", userId: "u_4", amount: 12.50, currency: "USD", merchantId: "m_grocery", country: "US", timestamp: new Date().toISOString() },
  ];

  for (const tx of txs) {
    // Key by userId — ensures ordering per user across partitions
    await transactions.publish(tx, { key: tx.userId });
    console.log(`Published: tx=${tx.txId} amount=$${tx.amount} country=${tx.country}`);
  }
}

// ---------------------------------------------------------------------------
// Replay: reprocess from a point in time (e.g., after fixing a bug)
// ---------------------------------------------------------------------------

async function replayFromYesterday() {
  const yesterday = Date.now() - 24 * 60 * 60 * 1000;

  console.log("Replaying transactions from yesterday...\n");

  // subscribeFrom seeks to the timestamp — all messages after that point are reprocessed
  const count = await transactions
    .subscribeFrom({ offset: { type: "timestamp", value: yesterday } })
    .map(scoreFraud)
    .filter((d) => d.decision !== "approved")
    .take(100) // limit for safety
    .reduce(0, (n) => n + 1);

  console.log(`Reprocessed ${count} suspicious transactions`);
}

// ---------------------------------------------------------------------------
// Offset management: check consumer group progress
// ---------------------------------------------------------------------------

async function checkConsumerLag() {
  const committedOffset = await transactions.getCommittedOffset({ group: "fraud-detection" });
  console.log(`Last committed offset: ${committedOffset ?? "none"}`);
}

export { startFraudDetector, simulatePayments, replayFromYesterday, checkConsumerLag };
