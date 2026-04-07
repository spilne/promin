/**
 * Consume payment events, enrich with user data, produce to output topic.
 * Batched commits every 5s — no per-message commit overhead.
 */

import { KafkaTopic, type KafkaClient } from "@promin/kafka";

declare const kafka: KafkaClient;

interface PaymentEvent {
  txId: string;
  userId: string;
  amount: number;
}

interface EnrichedPayment extends PaymentEvent {
  userName: string;
  riskScore: number;
}

const payments = new KafkaTopic<PaymentEvent>({
  kafka,
  topic: "payments",
  groupId: "enricher",
});

const enriched = new KafkaTopic<EnrichedPayment>({
  kafka,
  topic: "enriched-payments",
  groupId: "enriched-consumer",
});

await payments
  .subscribeAck({ commitIntervalMs: 5_000 })
  .parAsyncMap(10, async (envelope) => {
    const user = await fetchUser(envelope.value.userId);
    const risk = await scoreRisk(envelope.value);

    await enriched.publish({
      ...envelope.value,
      userName: user.name,
      riskScore: risk,
    });

    await envelope.ack();
  })
  .drain();

// Stubs
async function fetchUser(_id: string) {
  return { name: "Alice" };
}
async function scoreRisk(_event: PaymentEvent) {
  return 0.1;
}
