/**
 * Call an external payment API with circuit breaker.
 * After 3 consecutive failures, the circuit opens and rejects instantly
 * for 30s — no wasted requests to a dead service.
 */

import { Pipeline, CircuitBreaker } from "@promin/core";

const paymentBreaker = new CircuitBreaker({
  failureThreshold: 3,
  resetTimeoutMs: 30_000,
});

interface ChargeResult {
  txId: string;
  amount: number;
}

const chargeCard = (amount: number) =>
  Pipeline.fn(async () => {
    const res = await fetch("/api/payments/charge", {
      method: "POST",
      body: JSON.stringify({ amount }),
    });
    if (!res.ok) throw new Error(`Payment failed: ${res.status}`);
    return (await res.json()) as ChargeResult;
  })
    .withCircuitBreaker(paymentBreaker)
    .retry({ maxRetries: 2, when: (err) => err.message.includes("500") })
    .timeout(10_000);

const { data, error } = await chargeCard(99.99).runSafe();

if (data) {
  console.log(`Charged ${data.amount}, tx: ${data.txId}`);
} else {
  console.log(`Payment failed: ${error}`);
}
