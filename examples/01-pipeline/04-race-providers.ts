/**
 * Race 3 AI providers — fastest response wins, others are cancelled.
 * If all premium providers fail, fall back to a cheaper model.
 */

import { Pipeline, CircuitBreaker } from "@promin/core";

interface AiResponse {
  text: string;
  model: string;
}

const openaiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
const anthropicBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
const geminiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });

const callProvider = (url: string, breaker: CircuitBreaker, prompt: string) =>
  Pipeline.fn(() =>
    fetch(url, { method: "POST", body: JSON.stringify({ prompt }) }).then(
      (r) => r.json() as Promise<AiResponse>,
    ),
  )
    .withCircuitBreaker(breaker)
    .timeout(10_000);

async function askAi(prompt: string) {
  return Pipeline.race(
    callProvider("/ai/openai", openaiBreaker, prompt),
    callProvider("/ai/anthropic", anthropicBreaker, prompt),
    callProvider("/ai/gemini", geminiBreaker, prompt),
  )
    .orElsePipeline(() =>
      callProvider(
        "/ai/cheap-fallback",
        new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 60_000 }),
        prompt,
      ).retry(2),
    )
    .runPromise();
}

const response = await askAi("Explain monads in one sentence");
console.log(`${response.model}: ${response.text}`);
