import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";

import { LLMTimeoutError } from "../../adapters/llm-http.ts";
import { fakeFetch, hangingResponse, jsonResponse } from "../../adapters/tests/fake-fetch.ts";
import { decideWithRetry } from "../decide-with-retry.ts";
import { createSystemOneDecisionProvider, type SystemOneOptions } from "../system-one-provider.ts";
import { DecisionAnswerError, type DecisionProvider, type DecisionRequest } from "../types.ts";

const questions = {
  team: {
    type: "choice",
    instructions: "Which team?",
    options: [{ name: "billing" }, { name: "tech" }],
  },
  urgency: {
    type: "score",
    instructions: "How urgent?",
    levels: ["low", "mid", "high"],
  },
  refund: { type: "yesno", instructions: "Refund?" },
} as const;

const request: DecisionRequest = {
  state: { ticket: "charged twice" },
  questions,
};

const wireResponse = {
  model: "tev1-0.3",
  answers: {
    team: {
      type: "choice",
      choice: "billing",
      confidence: 0.9,
      probabilities: { billing: 0.9, tech: 0.1 },
    },
    urgency: {
      type: "score",
      score: 1.5,
      confidence: 0.6,
      legend: { "0": "low", "1": "mid", "2": "high" },
      probabilities: { "0": 0.1, "1": 0.3, "2": 0.6 },
    },
    refund: { type: "noul", noul: 0.97 },
  },
  usage: { input_tokens: 42, output_tokens: 0 },
};

function providerOptions(fetch: NonNullable<SystemOneOptions["fetch"]>): SystemOneOptions {
  return { baseUrl: "http://localhost:11434/", model: "tev1", apiKey: "k", fetch };
}

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 1_000 && !condition(); attempt++) {
    await Promise.resolve();
  }
  if (!condition()) throw new Error("condition was not observed");
}

describe("System One decision provider", () => {
  it("maps a request to the wire format", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(wireResponse));

    await createSystemOneDecisionProvider(providerOptions(fetch)).decide(request);

    expect(requests[0]!.url).toBe("http://localhost:11434/v1/systemone");
    expect(requests[0]!.body).toEqual({
      model: "tev1",
      state: { ticket: "charged twice" },
      questions: {
        team: {
          type: "choice",
          instructions: "Which team?",
          criteria: { billing: null, tech: null },
        },
        urgency: {
          type: "score",
          instructions: "How urgent?",
          criteria: ["low", "mid", "high"],
        },
        refund: { type: "noul", instructions: "Refund?" },
      },
    });
  });

  it("maps the response to checked answers", async () => {
    const { fetch } = fakeFetch(() => jsonResponse(wireResponse));

    await expect(
      createSystemOneDecisionProvider(providerOptions(fetch)).decide(request),
    ).resolves.toEqual({
      answers: {
        team: {
          choice: "billing",
          probability: 0.9,
          probabilities: { billing: 0.9, tech: 0.1 },
          confidence: 0.9,
          probabilitySource: "model",
        },
        urgency: {
          score: 1.5,
          normalized: 0.75,
          probabilities: [0.1, 0.3, 0.6],
          confidence: 0.6,
          probabilitySource: "model",
        },
        refund: { yes: 0.97, probabilitySource: "model" },
      },
      model: "tev1-0.3",
      usage: { inputTokens: 42, outputTokens: 0 },
    });
  });

  it("sends authorization only when a key is configured", async () => {
    const authorizations: Array<string | null> = [];
    const fetch: NonNullable<SystemOneOptions["fetch"]> = async (_url, init) => {
      authorizations.push(new Headers(init.headers).get("authorization"));
      return jsonResponse(wireResponse);
    };

    await createSystemOneDecisionProvider(providerOptions(fetch)).decide(request);
    await createSystemOneDecisionProvider({
      baseUrl: "http://localhost:11434",
      model: "tev1",
      fetch,
    }).decide(request);

    expect(authorizations).toEqual(["Bearer k", null]);
  });

  it("rejects a malformed response", async () => {
    const malformed = {
      ...wireResponse,
      answers: {
        ...wireResponse.answers,
        team: { ...wireResponse.answers.team, choice: "sales" },
      },
    };
    const { fetch } = fakeFetch(() => jsonResponse(malformed));

    await expect(
      createSystemOneDecisionProvider(providerOptions(fetch)).decide(request),
    ).rejects.toBeInstanceOf(DecisionAnswerError);
  });

  it("times out a server that never answers", async () => {
    const clock = FakeWallClock.create(0);
    const { fetch } = fakeFetch(hangingResponse);
    const pending = createSystemOneDecisionProvider({
      ...providerOptions(fetch),
      clock,
      timeoutMs: 1_000,
    })
      .decide(request)
      .catch((error: unknown) => error);

    await waitUntil(() => clock.pendingCount() > 0);
    clock.advance(1_000);

    expect(await pending).toBeInstanceOf(LLMTimeoutError);
  });
});

describe("decideWithRetry", () => {
  it("retries a transient failure, then gives up naming the server", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const { fetch } = fakeFetch(() => {
      calls += 1;
      throw new TypeError("fetch failed");
    });
    const inner = createSystemOneDecisionProvider({
      baseUrl: "http://127.0.0.1:1",
      model: "tev1",
      fetch,
      clock,
    });
    let failures = 0;
    const provider: DecisionProvider = {
      async decide(nextRequest) {
        try {
          return await inner.decide(nextRequest);
        } catch (error) {
          failures += 1;
          throw error;
        }
      },
    };
    const pending = decideWithRetry({ provider, request, clock }).catch((error: unknown) => error);

    await waitUntil(() => failures === 1 && clock.pendingCount() > 0);
    clock.advance(250);
    await waitUntil(() => failures === 2 && clock.pendingCount() > 0);
    clock.advance(500);
    const error = await pending;

    expect(calls).toBe(3);
    expect(String(error)).toContain("http://127.0.0.1:1");
  });

  it("does not retry a 400", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const { fetch } = fakeFetch(() => {
      calls += 1;
      return jsonResponse({ detail: "bad request" }, { status: 400 });
    });
    const provider = createSystemOneDecisionProvider({
      baseUrl: "http://localhost:11434",
      model: "tev1",
      fetch,
      clock,
    });

    await expect(decideWithRetry({ provider, request, clock })).rejects.toThrow("400");
    expect(calls).toBe(1);
  });

  it("stops waiting when the request is aborted", async () => {
    const clock = FakeWallClock.create(0);
    const abort = new AbortController();
    let calls = 0;
    const { fetch } = fakeFetch(() => {
      calls += 1;
      throw new TypeError("fetch failed");
    });
    const provider = createSystemOneDecisionProvider({
      baseUrl: "http://localhost:11434",
      model: "tev1",
      fetch,
      clock,
    });
    const pending = decideWithRetry({
      provider,
      request: { ...request, signal: abort.signal },
      clock,
    });

    await waitUntil(() => clock.pendingCount() > 0);
    abort.abort(new Error("cancelled"));

    await expect(pending).rejects.toThrow("cancelled");
    expect(calls).toBe(1);
  });
});
