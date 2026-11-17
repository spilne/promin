import { describe, expect, it } from "bun:test";

import { createSystemOneDecisionProvider } from "../system-one-provider.ts";

const questions = {
  team: {
    type: "choice",
    instructions: "Which support team should handle this?",
    options: [{ name: "billing" }, { name: "technical" }],
  },
  urgency: {
    type: "score",
    instructions: "How urgent is the request?",
    levels: ["low", "medium", "high"],
  },
  refund: { type: "yesno", instructions: "Should the extra charge be refunded?" },
} as const;

async function checkServer(options: {
  readonly baseUrl: string;
  readonly model: string;
  readonly apiKey?: string;
}): Promise<void> {
  const result = await createSystemOneDecisionProvider(options).decide({
    state: "I was charged twice. Please refund the extra charge.",
    questions,
  });
  expect(result.model).toBe(options.model);
  expect(Object.keys(result.answers).sort()).toEqual(["refund", "team", "urgency"]);
}

const key = process.env.TYPESAFE_API_KEY;
describe.skipIf(!key)("hosted Jev", () => {
  it("answers one question of each type", async () => {
    await checkServer({
      baseUrl: "https://api.typesafe.ai",
      model: "jev-1.14.0",
      apiKey: key,
    });
  });
});

const local = process.env.SYSTEM_ONE_URL;
describe.skipIf(!local)("a local System One server", () => {
  it("answers one question of each type", async () => {
    await checkServer({
      baseUrl: local ?? "",
      model: process.env.SYSTEM_ONE_MODEL ?? "tev1",
    });
  });
});
