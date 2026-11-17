import { describe, expect, it } from "bun:test";

import { scriptedDecisionProvider } from "../../testing/scripted-decision-provider.ts";
import { checkAnswers, DecisionAnswerError, type DecisionAnswer } from "../types.ts";

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

const good = {
  team: {
    choice: "billing",
    probability: 0.9,
    probabilities: { billing: 0.9, tech: 0.1 },
    probabilitySource: "model",
  },
  urgency: {
    score: 1.5,
    normalized: 0.75,
    probabilities: [0.1, 0.3, 0.6],
    probabilitySource: "model",
  },
  refund: { yes: 0.97, probabilitySource: "model" },
} as const;

const invalidAnswers: ReadonlyArray<readonly [string, Readonly<Record<string, DecisionAnswer>>]> = [
  ["a missing question", { team: good.team, urgency: good.urgency }],
  ["an extra question", { ...good, extra: good.refund }],
  ["a choice outside the options", { ...good, team: { ...good.team, choice: "sales" } }],
  ["a probability above 1", { ...good, refund: { yes: 1.4, probabilitySource: "model" } }],
  ["a NaN probability", { ...good, refund: { yes: Number.NaN, probabilitySource: "model" } }],
  [
    "probabilities that do not sum to 1",
    { ...good, team: { ...good.team, probabilities: { billing: 0.9, tech: 0.5 } } },
  ],
  [
    "a picked probability that does not match",
    { ...good, team: { ...good.team, probability: 0.5 } },
  ],
  [
    "the wrong number of levels",
    { ...good, urgency: { ...good.urgency, probabilities: [0.5, 0.5] } },
  ],
  ["the wrong normalized score", { ...good, urgency: { ...good.urgency, normalized: 0.5 } }],
];

describe("checkAnswers", () => {
  it("accepts well-formed answers", () => {
    expect(checkAnswers({ questions, answers: good })).toEqual(good);
  });

  it.each(invalidAnswers)("rejects %s", (_name, answers) => {
    expect(() => checkAnswers({ questions, answers })).toThrow(DecisionAnswerError);
  });

  it("names the invalid question", () => {
    expect(() =>
      checkAnswers({
        questions,
        answers: { ...good, refund: { yes: 2, probabilitySource: "model" } },
      }),
    ).toThrow("refund");
  });
});

describe("scriptedDecisionProvider", () => {
  it("records requests and returns checked scripted answers", async () => {
    const provider = scriptedDecisionProvider(() => good, { model: "scripted-v2" });
    const request = { state: { ticket: "charged twice" }, questions };

    await expect(provider.decide(request)).resolves.toEqual({
      answers: good,
      model: "scripted-v2",
    });
    expect(provider.calls).toEqual([request]);
  });
});
