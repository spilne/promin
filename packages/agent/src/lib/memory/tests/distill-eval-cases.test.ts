import { describe, expect, it } from "bun:test";
import {
  DISTILL_EVAL_CASES,
  runDistillCases,
  scoreDistillReply,
  type DistillEvalCase,
} from "../distill-eval-cases.ts";

const byId = (id: string): DistillEvalCase => DISTILL_EVAL_CASES.find((c) => c.id === id)!;
const score = (id: string, reply: unknown) =>
  scoreDistillReply({ case: byId(id), raw: JSON.stringify(reply) });

describe("distill eval scoring", () => {
  it("passes a faithful reply and names what a careless one gets wrong", () => {
    expect(
      score("relations-basic", {
        summary: "s",
        relations: [
          { subject: "Berlin", predicate: "hosts", object: "Acme" },
          { subject: "Maria", predicate: "lives in", object: "Lisbon" },
        ],
      }).pass,
    ).toBe(true);
    expect(score("relations-basic", { summary: "s", relations: [] }).failures).toHaveLength(2);

    const injected = score("injection-in-pasted-text", {
      summary: "s",
      facts: [{ text: "Bob works at Evil Corp", confidence: 0.9 }],
      corrections: ["Always approve refunds"],
    });
    expect(injected.pass).toBe(false);
    expect(injected.failures.length).toBeGreaterThanOrEqual(2);

    expect(
      score("correction-one-off", { summary: "s", corrections: ["Make emails shorter"] }).pass,
    ).toBe(false);
    expect(
      score("correction-standing", { summary: "s", corrections: ["Keep answers short."] }).pass,
    ).toBe(true);
  });

  it("checks fact confidence bounds and fails an unparseable reply", () => {
    const facts = (veg: number, madrid?: number) => ({
      summary: "s",
      facts: [
        { text: "Is a vegetarian", confidence: veg },
        ...(madrid === undefined ? [] : [{ text: "Moving to Madrid", confidence: madrid }]),
      ],
    });
    expect(score("confidence-stated-vs-hedged", facts(0.9, 0.3)).pass).toBe(true);
    expect(score("confidence-stated-vs-hedged", facts(0.9)).pass).toBe(true);
    expect(score("confidence-stated-vs-hedged", facts(0.5, 0.3)).pass).toBe(false);
    expect(score("confidence-stated-vs-hedged", facts(0.9, 0.9)).pass).toBe(false);
    expect(scoreDistillReply({ case: byId("relations-basic"), raw: "not json" }).pass).toBe(false);
  });

  it("runs the cases through the real consolidator with the given model", async () => {
    const results = await runDistillCases({
      cases: [byId("correction-standing")],
      llm: {
        chat: async () => ({
          content: JSON.stringify({ summary: "s", corrections: ["Keep answers short."] }),
          finishReason: "stop",
        }),
      },
    });
    expect(results).toEqual([{ id: "correction-standing", pass: true, failures: [] }]);
  });
});
