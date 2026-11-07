import { describe, expect, it } from "bun:test";
import { newEpisode, newFact } from "../record-factories.ts";

describe("record factories", () => {
  it("build active facts and episodes with defaults, clamping salience and confidence", () => {
    const fact = newFact({ input: { text: "likes tea", source: "model" }, now: 5 });
    expect(fact).toMatchObject({
      text: "likes tea",
      createdAt: 5,
      updatedAt: 5,
      retiredAt: null,
      supersededBy: null,
      confidence: 0.6,
    });
    expect(newFact({ input: { text: "x", source: "model" }, now: 5 }).id).not.toBe(fact.id);

    const at = (salience?: number) =>
      newEpisode({ input: { summary: "s", ...(salience !== undefined && { salience }) }, now: 9 });
    expect(at().salience).toBe(0.5);
    expect(at(Number.NaN).salience).toBe(0.5);
    expect(at(7).salience).toBe(1);
    expect(at(-1).salience).toBe(0);
    expect(at()).toMatchObject({ occurredAt: 9, createdAt: 9, outcome: null, metadata: {} });
  });
});
