import { describe, it, expect } from "bun:test";
import { runCouncil, createCouncilTool, formatCouncilResult } from "../council.ts";
import type { LLMProvider } from "../llm-provider.ts";

// ---- mock LLM ----

interface CallRecord {
  systemPrompt: string | undefined;
  userContent: string;
}

function makeLlm(response: string): LLMProvider & { calls: CallRecord[] } {
  const calls: CallRecord[] = [];
  return {
    calls,
    async chat(params) {
      const sys = params.messages.find((m) => m.role === "system")?.content;
      const user = params.messages.find((m) => m.role === "user")?.content ?? "";
      calls.push({ systemPrompt: sys, userContent: user });
      return { content: response, finishReason: "stop" };
    },
  };
}

// ---- fixtures ----

const QUESTION = "Should we use microservices or a monolith?";

function makeCouncil() {
  const advocate = makeLlm("microservices scale better");
  const skeptic = makeLlm("monolith is simpler to start");
  const synth = makeLlm("start monolith, plan for microservices later");
  return { advocate, skeptic, synth };
}

// ---- tests ----

describe("runCouncil", () => {
  it("calls each councilor once in a single round", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(advocate.calls).toHaveLength(1);
    expect(skeptic.calls).toHaveLength(1);
    expect(synth.calls).toHaveLength(1);
  });

  it("passes the question to each councilor", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(advocate.calls[0]!.userContent).toContain(QUESTION);
    expect(skeptic.calls[0]!.userContent).toContain(QUESTION);
  });

  it("returns verdict from synthesizer", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const result = await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(result.verdict).toBe("start monolith, plan for microservices later");
  });

  it("includes one round with contributions from all councilors", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const result = await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(result.rounds).toHaveLength(1);
    expect(result.rounds[0]!.contributions).toHaveLength(2);
    expect(result.rounds[0]!.contributions[0]!.councilor).toBe("Alice");
    expect(result.rounds[0]!.contributions[1]!.councilor).toBe("Bob");
  });

  it("runs two rounds when rounds=2", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const result = await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
      rounds: 2,
    });
    expect(result.rounds).toHaveLength(2);
    expect(advocate.calls).toHaveLength(2);
    expect(skeptic.calls).toHaveLength(2);
  });

  it("includes others' round 1 output in round 2 prompts", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
      rounds: 2,
    });
    // Alice's round 2 prompt should contain Bob's round 1 output
    const aliceRound2 = advocate.calls[1]!.userContent;
    expect(aliceRound2).toContain("monolith is simpler to start");
    // Bob's round 2 prompt should contain Alice's round 1 output
    const bobRound2 = skeptic.calls[1]!.userContent;
    expect(bobRound2).toContain("microservices scale better");
  });

  it("includes own prior output separately from others in round 2+", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
      rounds: 2,
    });
    const aliceRound2 = advocate.calls[1]!.userContent;
    // Alice sees her own round-1 output under "Your previous analysis"
    expect(aliceRound2).toContain("Your previous analysis");
    expect(aliceRound2).toContain("microservices scale better");
    // Alice also sees Bob's output under "Other council members"
    expect(aliceRound2).toContain("Other council members");
    expect(aliceRound2).toContain("monolith is simpler to start");
  });

  it("passes full deliberation transcript to synthesizer", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    await runCouncil(QUESTION, {
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    const synthPrompt = synth.calls[0]!.userContent;
    expect(synthPrompt).toContain("Alice");
    expect(synthPrompt).toContain("microservices scale better");
    expect(synthPrompt).toContain("Bob");
    expect(synthPrompt).toContain("monolith is simpler to start");
  });
});

describe("createCouncilTool", () => {
  it("returns a tool with the council name", () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const t = createCouncilTool({
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(t.name).toBe("council");
  });

  it("accepts a custom tool name", () => {
    const { advocate, synth } = makeCouncil();
    const t = createCouncilTool({
      name: "designCouncil",
      councilors: [{ name: "Alice", llm: advocate, role: "advocate" }],
      synthesizer: synth,
    });
    expect(t.name).toBe("designCouncil");
  });

  it("executes runCouncil and returns formatted output", async () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const t = createCouncilTool({
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    const output = await t.execute({ question: QUESTION });
    expect(output).toContain("Verdict:");
    expect(output).toContain("start monolith, plan for microservices later");
    expect(output).toContain("Deliberation:");
  });

  it("mentions councilor names in its description", () => {
    const { advocate, skeptic, synth } = makeCouncil();
    const t = createCouncilTool({
      councilors: [
        { name: "Alice", llm: advocate, role: "advocate" },
        { name: "Bob", llm: skeptic, role: "skeptic" },
      ],
      synthesizer: synth,
    });
    expect(t.description).toContain("Alice");
    expect(t.description).toContain("Bob");
  });
});

describe("formatCouncilResult", () => {
  it("includes verdict and deliberation sections", () => {
    const result = {
      verdict: "go with monolith",
      rounds: [
        {
          round: 1,
          contributions: [
            { councilor: "Alice", role: "advocate", text: "scale matters" },
            { councilor: "Bob", role: "skeptic", text: "simplicity wins" },
          ],
        },
      ],
    };
    const output = formatCouncilResult(result);
    expect(output).toContain("Verdict:");
    expect(output).toContain("go with monolith");
    expect(output).toContain("Round 1:");
    expect(output).toContain("Alice");
    expect(output).toContain("Bob");
  });
});
