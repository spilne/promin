import { describe, it, expect } from "bun:test";
import type { Interface } from "node:readline";
import { Terminal } from "../terminal.ts";

type QuestionCallback = (answer: string) => void;

interface FakeRl {
  rl: Interface;
  answerQuestion: (answer: string) => void;
}

function fakeRlWithQuestion(): FakeRl {
  let pending: QuestionCallback | null = null;
  const rl = {
    rawListeners: () => [],
    removeListener: () => {},
    on: () => {},
    question: (_prompt: string, cb: QuestionCallback) => {
      pending = cb;
    },
  } as unknown as Interface;
  return {
    rl,
    answerQuestion: (answer: string) => {
      pending?.(answer);
      pending = null;
    },
  };
}

function captureStdout(fn: () => void): string[] {
  const chunks: string[] = [];
  const orig = process.stdout.write.bind(process.stdout);
  (process.stdout as any).write = (chunk: string | Buffer) => {
    chunks.push(typeof chunk === "string" ? chunk : chunk.toString());
    return true;
  };
  try {
    fn();
  } finally {
    (process.stdout as any).write = orig;
  }
  return chunks;
}

describe("Terminal.ask()", () => {
  it("passes the question text to rl.question", async () => {
    let capturedPrompt = "";
    let capturedCb: QuestionCallback | null = null;
    const rl = {
      rawListeners: () => [],
      removeListener: () => {},
      on: () => {},
      question: (prompt: string, cb: QuestionCallback) => {
        capturedPrompt = prompt;
        capturedCb = cb;
      },
    } as unknown as Interface;
    const term = new Terminal(rl);

    const captured: string[] = [];
    (process.stdout as any).write = (c: string | Buffer) => {
      captured.push(typeof c === "string" ? c : c.toString());
      return true;
    };

    const p = term.ask("Enter API key");
    capturedCb?.("sk-test");
    await p;

    const orig = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = orig;

    expect(capturedPrompt).toContain("Enter API key");
  });

  it("sets suppress = true while waiting for input", () => {
    const { rl } = fakeRlWithQuestion();
    const term = new Terminal(rl);

    let suppressDuringQuestion = false;
    (rl as any).question = (_prompt: string, _cb: QuestionCallback) => {
      suppressDuringQuestion = term.suppress;
    };

    const p = term.ask("prompt");
    expect(suppressDuringQuestion).toBe(true);

    // answer not called — check state before resolution
    expect(term.suppress).toBe(true);

    // resolve manually so we don't leak the promise
    (rl as any).question = (_: string, cb: QuestionCallback) => cb("");
    void p;
  });

  it("restores suppress = false after the user answers", async () => {
    const { rl, answerQuestion } = fakeRlWithQuestion();
    const term = new Terminal(rl);

    const captured: string[] = [];
    (process.stdout as any).write = (c: string | Buffer) => {
      captured.push(typeof c === "string" ? c : c.toString());
      return true;
    };

    const p = term.ask("Key?");
    expect(term.suppress).toBe(true);
    answerQuestion("value");
    await p;

    expect(term.suppress).toBe(false);

    const orig = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = orig;
  });

  it("returns the trimmed answer", async () => {
    const { rl, answerQuestion } = fakeRlWithQuestion();
    const term = new Terminal(rl);

    const captured: string[] = [];
    (process.stdout as any).write = (c: string | Buffer) => {
      captured.push(typeof c === "string" ? c : c.toString());
      return true;
    };

    const p = term.ask("Name?");
    answerQuestion("  hello world  ");
    const result = await p;

    const orig = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = orig;

    expect(result).toBe("hello world");
  });

  it("sets inPrompt = true during the question and false after", async () => {
    const { rl, answerQuestion } = fakeRlWithQuestion();
    const term = new Terminal(rl);

    let inPromptDuringQuestion = false;
    const originalQuestion = (rl as any).question;
    (rl as any).question = (prompt: string, cb: QuestionCallback) => {
      inPromptDuringQuestion = term.inPrompt;
      originalQuestion.call(rl, prompt, cb);
    };

    const captured: string[] = [];
    (process.stdout as any).write = (c: string | Buffer) => {
      captured.push(typeof c === "string" ? c : c.toString());
      return true;
    };

    const p = term.ask("Q?");
    expect(inPromptDuringQuestion).toBe(true);
    answerQuestion("ans");
    await p;

    const orig = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = orig;

    expect(term.inPrompt).toBe(false);
  });

  it("spinner writes are suppressed during ask()", () => {
    const { rl } = fakeRlWithQuestion();
    const term = new Terminal(rl);

    // Start ask — this sets suppress = true
    term.ask("Waiting?");

    const writes = captureStdout(() => {
      term.startSpinner("this-should-not-appear");
      term.stopSpinner();
    });

    expect(writes.some((s) => s.includes("this-should-not-appear"))).toBe(false);

    // Clean up: restore suppress
    term.suppress = false;
    term.stopSpinner();
  });
});
