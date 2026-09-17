import type { LLMProvider, LLMUsage } from "../lib/llm-provider.ts";

type Totals = { input: number; output: number; cacheRead: number; cacheWrite: number };
const zeroTotals = (): Totals => ({ input: 0, output: 0, cacheRead: 0, cacheWrite: 0 });

export function fmtN(n: number): string {
  return n >= 10_000
    ? `${Math.round(n / 1000)}k`
    : n >= 1000
      ? `${(n / 1000).toFixed(1)}k`
      : String(n);
}

function fmtCost(usd: number): string {
  if (usd === 0) return "$0.00";
  if (usd < 0.001) return `$${usd.toFixed(5)}`;
  if (usd < 0.01) return `$${usd.toFixed(4)}`;
  return `$${usd.toFixed(3)}`;
}

// USD per million tokens. Matched by substring of the model label.
const PRICING: Array<{
  match: string;
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
}> = [
  { match: "claude-opus-4", input: 15.0, output: 75.0, cacheRead: 1.5, cacheWrite: 18.75 },
  { match: "claude-sonnet-4", input: 3.0, output: 15.0, cacheRead: 0.3, cacheWrite: 3.75 },
  { match: "claude-haiku-4", input: 0.8, output: 4.0, cacheRead: 0.08, cacheWrite: 1.0 },
  { match: "gpt-4o-mini", input: 0.15, output: 0.6, cacheRead: 0.075, cacheWrite: 0 },
  { match: "gpt-4o", input: 2.5, output: 10.0, cacheRead: 1.25, cacheWrite: 0 },
  { match: "gemini-2.0-flash", input: 0.1, output: 0.4, cacheRead: 0.025, cacheWrite: 0 },
  { match: "gemini-1.5-pro", input: 1.25, output: 5.0, cacheRead: 0.3125, cacheWrite: 0 },
];

function modelCost(label: string, t: Totals): number {
  const p = PRICING.find((r) => label.includes(r.match));
  if (!p) return 0;
  return (
    (t.input * p.input +
      t.output * p.output +
      t.cacheRead * p.cacheRead +
      t.cacheWrite * p.cacheWrite) /
    1_000_000
  );
}

export class UsageTracker {
  readonly tokenBudget: number | null;
  private sessionUsage = zeroTotals();
  private sessionByModel = new Map<string, Totals>();
  private turnUsage = zeroTotals();
  private turnByModel = new Map<string, Totals>();

  constructor() {
    this.tokenBudget = process.env.SESSION_TOKEN_BUDGET
      ? Number(process.env.SESSION_TOKEN_BUDGET)
      : null;
  }

  sessionTokensUsed(): number {
    return this.sessionUsage.input + this.sessionUsage.output;
  }

  withTracking(llm: LLMProvider, label: string): LLMProvider {
    const add = (u: LLMUsage) => {
      const addTo = (t: Totals) => {
        t.input += u.inputTokens;
        t.output += u.outputTokens;
        t.cacheRead += u.cacheReadTokens ?? 0;
        t.cacheWrite += u.cacheWriteTokens ?? 0;
      };
      addTo(this.turnUsage);
      addTo(this.sessionUsage);
      const lt = this.turnByModel.get(label) ?? zeroTotals();
      addTo(lt);
      this.turnByModel.set(label, lt);
      const sm = this.sessionByModel.get(label) ?? zeroTotals();
      addTo(sm);
      this.sessionByModel.set(label, sm);
    };
    const wrapped: LLMProvider = {
      chat: async (params) => {
        const r = await llm.chat(params);
        if (r.usage) add(r.usage);
        return r;
      },
    };
    if (llm.chatStream) {
      const orig = llm.chatStream.bind(llm);
      wrapped.chatStream = async function* (params) {
        for await (const chunk of orig(params)) {
          if (chunk.usage) add(chunk.usage);
          yield chunk;
        }
      };
    }
    return wrapped;
  }

  /** Total cost in USD for the current session across all models. */
  sessionCost(): number {
    let total = 0;
    for (const [label, t] of this.sessionByModel) total += modelCost(label, t);
    return total;
  }

  /** Total cost in USD for the current turn. */
  turnCost(): number {
    let total = 0;
    for (const [label, t] of this.turnByModel) total += modelCost(label, t);
    return total;
  }

  printUsage(): void {
    const t = this.turnUsage;
    if (!t.input && !t.output) return;

    // Token counts — Aider style
    const tokParts: string[] = [`${fmtN(t.input)} sent`];
    if (t.cacheWrite) tokParts.push(`${fmtN(t.cacheWrite)} cache write`);
    if (t.cacheRead) tokParts.push(`${fmtN(t.cacheRead)} cache hit`);
    tokParts.push(`${fmtN(t.output)} received`);
    const tokenLine = `Tokens: ${tokParts.join(", ")}.`;

    // Cost line
    const msgCost = this.turnCost();
    const sesCost = this.sessionCost();
    const hasCost = msgCost > 0 || sesCost > 0;
    const costLine = hasCost
      ? `  Cost: ${fmtCost(msgCost)} message, ${fmtCost(sesCost)} session.`
      : "";

    // Token budget indicator
    const used = this.sessionTokensUsed();
    const budgetLine = this.tokenBudget
      ? `  Session: ${fmtN(used)} / ${fmtN(this.tokenBudget)} tokens.`
      : "";

    process.stdout.write(`\x1b[2m  ${tokenLine}${costLine}${budgetLine}\x1b[0m\n`);

    if (this.sessionByModel.size > 1) {
      const labelWidth = Math.max(...[...this.sessionByModel.keys()].map((k) => k.length));
      for (const [label, m] of this.sessionByModel) {
        if (!m.input && !m.output) continue;
        const mp = [`${fmtN(m.input)} sent`, `${fmtN(m.output)} received`];
        if (m.cacheRead) mp.push(`${fmtN(m.cacheRead)} cache hit`);
        process.stdout.write(`\x1b[2m    ${label.padEnd(labelWidth)}  ${mp.join(", ")}\x1b[0m\n`);
      }
    }
  }

  resetTurn(): void {
    this.turnUsage = zeroTotals();
    this.turnByModel = new Map();
  }

  resetSession(): void {
    Object.assign(this.sessionUsage, zeroTotals());
    this.sessionByModel.clear();
    this.resetTurn();
  }
}
