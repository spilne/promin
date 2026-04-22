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

  printUsage(): void {
    const t = this.turnUsage;
    if (!t.input && !t.output) return;

    const parts: string[] = [`in ${fmtN(t.input)}`, `out ${fmtN(t.output)}`];
    if (t.cacheRead) parts.push(`cached ${fmtN(t.cacheRead)}`);
    if (t.cacheWrite) parts.push(`wrote ${fmtN(t.cacheWrite)}`);
    const used = this.sessionTokensUsed();
    const budgetStr = this.tokenBudget
      ? `${fmtN(used)} / ${fmtN(this.tokenBudget)} tokens`
      : `${fmtN(used)} tokens`;
    parts.push(`·  session ${budgetStr}`);
    process.stdout.write(`\x1b[2m  ${parts.join("  ")}\x1b[0m\n`);

    if (this.sessionByModel.size > 1) {
      const labelWidth = Math.max(...[...this.sessionByModel.keys()].map((k) => k.length));
      for (const [label, m] of this.sessionByModel) {
        if (!m.input && !m.output) continue;
        const mp: string[] = [`in ${fmtN(m.input)}`, `out ${fmtN(m.output)}`];
        if (m.cacheRead) mp.push(`cached ${fmtN(m.cacheRead)}`);
        process.stdout.write(`\x1b[2m    ${label.padEnd(labelWidth)}  ${mp.join("  ")}\x1b[0m\n`);
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
