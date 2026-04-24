import { createInterface, type Interface } from "node:readline";
import { readdir } from "node:fs/promises";
import type { ToolRegistry } from "../tool-registry.ts";
import { Terminal } from "./terminal.ts";

const DIM = "\x1b[2m";
const RST = "\x1b[0m";

export interface CommandDef {
  cmd: string;
  args?: string;
  desc: string | (() => string);
}

export interface ChatTerminalConfig {
  /** Slash commands shown in the dropdown and /help. */
  commands?: CommandDef[];
  /** Workspace root used for @-file fuzzy completion. Defaults to cwd. */
  workspace?: string;
  historySize?: number;
}

type CompleterCb = (err: Error | null, result: [string[], string]) => void;

/**
 * Readline + Terminal bundle with built-in REPL completions:
 *   - `/cmd` dropdown from `commands` (with descriptions)
 *   - `:tool [json]` dropdown from a ToolRegistry (set via `setRegistry`)
 *   - `@file` fuzzy completion inside normal messages
 *
 * Both dropdowns auto-open when `:` or `/` is the first character typed.
 */
export class ChatTerminal {
  readonly term: Terminal;
  readonly rl: Interface;

  private readonly _commands: CommandDef[];
  private readonly _workspace: string;
  private _registry: ToolRegistry | null = null;
  private _completionBusy = false;

  constructor(config: ChatTerminalConfig = {}) {
    this._commands = config.commands ?? [];
    this._workspace = config.workspace ?? process.cwd();

    this.rl = createInterface({
      input: process.stdin,
      output: process.stdout,
      historySize: config.historySize ?? 100,
      completer: (line: string, cb: CompleterCb) => this._completer(line, cb),
    });
    this.term = new Terminal(this.rl);
    this._setupKeypressHook();
  }

  /** Call once the ToolRegistry is ready (it's typically created async after Terminal). */
  setRegistry(registry: ToolRegistry): void {
    this._registry = registry;
  }

  private _completer(line: string, cb: CompleterCb): void {
    (async (): Promise<[string[], string]> => {
      // :tool direct-call (line starts with :, no space yet)
      if (/^:\S*$/.test(line) && this._registry) {
        const prefix = line.slice(1);
        const hits = Object.entries(this._registry.getTools()).filter(([n]) =>
          n.startsWith(prefix),
        );
        if (hits.length === 0) return [[], line];
        const labels = hits.map(
          ([n, def]) => `:${n}  ${DIM}${def.description.split("\n")[0]}${RST}`,
        );
        const values = hits.map(([n]) => `:${n} `);
        const selected = await this.term.showInlineMenu(labels, values);
        if (!selected) return [[], line];
        return [[selected], line];
      }
      // @-file fuzzy match (@ inside a normal message)
      const atMatch = line.match(/@(\S*)$/);
      if (atMatch) {
        const files = await this._listWorkspaceFiles(atMatch[1]);
        if (files.length === 0) return [[], line];
        const selected = await this.term.showInlineMenu(files.map((f) => `@${f}`));
        if (!selected) return [[], line];
        return [[line.slice(0, line.length - atMatch[0].length) + selected], line];
      }
      // slash-command dropdown with descriptions
      if (line.startsWith("/")) {
        const hits = this._commands.filter((c) => c.cmd.startsWith(line));
        if (hits.length === 0) return [[], line];
        const labels = hits.map((c) => {
          const label = c.args ? `${c.cmd} ${c.args}` : c.cmd;
          const d = typeof c.desc === "function" ? c.desc() : c.desc;
          return `${label}  ${DIM}${d}${RST}`;
        });
        const values = hits.map((c) => c.cmd);
        const selected = await this.term.showInlineMenu(labels, values);
        if (!selected) return [[], line];
        return [[selected], line];
      }
      return [[], line];
    })().then(
      (r) => cb(null, r),
      (e) => cb(e instanceof Error ? e : new Error(String(e)), [[], line]),
    );
  }

  private _setupKeypressHook(): void {
    // Auto-open dropdown when : or / is the very first character typed.
    // setImmediate defers until readline has appended the char to rl.line.
    process.stdin.on("keypress", () => {
      if (this._completionBusy || !this.term.inPrompt) return;
      setImmediate(() => {
        const current = (this.rl as unknown as { line: string }).line ?? "";
        const trigger = current === "/" || (current === ":" && this._registry !== null);
        if (!trigger) return;
        this._completionBusy = true;
        this._completer(current, (err, result) => {
          this._completionBusy = false;
          if (err || !result) return;
          const [completions] = result;
          if (!completions.length) return;
          const selected = completions[0];
          if (!selected || selected === current) return;
          this.rl.write("", { ctrl: true, name: "u" }); // Ctrl+U: clear line
          this.rl.write(selected);
        });
      });
    });
  }

  private async _listWorkspaceFiles(prefix: string, limit = 20): Promise<string[]> {
    try {
      const all = (await readdir(this._workspace, { recursive: true })) as string[];
      const skip = (f: string) =>
        f.split("/").some((p) => p.startsWith(".")) ||
        f.includes("node_modules") ||
        f.includes("/dist/");
      return all.filter((f) => !skip(f) && (!prefix || f.startsWith(prefix))).slice(0, limit);
    } catch {
      return [];
    }
  }
}
