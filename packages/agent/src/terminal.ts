// ---------------------------------------------------------------------------
// `@promin/agent/terminal` — the Node TTY REPL kit: raw-mode terminal,
// markdown rendering, a chat terminal and the console turn runner. Kept off
// the package root so server and library consumers don't load TTY code.
// ---------------------------------------------------------------------------

export { Terminal, isDumbTerminal, PROMPT, PLAIN_PROMPT } from "./lib/terminal/terminal.ts";
export type { TreeNode, AgentUIRenderer } from "./lib/terminal/terminal.ts";

export { TerminalIO } from "./lib/terminal/terminal-io.ts";

export { MarkdownRenderer, osc8 } from "./lib/terminal/terminal-markdown.ts";
export type { MarkdownRendererConfig } from "./lib/terminal/terminal-markdown.ts";

export { ChatTerminal } from "./lib/terminal/chat-terminal.ts";
export type { ChatTerminalConfig, CommandDef } from "./lib/terminal/chat-terminal.ts";

export { ConsoleRunner, abortable } from "./lib/terminal/console-runner.ts";
export type { RunTurnOptions, RunTurnResult, TurnTracker } from "./lib/terminal/console-runner.ts";
