import type { Logger } from "../../src/index.ts";

export interface DemoLogger extends Logger {
  child(scope: string): DemoLogger;
}

export function createDemoLogger(scope?: string): DemoLogger {
  const prefix = scope ? `[${scope}]` : "";
  const write = (level: "log" | "warn" | "error", message: string, ...args: unknown[]) => {
    const time = new Date().toTimeString().slice(0, 8);
    const parts = [`[${time}]`];
    if (prefix) parts.push(prefix);
    console[level](parts.join(" "), message, ...args);
  };
  return {
    log: (message, ...args) => write("log", message, ...args),
    warn: (message, ...args) => write("warn", message, ...args),
    error: (message, ...args) => write("error", message, ...args),
    child: (name) => createDemoLogger(scope ? `${scope}:${name}` : name),
  };
}
