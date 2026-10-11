// ---------------------------------------------------------------------------
// Stdio transport for operator-defined MCP servers.
//
// Like the SDK's `StdioClientTransport` (newline-delimited JSON-RPC over the
// child's stdin / stdout, the SDK's safe default environment plus the
// server's `env`), with three differences:
//
//   - the server runs in its own process group, and closing the transport
//     (or the server exiting) signals the whole group, so helpers it spawned
//     don't outlive it;
//   - stderr goes to a callback line by line (length-capped) instead of
//     being inherited by the host's stderr;
//   - a line longer than `MAX_STDIO_MESSAGE_BYTES` closes the connection
//     instead of growing the buffer without bound.
// ---------------------------------------------------------------------------

import { spawn, type ChildProcess } from "node:child_process";
import { getDefaultEnvironment } from "@modelcontextprotocol/sdk/client/stdio.js";
import { deserializeMessage, serializeMessage } from "@modelcontextprotocol/sdk/shared/stdio.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { SystemWallClock, type WallClock } from "@promin/workflow";

/** Longest JSON-RPC message (one line) accepted from a stdio server. */
export const MAX_STDIO_MESSAGE_BYTES = 8 * 1024 * 1024;
const MAX_STDERR_LINE_CHARS = 1_000;
const KILL_GRACE_MS = 2_000;

export interface ProcessGroupStdioTransportConfig {
  readonly command: string;
  readonly args?: ReadonlyArray<string>;
  /** Added to the SDK's safe default environment. */
  readonly env?: Readonly<Record<string, string>>;
  readonly cwd?: string;
  /** Receives each stderr line (control characters removed, capped). */
  readonly onStderr?: (line: string) => void;
  /** Time source for the SIGTERM → SIGKILL grace. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class ProcessGroupStdioTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;

  private child: ChildProcess | undefined;
  private exited = false;
  private pending: Buffer[] = [];
  private pendingBytes = 0;
  private stderrTail = "";
  private readonly clock: WallClock;

  constructor(private readonly config: ProcessGroupStdioTransportConfig) {
    this.clock = config.clock ?? SystemWallClock;
  }

  /** The server's process id (also its process group id). */
  get pid(): number | undefined {
    return this.child?.pid;
  }

  start(): Promise<void> {
    if (this.child) throw new Error("ProcessGroupStdioTransport already started");
    return new Promise((resolve, reject) => {
      const child = spawn(this.config.command, [...(this.config.args ?? [])], {
        env: { ...getDefaultEnvironment(), ...this.config.env },
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
        detached: true,
        ...(this.config.cwd !== undefined && { cwd: this.config.cwd }),
      });
      this.child = child;
      child.once("error", (err) => {
        reject(err);
        this.onerror?.(err);
      });
      child.once("spawn", () => resolve());
      child.once("exit", () => {
        this.exited = true;
        // Whatever the server left running in its group goes with it.
        this.signalGroup("SIGKILL");
      });
      child.once("close", () => this.onclose?.());
      child.stdin?.on("error", (err) => this.onerror?.(err));
      child.stdout?.on("data", (chunk: Buffer) => this.onStdout(chunk));
      child.stdout?.on("error", (err) => this.onerror?.(err));
      child.stderr?.on("data", (chunk: Buffer) => this.onStderrChunk(chunk));
    });
  }

  send(message: JSONRPCMessage): Promise<void> {
    const stdin = this.child?.stdin;
    if (!stdin || this.exited) return Promise.reject(new Error("Not connected"));
    return new Promise((resolve, reject) => {
      if (stdin.write(serializeMessage(message))) {
        resolve();
        return;
      }
      // A server that stops reading must not park the sender forever: the
      // wait ends when the pipe drains, breaks or closes (on close/exit).
      const done = (err?: Error) => {
        stdin.off("drain", onDrain);
        stdin.off("close", onClose);
        stdin.off("error", onError);
        if (err) reject(err);
        else resolve();
      };
      const onDrain = () => done();
      const onClose = () => done(new Error("MCP stdio server closed its input"));
      const onError = (err: Error) => done(err);
      stdin.once("drain", onDrain);
      stdin.once("close", onClose);
      stdin.once("error", onError);
    });
  }

  async close(): Promise<void> {
    const child = this.child;
    if (!child || this.exited) return;
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      child.stdin?.end();
    } catch {
      // already closed
    }
    this.signalGroup("SIGTERM");
    const grace = new Promise<void>((resolve) => {
      const timer = this.clock.setTimeout(() => resolve(), KILL_GRACE_MS);
      void exited.then(() => timer.clear());
    });
    await Promise.race([exited, grace]);
    if (!this.exited) this.signalGroup("SIGKILL");
  }

  private signalGroup(signal: NodeJS.Signals): void {
    const pid = this.child?.pid;
    if (pid === undefined) return;
    try {
      process.kill(-pid, signal);
    } catch {
      // group already gone
    }
  }

  private onStdout(chunk: Buffer): void {
    let start = 0;
    for (let i = chunk.indexOf(0x0a); i >= 0; i = chunk.indexOf(0x0a, start)) {
      this.pending.push(chunk.subarray(start, i));
      const line = Buffer.concat(this.pending).toString("utf8").replace(/\r$/, "");
      this.pending = [];
      this.pendingBytes = 0;
      start = i + 1;
      if (line.length === 0) continue;
      try {
        this.onmessage?.(deserializeMessage(line));
      } catch (err) {
        this.onerror?.(err instanceof Error ? err : new Error(String(err)));
      }
    }
    if (start < chunk.length) {
      this.pending.push(chunk.subarray(start));
      this.pendingBytes += chunk.length - start;
      if (this.pendingBytes > MAX_STDIO_MESSAGE_BYTES) {
        this.pending = [];
        this.pendingBytes = 0;
        this.onerror?.(new Error(`MCP stdio message exceeds ${MAX_STDIO_MESSAGE_BYTES} bytes`));
        void this.close();
      }
    }
  }

  private onStderrChunk(chunk: Buffer): void {
    const lines = (this.stderrTail + chunk.toString("utf8")).split("\n");
    this.stderrTail = (lines.pop() ?? "").slice(0, MAX_STDERR_LINE_CHARS);
    for (const line of lines) {
      // eslint-disable-next-line no-control-regex
      const clean = line.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, MAX_STDERR_LINE_CHARS);
      if (clean.trim().length > 0) this.config.onStderr?.(clean);
    }
  }
}
