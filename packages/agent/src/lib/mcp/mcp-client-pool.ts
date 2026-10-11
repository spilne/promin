// ---------------------------------------------------------------------------
// `McpClientPool` — shared MCP connections, isolated per tenant.
//
// A client is keyed by a hash of the full server config, the tenant
// namespace and the resolved credential. Two recipes (or two tenants) that
// both call their server "github" therefore never share a connection, a
// spawned process, a token or a schema validator, while repeated calls
// under the same (config, namespace, credential) reuse one connection.
// When a server's credential for a namespace changes, the client holding
// the old one is closed once it is idle.
//
// Each client runs at most `maxConcurrentCalls` calls at once; the rest
// queue (and leave the queue when their signal aborts). A client whose
// transport closed or whose request failed below the protocol is dropped
// and the next call connects again. Beyond `maxClients`, the least
// recently used idle clients are closed. After `closeAll()` the pool
// refuses new work.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import type { WallClock } from "@promin/workflow";
import type { OutboundPolicy } from "../outbound/outbound-policy.ts";
import { connectMcpClient, createMcpTransport } from "./mcp-client.ts";
import { McpInputValidators } from "./mcp-schema-validator.ts";
import {
  DEFAULT_MCP_MAX_CONCURRENT_CALLS,
  DEFAULT_MCP_TIMEOUT_MS,
  type McpCallOptions,
  type McpClient,
  type McpServerConfig,
  type McpToolDefinition,
  type McpToolResult,
} from "./types.ts";

/** Opens one client for a server. The pool's default spawns / dials it. */
export type McpConnect = (params: {
  readonly server: McpServerConfig;
  readonly credential?: string;
}) => Promise<McpClient>;

export interface McpClientPoolConfig {
  /**
   * Which hosts http servers may live on. Default: public http(s) hosts
   * only; a server's `allowPrivateNetwork` widens it for that server.
   */
  readonly outboundPolicy?: OutboundPolicy;
  /** Open clients kept at most; idle ones beyond it are closed. Default 64. */
  readonly maxClients?: number;
  /** Cap on one http response body. Default `DEFAULT_MCP_MAX_RESPONSE_BYTES`. */
  readonly maxResponseBytes?: number;
  /**
   * Receives stdio servers' stderr lines, prefixed with the server name.
   * Default: `console.warn`.
   */
  readonly onServerLog?: (line: string) => void;
  /** Time source for listing deadlines and process shutdown. Default `SystemWallClock`. */
  readonly clock?: WallClock;
  /** Replace how clients are opened. Tests connect in-memory servers here. */
  readonly connect?: McpConnect;
}

/** The pool's view of one server for one tenant and credential. */
export interface McpServerHandle {
  readonly server: McpServerConfig;
  /** Pool key: hash of server config, namespace and credential. */
  readonly key: string;
  listTools(options?: McpCallOptions): Promise<McpToolDefinition[]>;
  callTool(params: {
    readonly name: string;
    readonly arguments: Readonly<Record<string, unknown>>;
    readonly options?: McpCallOptions;
  }): Promise<McpToolResult>;
  /** Compiled input validators private to this server, tenant and credential. */
  readonly validators: McpInputValidators;
}

/** The pool was closed; it takes no new work. */
export class McpPoolClosedError extends Error {
  constructor() {
    super("MCP client pool is closed");
    this.name = "McpPoolClosedError";
  }
}

/** A handle's credential was rotated out; load the tools again. */
export class McpCredentialRotatedError extends Error {
  constructor() {
    super("The MCP server's credential changed since these tools were loaded; load them again");
    this.name = "McpCredentialRotatedError";
  }
}

const DEFAULT_MAX_CLIENTS = 64;
/** Groups remembered for credential rotation, beyond which the oldest are forgotten. */
const MAX_GROUPS = 10_000;

interface PoolEntry {
  readonly key: string;
  /** Hash of server config, namespace and credential scope: entries differing only in credential. */
  readonly group: string;
  readonly server: McpServerConfig;
  readonly credential: string | undefined;
  readonly validators: McpInputValidators;
  current: McpClient | undefined;
  connecting: Promise<McpClient> | undefined;
  active: number;
  readonly waiters: Array<() => void>;
  /** Replaced (new credential) or evicted: closed once idle, never reused. */
  retired: boolean;
}

export class McpClientPool {
  private readonly entries = new Map<string, PoolEntry>();
  /** Group → key of the credential most recently resolved for it. */
  private readonly groups = new Map<string, string>();
  private readonly connect: McpConnect;
  private readonly maxClients: number;
  private closed = false;

  constructor(config: McpClientPoolConfig = {}) {
    const policy = config.outboundPolicy;
    const log = config.onServerLog ?? ((line: string) => console.warn(line));
    this.maxClients = config.maxClients ?? DEFAULT_MAX_CLIENTS;
    this.connect =
      config.connect ??
      ((params) =>
        connectMcpClient({
          transport: createMcpTransport({
            server: params.server,
            ...(params.credential !== undefined && { credential: params.credential }),
            ...(policy !== undefined && { policy }),
            ...(config.maxResponseBytes !== undefined && {
              maxResponseBytes: config.maxResponseBytes,
            }),
            ...(config.clock !== undefined && { clock: config.clock }),
            onStderr: (line) => log(`[mcp:${params.server.name}] ${line}`),
          }),
          timeoutMs: params.server.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS,
          ...(config.clock !== undefined && { clock: config.clock }),
          closeOnTimeout: params.server.transport === "http",
        }));
  }

  /**
   * The handle for `server` under tenant `namespaceId` with `credential`
   * (the resolved `credentialRef`, if any), freshly resolved at
   * `credentialScope` (an opaque id of the scope it was found at, e.g.
   * `resource:u-1`). Cheap: nothing connects until the first call.
   *
   * A credential for the same server, namespace and credential scope that
   * differs from the last one handed in is a rotation: clients holding the
   * old one are closed once idle, and older handles fail with
   * `McpCredentialRotatedError` instead of reconnecting with it.
   */
  server(params: {
    readonly server: McpServerConfig;
    readonly namespaceId?: string;
    readonly credential?: string;
    readonly credentialScope?: string;
  }): McpServerHandle {
    if (this.closed) throw new McpPoolClosedError();
    const key = mcpPoolKey(params);
    const group = mcpGroupKey(params);
    this.groups.delete(group);
    this.groups.set(group, key);
    if (this.groups.size > MAX_GROUPS) {
      const oldest = this.groups.keys().next().value;
      if (oldest !== undefined) this.groups.delete(oldest);
    }
    this.retireGroup({ group, keep: key });
    const entryFor = () =>
      this.entry({
        key,
        group,
        server: params.server,
        ...(params.credential !== undefined && { credential: params.credential }),
      });
    const first = entryFor();
    return {
      server: params.server,
      key,
      validators: first.validators,
      listTools: async (options) =>
        this.run({ entry: entryFor(), signal: options?.signal, op: (c) => c.listTools(options) }),
      callTool: async (call) =>
        this.run({ entry: entryFor(), signal: call.options?.signal, op: (c) => c.callTool(call) }),
    };
  }

  /** Clients currently held (connected or connecting). */
  get size(): number {
    let n = 0;
    for (const e of this.entries.values()) if (e.current || e.connecting) n += 1;
    return n;
  }

  /** Close every client and refuse new work. Idempotent. */
  async closeAll(): Promise<void> {
    this.closed = true;
    const entries = [...this.entries.values()];
    this.entries.clear();
    await Promise.all(entries.map((e) => closeEntry(e)));
  }

  private entry(params: {
    readonly key: string;
    readonly group: string;
    readonly server: McpServerConfig;
    readonly credential?: string;
  }): PoolEntry {
    if (this.closed) throw new McpPoolClosedError();
    const current = this.groups.get(params.group);
    if (current !== undefined && current !== params.key) throw new McpCredentialRotatedError();
    let entry = this.entries.get(params.key);
    if (entry) {
      // Re-insert: Map order doubles as least-recently-used order.
      this.entries.delete(params.key);
    } else {
      entry = {
        key: params.key,
        group: params.group,
        server: params.server,
        credential: params.credential,
        validators: new McpInputValidators(),
        current: undefined,
        connecting: undefined,
        active: 0,
        waiters: [],
        retired: false,
      };
    }
    this.entries.set(params.key, entry);
    this.evictIdle(params.key);
    return entry;
  }

  /** A new credential for the same server, namespace and credential scope: drop the old clients. */
  private retireGroup(params: { group: string; keep: string }): void {
    for (const entry of this.entries.values()) {
      if (entry.group !== params.group || entry.key === params.keep) continue;
      this.retire(entry);
    }
  }

  private retire(entry: PoolEntry): void {
    this.entries.delete(entry.key);
    entry.retired = true;
    if (entry.active === 0 && entry.waiters.length === 0) void closeEntry(entry);
  }

  /** Close least recently used idle entries past the cap, never `keep`. */
  private evictIdle(keep: string | undefined): void {
    for (const entry of this.entries.values()) {
      if (this.entries.size <= this.maxClients) return;
      if (entry.key === keep || entry.active > 0 || entry.waiters.length > 0) continue;
      this.retire(entry);
    }
  }

  private async run<T>(params: {
    readonly entry: PoolEntry;
    readonly signal: AbortSignal | undefined;
    readonly op: (client: McpClient) => Promise<T>;
  }): Promise<T> {
    const { entry } = params;
    await acquire({ entry, signal: params.signal });
    try {
      const client = await this.clientFor(entry);
      try {
        return await params.op(client);
      } finally {
        if (client.closed && entry.current === client) {
          entry.current = undefined;
          void client.close();
        }
      }
    } finally {
      release(entry);
      if (entry.retired && entry.active === 0 && entry.waiters.length === 0) {
        void closeEntry(entry);
      }
      // Entries kept over the cap while busy are closed once idle.
      this.evictIdle(undefined);
    }
  }

  private clientFor(entry: PoolEntry): Promise<McpClient> {
    if (this.closed) return Promise.reject(new McpPoolClosedError());
    if (entry.current?.closed) {
      void entry.current.close();
      entry.current = undefined;
    }
    if (entry.current) return Promise.resolve(entry.current);
    entry.connecting ??= this.connect({
      server: entry.server,
      ...(entry.credential !== undefined && { credential: entry.credential }),
    }).then(
      (client) => {
        entry.connecting = undefined;
        if (this.closed || entry.retired) {
          // Closed or replaced while connecting: serve this call, keep nothing.
          queueMicrotask(() => {
            if (entry.active === 0) void client.close();
          });
        }
        entry.current = client;
        return client;
      },
      (err: unknown) => {
        entry.connecting = undefined;
        throw err;
      },
    );
    return entry.connecting;
  }
}

/** Wait for a call slot on `entry`, or reject when `signal` aborts first. */
function acquire(params: { entry: PoolEntry; signal: AbortSignal | undefined }): Promise<void> {
  const { entry, signal } = params;
  if (signal?.aborted) return Promise.reject(signal.reason);
  const limit = Math.max(1, entry.server.maxConcurrentCalls ?? DEFAULT_MCP_MAX_CONCURRENT_CALLS);
  if (entry.active < limit) {
    entry.active += 1;
    return Promise.resolve();
  }
  return new Promise<void>((resolve, reject) => {
    const grant = () => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    };
    const onAbort = () => {
      const i = entry.waiters.indexOf(grant);
      if (i >= 0) entry.waiters.splice(i, 1);
      reject(signal?.reason);
    };
    entry.waiters.push(grant);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Hand the slot to the next waiter, or free it. */
function release(entry: PoolEntry): void {
  const next = entry.waiters.shift();
  if (next) next();
  else entry.active -= 1;
}

async function closeEntry(entry: PoolEntry): Promise<void> {
  const client = entry.current ?? (await entry.connecting?.catch(() => undefined));
  entry.current = undefined;
  await client?.close();
}

/** Pool key for a server config under a tenant namespace and credential. */
export function mcpPoolKey(params: {
  readonly server: McpServerConfig;
  readonly namespaceId?: string;
  readonly credential?: string;
}): string {
  return createHash("sha256")
    .update(
      canonicalJson({
        server: params.server,
        namespaceId: params.namespaceId ?? null,
        credential: params.credential ?? null,
      }),
    )
    .digest("hex");
}

/** Key of the entries that differ only in credential (see `server`). */
function mcpGroupKey(params: {
  readonly server: McpServerConfig;
  readonly namespaceId?: string;
  readonly credentialScope?: string;
}): string {
  return createHash("sha256")
    .update(
      canonicalJson({
        server: params.server,
        namespaceId: params.namespaceId ?? null,
        credentialScope: params.credentialScope ?? null,
        group: true,
      }),
    )
    .digest("hex");
}

/** JSON with object keys sorted, so equal configs hash equally. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
