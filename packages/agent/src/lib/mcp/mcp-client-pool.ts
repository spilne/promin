// ---------------------------------------------------------------------------
// `McpClientPool` — lifecycle manager for MCP clients. stdio clients
// spawn processes; opening one per request is expensive. The pool
// caches clients by config-key and reuses across requests, closing
// them only on host shutdown.
//
// Two implementations live here:
//
//   DefaultMcpClientPool — production. Lazy-opens via createSdkMcpClient
//                          on first get(). Never evicts; relies on the
//                          host calling closeAll() at shutdown.
//
//   InMemoryMcpClientPool — test seam. Caller pre-registers the client
//                          factory; pool returns whatever the factory
//                          builds. Skips the SDK, so no real MCP server
//                          needed.
// ---------------------------------------------------------------------------

import { createSdkMcpClient } from "./mcp-client.ts";
import type { McpClient, McpServerConfig } from "./types.ts";

export interface McpClientPool {
  /**
   * Get-or-open the client for `config`. The pool keys cached clients
   * by `config.name` (which is meant to be unique per host); reusing
   * the same name with a different transport/url returns the cached
   * one and ignores the new args.
   */
  get(config: McpServerConfig): Promise<McpClient>;

  /** Close every open client. Idempotent. */
  closeAll(): Promise<void>;
}

export interface DefaultMcpClientPoolConfig {
  /**
   * Resolver that turns a `credentialRef` into a concrete bearer
   * token at connect time. Forward-compat with promin-an9l (BYOK).
   * Optional — when omitted, only configs with an inline `authToken`
   * (or stdio with env-baked creds) work.
   */
  readonly resolveCredential?: (ref: string) => Promise<string>;
}

export class DefaultMcpClientPool implements McpClientPool {
  private readonly config: DefaultMcpClientPoolConfig;
  private readonly clients = new Map<string, Promise<McpClient>>();

  constructor(config: DefaultMcpClientPoolConfig = {}) {
    this.config = config;
  }

  async get(config: McpServerConfig): Promise<McpClient> {
    const cached = this.clients.get(config.name);
    if (cached) return cached;
    const promise = (async () => {
      try {
        return await createSdkMcpClient({
          server: config,
          ...(this.config.resolveCredential !== undefined && {
            resolveCredential: this.config.resolveCredential,
          }),
        });
      } catch (err) {
        // Failed open: drop from cache so the next get() retries.
        this.clients.delete(config.name);
        throw err;
      }
    })();
    this.clients.set(config.name, promise);
    return promise;
  }

  async closeAll(): Promise<void> {
    const promises = Array.from(this.clients.values()).map(async (p) => {
      try {
        const c = await p;
        await c.close();
      } catch {
        // best-effort
      }
    });
    this.clients.clear();
    await Promise.all(promises);
  }
}

/**
 * Test pool — pre-registered factories let tests return canned clients
 * without spinning up real MCP servers.
 */
export class InMemoryMcpClientPool implements McpClientPool {
  private readonly factories = new Map<string, () => McpClient>();
  private readonly clients = new Map<string, McpClient>();

  /** Register a factory keyed by config.name. */
  register(name: string, factory: () => McpClient): void {
    this.factories.set(name, factory);
  }

  async get(config: McpServerConfig): Promise<McpClient> {
    const cached = this.clients.get(config.name);
    if (cached) return cached;
    const factory = this.factories.get(config.name);
    if (!factory) {
      throw new Error(
        `InMemoryMcpClientPool: no factory registered for server '${config.name}'. ` +
          "Call pool.register(name, factory) before .get().",
      );
    }
    const client = factory();
    this.clients.set(config.name, client);
    return client;
  }

  async closeAll(): Promise<void> {
    const all = Array.from(this.clients.values());
    this.clients.clear();
    await Promise.all(all.map((c) => c.close()));
  }
}
