// ---------------------------------------------------------------------------
// FederationManifest — host-side allowlist that gates `RemoteAgentBackend`
// recipe registration. A recipe pointing at an endpoint not in the
// manifest fails at registration time, NOT at first invocation.
//
// Why gate at registration: catches misconfig early (bad endpoint typo
// in a recipe file, malicious self-registration claiming a peer URL),
// surfaces clear errors to the dashboard / CI instead of a runtime
// 500. Local recipes always pass — gating only applies to
// `backend.type === 'remote'`.
//
// Implementations:
//   - StaticFederationManifest — host config: { endpoint: agentIds[] }[]
//   - AllowAllFederationManifest — opt-out for trusted environments
//
// Hosts that don't care wrap their registry with `AllowAllFederationManifest`
// or simply skip the wrapper. The decorator is opt-in.
// ---------------------------------------------------------------------------

export interface FederationManifest {
  /**
   * Returns true when (endpoint, agentId) is allowed by the manifest.
   * Async to allow KMS / config-service-backed implementations.
   */
  isAllowed(params: { readonly endpoint: string; readonly agentId: string }): Promise<boolean>;
}

/** Reason a federation registration was rejected. Surfaces in the thrown error. */
export class FederationManifestError extends Error {
  readonly _tag = "FederationManifest";
  constructor(
    public readonly endpoint: string,
    public readonly agentId: string,
  ) {
    super(
      `Federation manifest rejects remote agent "${agentId}" at endpoint "${endpoint}". ` +
        "Add the (endpoint, agentId) pair to the host's federation manifest before registering.",
    );
    this.name = "FederationManifestError";
  }
}

/**
 * Static manifest from a JSON-serializable config. Most common shape:
 *
 *   const manifest = StaticFederationManifest.fromConfig([
 *     { endpoint: 'https://org-b.example', agentIds: ['writer', 'analyst'] },
 *     { endpoint: 'https://org-c.example', agentIds: ['*'] },  // wildcard
 *   ]);
 *
 * Endpoint matching is exact-string (after the recipe normalizes trailing
 * slashes — caller's responsibility); agentId matching supports `'*'`
 * as 'any agent at this endpoint'.
 */
export class StaticFederationManifest implements FederationManifest {
  private readonly entries: ReadonlyArray<StaticFederationEntry>;

  constructor(entries: ReadonlyArray<StaticFederationEntry>) {
    this.entries = entries.map((e) => ({
      endpoint: normalizeEndpoint(e.endpoint),
      agentIds: [...e.agentIds],
    }));
  }

  static fromConfig(
    entries: ReadonlyArray<{ endpoint: string; agentIds: ReadonlyArray<string> }>,
  ): StaticFederationManifest {
    return new StaticFederationManifest(entries);
  }

  async isAllowed(params: { endpoint: string; agentId: string }): Promise<boolean> {
    const ep = normalizeEndpoint(params.endpoint);
    for (const entry of this.entries) {
      if (entry.endpoint !== ep) continue;
      if (entry.agentIds.includes("*")) return true;
      if (entry.agentIds.includes(params.agentId)) return true;
    }
    return false;
  }
}

interface StaticFederationEntry {
  readonly endpoint: string;
  readonly agentIds: ReadonlyArray<string>;
}

/** Trusted-environment passthrough — every (endpoint, agentId) is allowed. */
export class AllowAllFederationManifest implements FederationManifest {
  async isAllowed(): Promise<boolean> {
    return true;
  }
}

function normalizeEndpoint(endpoint: string): string {
  return endpoint.replace(/\/+$/, "");
}
