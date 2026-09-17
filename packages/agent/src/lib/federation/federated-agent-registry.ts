// ---------------------------------------------------------------------------
// FederatedAgentRegistry — decorator around any AgentRegistry impl that
// gates `backend.type === 'remote'` registrations against a
// FederationManifest. Other operations (get / list / versions /
// unregister) pass through unchanged.
//
// Compose by wrapping at the host's wiring site:
//
//   const base = SqliteAgentRegistry.make({ db });
//   const registry = new FederatedAgentRegistry(base, manifest);
//
// Hosts that don't need federation gating skip this wrapper.
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  ListAgentsParams,
  RegisterAgentInput,
  RegisteredAgent,
} from "../registry/types.ts";
import { FederationManifestError, type FederationManifest } from "./types.ts";

export class FederatedAgentRegistry implements AgentRegistry {
  constructor(
    private readonly inner: AgentRegistry,
    private readonly manifest: FederationManifest,
  ) {}

  async register(input: RegisterAgentInput): Promise<RegisteredAgent> {
    if (input.backend.type === "remote") {
      const allowed = await this.manifest.isAllowed({
        endpoint: input.backend.endpoint,
        agentId: input.id,
      });
      if (!allowed) {
        throw new FederationManifestError(input.backend.endpoint, input.id);
      }
    }
    return this.inner.register(input);
  }

  async get(id: string, version?: string): Promise<RegisteredAgent | null> {
    return this.inner.get(id, version);
  }

  async list(params?: ListAgentsParams): Promise<RegisteredAgent[]> {
    return this.inner.list(params);
  }

  async versions(id: string): Promise<RegisteredAgent[]> {
    return this.inner.versions(id);
  }

  async unregister(id: string, version?: string): Promise<void> {
    return this.inner.unregister(id, version);
  }
}
