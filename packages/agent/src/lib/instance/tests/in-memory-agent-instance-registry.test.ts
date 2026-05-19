// ---------------------------------------------------------------------------
// `InMemoryAgentInstanceRegistry` — the conformance baseline every
// persistent backend must match. Runs the shared
// `agentInstanceRegistryTestSuite` (resolveOrCreate idempotency,
// deterministic ids, namespace isolation, list filters + order, update,
// delete, and the wipeAgentInstance cascade).
// ---------------------------------------------------------------------------

import { agentInstanceRegistryTestSuite } from "../agent-instance-registry-test-suite.ts";
import { InMemoryAgentInstanceRegistry } from "../in-memory-agent-instance-registry.ts";

agentInstanceRegistryTestSuite(() => new InMemoryAgentInstanceRegistry());
