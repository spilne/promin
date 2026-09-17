import { agentRegistryTestSuite } from "../agent-registry-test-suite.ts";
import { InMemoryAgentRegistry } from "../in-memory-agent-registry.ts";

agentRegistryTestSuite(() => new InMemoryAgentRegistry());
