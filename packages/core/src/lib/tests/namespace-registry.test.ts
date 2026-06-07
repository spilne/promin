import { InMemoryNamespaceRegistry } from "../namespace-registry.ts";
import { namespaceRegistryTestSuite } from "../namespace-registry-test-suite.ts";

namespaceRegistryTestSuite(() => new InMemoryNamespaceRegistry());
