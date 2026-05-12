import { InMemoryDagRegistry } from "../registry.ts";
import { dagRegistryTestSuite } from "../dag-registry-test-suite.ts";

dagRegistryTestSuite(() => new InMemoryDagRegistry());
