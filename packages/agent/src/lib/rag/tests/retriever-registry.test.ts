import { InMemoryRetrieverRegistry } from "../retriever-registry.ts";
import { retrieverRegistryTestSuite } from "../retriever-registry-test-suite.ts";

retrieverRegistryTestSuite(() => new InMemoryRetrieverRegistry());
