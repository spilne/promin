import { InMemoryVersionedRegistry } from "../in-memory-versioned-registry.ts";
import { versionedRegistryTestSuite } from "../versioned-registry-test-suite.ts";

versionedRegistryTestSuite({
  name: "InMemoryVersionedRegistry",
  factory: ({ kind, clock }) => new InMemoryVersionedRegistry({ kind, clock }),
});
