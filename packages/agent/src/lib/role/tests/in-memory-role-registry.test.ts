import { roleRegistryTestSuite } from "../role-registry-test-suite.ts";
import { InMemoryRoleRegistry } from "../in-memory-role-registry.ts";

roleRegistryTestSuite(() => new InMemoryRoleRegistry());
