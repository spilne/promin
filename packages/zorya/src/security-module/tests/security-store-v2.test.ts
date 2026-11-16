import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { securityStoreV2TestSuite } from "./security-store-v2-test-suite.ts";

securityStoreV2TestSuite("InMemorySecurityStore v2 conformance", {
  create: () => new InMemorySecurityStore(),
});
