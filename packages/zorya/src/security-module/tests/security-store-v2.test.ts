import { FakeWallClock } from "@promin/workflow";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { securityStoreV2TestSuite } from "./security-store-v2-test-suite.ts";

securityStoreV2TestSuite("InMemorySecurityStore v2 conformance", {
  create: () => new InMemorySecurityStore(),
  createClocked: () => {
    const clock = FakeWallClock.create(1_000);
    return {
      store: new InMemorySecurityStore({ clock }),
      currentTimeMs: () => clock.currentTimeMs(),
      advance: (ms: number) => clock.advance(ms),
    };
  },
});
