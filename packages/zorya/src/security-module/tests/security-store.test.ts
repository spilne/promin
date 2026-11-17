import { FakeWallClock } from "@promin/workflow";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { securityStoreTestSuite } from "./security-store-test-suite.ts";

securityStoreTestSuite("InMemorySecurityStore conformance", {
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
