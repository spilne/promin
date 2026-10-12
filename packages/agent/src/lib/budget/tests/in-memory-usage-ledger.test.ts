import { FakeWallClock } from "@promin/workflow";
import { InMemoryUsageLedger } from "../usage-ledger.ts";
import { usageLedgerTestSuite } from "../usage-ledger-test-suite.ts";

usageLedgerTestSuite(() => {
  const clock = FakeWallClock.create(1_000_000);
  return {
    ledger: new InMemoryUsageLedger({ clock }),
    age: async (ms) => {
      clock.advance(ms);
    },
  };
});
