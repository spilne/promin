import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { stepQueueTestSuite } from "../step-queue-test-suite.ts";
import { InMemoryLeaderLeases } from "../../scheduler/leader-lease.ts";

stepQueueTestSuite(({ maxDeliveries, clock }) => new InMemoryStepQueue({ maxDeliveries, clock }), {
  fakeClock: true,
  leaseFenced: () => {
    const leases = new InMemoryLeaderLeases();
    return { queue: new InMemoryStepQueue({ leaderLeases: leases }), leases };
  },
});
