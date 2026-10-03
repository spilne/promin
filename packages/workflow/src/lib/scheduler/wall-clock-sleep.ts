// ---------------------------------------------------------------------------
// wallClockSleep — an Eff that waits on an injected WallClock timer.
// ---------------------------------------------------------------------------

import { async, succeed, type Eff } from "@spilne/perfect-core";
import type { WallClock } from "../shared/wall-clock.ts";

/**
 * Sleep on a `WallClock` rather than perfect's fiber-level `Clock` service,
 * so scheduler waits follow the scheduler's injected clock
 * (`FakeWallClock.advance` drives them in tests). Interruption clears the
 * timer, so a stopped consumer leaves nothing pending on the clock.
 */
export function wallClockSleep(params: { clock: WallClock; ms: number }): Eff<void> {
  return async<void>((resume) => {
    const handle = params.clock.setTimeout(() => resume(succeed(undefined)), params.ms);
    return () => handle.clear();
  }).orDie();
}
