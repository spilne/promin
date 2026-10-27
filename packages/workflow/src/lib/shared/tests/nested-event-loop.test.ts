// A caller that settles an engine promise and then immediately enters a
// nested event loop (Bun's `expect(promise).resolves` spins one) must not
// stall the engine. The cases live in `nested-event-loop.fixture.ts` and run
// in a child process: a stall there never lets the process exit, so the
// child is killed after a deadline and the stall reported as a failure.

import { describe, expect, it } from "bun:test";
import { fileURLToPath } from "node:url";

const FIXTURE = fileURLToPath(new URL("./nested-event-loop.fixture.ts", import.meta.url));
// Generous next to the fixture's 2 s per-case timeouts, so a slow machine
// still reports the per-case failure rather than the kill.
const CHILD_DEADLINE_MS = 30_000;

describe("engine scheduler under a nested event loop", () => {
  it(
    "settles every engine promise and lets the process exit",
    async () => {
      const child = Bun.spawn({
        cmd: [process.execPath, "--conditions=@promin/source", "test", FIXTURE],
        stdout: "pipe",
        stderr: "pipe",
        env: { ...process.env, FORCE_COLOR: "0" },
      });
      // A real deadline for a child process, not engine time math, so it
      // does not go through a Clock.
      let killed = false;
      const deadline = setTimeout(() => {
        killed = true;
        child.kill(9);
      }, CHILD_DEADLINE_MS);
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      clearTimeout(deadline);

      const output = `${stdout}\n${stderr}`;
      expect({ killed, exitCode, output }).toMatchObject({ killed: false, exitCode: 0 });
      expect(output).toMatch(/\b5 pass\b/);
      expect(output).toMatch(/\b0 fail\b/);
    },
    CHILD_DEADLINE_MS + 5_000,
  );
});
