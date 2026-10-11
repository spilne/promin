import { afterEach, describe, expect, it } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeWallClock } from "@promin/workflow";
import { createShellTool } from "../shell-tool.ts";

const dirs: string[] = [];
afterEach(async () => {
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

async function workspace(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "promin-shell-"));
  dirs.push(dir);
  return dir;
}

/** True while a process with this pid exists. */
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

describe("createShellTool", () => {
  it("requires approval by default", () => {
    expect(createShellTool({ allowedCommands: ["echo"] }).requireApproval).toBe(true);
  });

  it("refuses an empty allowlist and credential env names", () => {
    expect(() => createShellTool({ allowedCommands: [] })).toThrow("at least one");
    expect(() =>
      createShellTool({ allowedCommands: ["echo"], inheritEnv: ["PATH", "OPENAI_API_KEY"] }),
    ).toThrow("OPENAI_API_KEY");
  });

  it("passes arguments as argv, never through a shell", async () => {
    const cwd = await workspace();
    const shell = createShellTool({ cwd, allowedCommands: ["echo"] });
    const out = await shell.execute({
      command: "echo",
      args: ["hi; touch pwned", "$(touch pwned2)"],
    });
    expect(out).toBe("exit 0\nhi; touch pwned $(touch pwned2)\n");
    expect(existsSync(join(cwd, "pwned"))).toBe(false);
    expect(existsSync(join(cwd, "pwned2"))).toBe(false);
  });

  it("matches the executable exactly against the allowlist", async () => {
    const cwd = await workspace();
    const shell = createShellTool({ cwd, allowedCommands: ["echo"] });
    for (const command of ["echo hi; touch pwned", "echo;touch", "./echo", "/bin/echo", "sh"]) {
      const out = await shell.execute({ command, args: [] });
      expect([command, out.startsWith("Rejected:")]).toEqual([command, true]);
    }
    expect(existsSync(join(cwd, "pwned"))).toBe(false);
  });

  it("runs the child with a scrubbed environment", async () => {
    process.env.PROMIN_TEST_API_KEY = "sk-should-not-leak";
    try {
      const shell = createShellTool({ allowedCommands: ["env"], env: { EXTRA: "1" } });
      const out = await shell.execute({ command: "env", args: [] });
      expect(out).not.toContain("sk-should-not-leak");
      expect(out).toContain("EXTRA=1");
      expect(out).toContain("PATH=");
    } finally {
      delete process.env.PROMIN_TEST_API_KEY;
    }
  });

  it("kills the whole process group on timeout, driven by the clock", async () => {
    const clock = FakeWallClock.create(0);
    const cwd = await workspace();
    // `sh` is allowlisted here only to fork a grandchild we can observe.
    const shell = createShellTool({ cwd, allowedCommands: ["sh"], timeoutMs: 5_000, clock });
    const done = shell.execute({ command: "sh", args: ["-c", "sleep 300 & echo $! > pid; wait"] });
    const pidFile = join(cwd, "pid");
    for (
      let i = 0;
      i < 2_000 && !(existsSync(pidFile) && (await readFile(pidFile, "utf8")).endsWith("\n"));
      i++
    ) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(clock.pendingCount()).toBeGreaterThan(0);
    clock.advance(5_000);
    const out = await done;
    expect(out).toStartWith("timed out after 5000 ms");
    const grandchild = Number((await readFile(pidFile, "utf8")).trim());
    expect(grandchild).toBeGreaterThan(0);
    for (let i = 0; i < 200 && alive(grandchild); i++) await new Promise((r) => setTimeout(r, 5));
    expect(alive(grandchild)).toBe(false);
  });

  it("caps output and stops a runaway producer", async () => {
    const shell = createShellTool({ allowedCommands: ["yes"], maxOutputBytes: 1_000 });
    const out = await shell.execute({ command: "yes", args: [] });
    expect(out).toStartWith("output exceeded 1000 bytes");
    expect(out.length).toBeLessThan(1_200);
  });
});
