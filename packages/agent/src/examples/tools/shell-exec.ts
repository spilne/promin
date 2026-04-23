import { tool } from "../../lib/index.ts";
import { z } from "zod";

export default tool({
  name: "shell-exec",
  description: "Execute an arbitrary shell command and return stdout + stderr.",
  parameters: z.object({ command: z.string() }),
  execute: async ({ command }) => {
    const proc = Bun.spawn(["sh", "-c", command], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    await proc.exited;
    return stdout + (stderr ? "\nSTDERR:\n" + stderr : "");
  },
});
