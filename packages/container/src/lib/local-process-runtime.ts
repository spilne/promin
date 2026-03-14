// ---------------------------------------------------------------------------
// LocalProcessRuntime — runs "containers" as local processes via Bun.spawn
//
// For dev/test. No Docker needed. The "container" is just a subprocess
// that reads input from a temp file and writes output to another.
// ---------------------------------------------------------------------------

import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ContainerRuntime, ContainerSpec, ContainerResult } from "./container-runtime.ts";

export interface LocalProcessRuntimeConfig {
  /** Base temp directory. Default: os.tmpdir(). */
  tmpDir?: string;
}

export class LocalProcessRuntime implements ContainerRuntime {
  private readonly tmpDir: string;

  constructor(config?: LocalProcessRuntimeConfig) {
    this.tmpDir = config?.tmpDir ?? tmpdir();
  }

  async run(params: {
    spec: ContainerSpec;
    input: string;
    stepName: string;
    workflowId: string;
  }): Promise<ContainerResult> {
    const { spec, input, stepName, workflowId } = params;
    const startTime = Date.now();

    // Create temp dir for I/O
    const dir = await mkdtemp(join(this.tmpDir, `promin-${workflowId}-${stepName}-`));
    const inputPath = join(dir, "input.json");
    const outputPath = join(dir, "output.json");

    try {
      // Write input
      await writeFile(inputPath, input, "utf-8");

      // Build env
      const env: Record<string, string> = {
        ...spec.env,
        PIPELINE_INPUT_PATH: inputPath,
        PIPELINE_OUTPUT_PATH: outputPath,
        PIPELINE_STEP_NAME: stepName,
        PIPELINE_WORKFLOW_ID: workflowId,
      };

      // Spawn process
      const proc = Bun.spawn(spec.command, {
        cwd: spec.workDir,
        env: { ...process.env, ...env },
        stdout: "pipe",
        stderr: "pipe",
      });

      // Handle timeout
      let timedOut = false;
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
      if (spec.timeoutMs) {
        timeoutHandle = setTimeout(() => {
          timedOut = true;
          proc.kill();
        }, spec.timeoutMs);
      }

      // Wait for completion
      const exitCode = await proc.exited;
      if (timeoutHandle) clearTimeout(timeoutHandle);

      const stdout = await new Response(proc.stdout).text();
      const stderr = await new Response(proc.stderr).text();

      if (timedOut) {
        return {
          exitCode: -1,
          stdout,
          stderr: `Timed out after ${spec.timeoutMs}ms`,
          durationMs: Date.now() - startTime,
        };
      }

      // Try to read output file
      let output: unknown;
      try {
        const outputJson = await readFile(outputPath, "utf-8");
        output = JSON.parse(outputJson);
      } catch {
        // No output file — stdout is the output
      }

      return {
        exitCode,
        stdout,
        stderr,
        output,
        durationMs: Date.now() - startTime,
      };
    } finally {
      // Cleanup temp dir
      await rm(dir, { recursive: true, force: true }).catch(() => {});
    }
  }
}
