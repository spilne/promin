// ---------------------------------------------------------------------------
// DockerRuntime — runs steps as Docker containers via `docker` CLI
//
// Mounts a temp directory with input.json, runs the container,
// reads output.json from the same mount point.
// ---------------------------------------------------------------------------

import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ContainerRuntime, ContainerSpec, ContainerResult } from "./container-runtime.ts";

export interface DockerRuntimeConfig {
  /** Docker CLI path. Default: "docker". */
  dockerPath?: string;
  /** Base temp directory for mounts. Default: os.tmpdir(). */
  tmpDir?: string;
  /** Docker network to attach. */
  network?: string;
  /** Extra docker run arguments. */
  extraArgs?: string[];
}

export class DockerRuntime implements ContainerRuntime {
  private readonly dockerPath: string;
  private readonly tmpDir: string;
  private readonly network?: string;
  private readonly extraArgs: string[];

  constructor(config?: DockerRuntimeConfig) {
    this.dockerPath = config?.dockerPath ?? "docker";
    this.tmpDir = config?.tmpDir ?? tmpdir();
    this.network = config?.network;
    this.extraArgs = config?.extraArgs ?? [];
  }

  async run(params: {
    spec: ContainerSpec;
    input: string;
    stepName: string;
    workflowId: string;
  }): Promise<ContainerResult> {
    const { spec, input, stepName, workflowId } = params;
    const startTime = Date.now();

    // Create temp dir for I/O mount
    const dir = await mkdtemp(join(this.tmpDir, `promin-docker-${workflowId}-${stepName}-`));
    const inputPath = join(dir, "input.json");
    const outputPath = join(dir, "output.json");

    try {
      await writeFile(inputPath, input, "utf-8");

      // Build docker run command
      const args = ["run", "--rm"];

      // Mount I/O directory
      args.push("-v", `${dir}:/pipeline`);

      // Env vars
      args.push("-e", "PIPELINE_INPUT_PATH=/pipeline/input.json");
      args.push("-e", "PIPELINE_OUTPUT_PATH=/pipeline/output.json");
      args.push("-e", `PIPELINE_STEP_NAME=${stepName}`);
      args.push("-e", `PIPELINE_WORKFLOW_ID=${workflowId}`);

      if (spec.env) {
        for (const [k, v] of Object.entries(spec.env)) {
          args.push("-e", `${k}=${v}`);
        }
      }

      // Resource limits
      if (spec.memoryLimit) args.push("--memory", spec.memoryLimit);
      if (spec.cpuLimit) args.push("--cpus", spec.cpuLimit);

      // Working directory
      if (spec.workDir) args.push("-w", spec.workDir);

      // Network
      if (this.network) args.push("--network", this.network);

      // Extra args
      args.push(...this.extraArgs);

      // Image + command
      args.push(spec.image, ...spec.command);

      // Run docker
      const proc = Bun.spawn([this.dockerPath, ...args], {
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

      const exitCode = await proc.exited;
      if (timeoutHandle) clearTimeout(timeoutHandle);

      const stdout = await new Response(proc.stdout).text();
      const stderr = await new Response(proc.stderr).text();

      if (timedOut) {
        return {
          exitCode: -1,
          stdout,
          stderr: `Container timed out after ${spec.timeoutMs}ms`,
          durationMs: Date.now() - startTime,
        };
      }

      // Try to read output file
      let output: unknown;
      try {
        const outputJson = await readFile(outputPath, "utf-8");
        output = JSON.parse(outputJson);
      } catch {
        // No output file
      }

      return {
        exitCode,
        stdout,
        stderr,
        output,
        durationMs: Date.now() - startTime,
      };
    } finally {
      await rm(dir, { recursive: true, force: true }).catch(() => {});
    }
  }
}
