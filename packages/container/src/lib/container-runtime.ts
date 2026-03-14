// ---------------------------------------------------------------------------
// ContainerRuntime — interface for executing steps in isolated containers
//
// Input is serialized to JSON, mounted in the container.
// Container runs a command, writes output to a path.
// Runtime reads the output and returns it.
// ---------------------------------------------------------------------------

export interface ContainerSpec {
  /** Docker image (e.g. "python:3.12-slim", "node:20-alpine"). */
  image: string;
  /** Command to run inside the container. */
  command: string[];
  /** Environment variables. */
  env?: Record<string, string>;
  /** Working directory inside the container. */
  workDir?: string;
  /** Memory limit (e.g. "512m", "2g"). */
  memoryLimit?: string;
  /** CPU limit (e.g. "0.5", "2"). */
  cpuLimit?: string;
  /** Timeout in ms. Container is killed after this. */
  timeoutMs?: number;
  /** GPU request (for K8s runtime). */
  gpu?: boolean;
}

export interface ContainerResult {
  /** Exit code of the container process. */
  exitCode: number;
  /** Stdout from the container. */
  stdout: string;
  /** Stderr from the container. */
  stderr: string;
  /** Parsed output (from /pipeline/output/data.json if it exists). */
  output?: unknown;
  /** Execution duration in ms. */
  durationMs: number;
}

export interface ContainerRuntime {
  /**
   * Run a container with the given spec and input.
   *
   * The runtime:
   * 1. Serializes input to JSON
   * 2. Makes it available to the container (mount, env, or stdin)
   * 3. Runs the container
   * 4. Captures stdout/stderr and output
   * 5. Returns the result
   */
  run(params: {
    spec: ContainerSpec;
    input: string; // JSON-serialized input
    stepName: string;
    workflowId: string;
  }): Promise<ContainerResult>;
}
