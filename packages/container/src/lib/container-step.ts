// ---------------------------------------------------------------------------
// ContainerStep — register a container-backed step in a StepRegistry
//
// Bridges ContainerRuntime to the distributed worker step system.
// Input is serialized via JSON, sent to the container, output parsed back.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import type { StepHandler, WorkerStepOptions } from "@promin/core";
import type { ContainerRuntime, ContainerSpec } from "./container-runtime.ts";

export interface ContainerStepConfig {
  /** Container spec — image, command, env, limits. */
  spec: ContainerSpec;
  /** Container runtime to use (Local, Docker, K8s). */
  runtime: ContainerRuntime;
  /** Step-level options (retry, onFailure, compensate). */
  options?: WorkerStepOptions;
}

/**
 * Create a step handler that executes inside a container.
 *
 * The handler serializes ctx.prev (or ctx.input for root steps) to JSON,
 * passes it to the container, and parses the output.
 *
 * @example
 * ```ts
 * const registry = new MapStepRegistry();
 *
 * registry.register(
 *   "train-model",
 *   ...containerStep({
 *     spec: {
 *       image: "my-ml-image:latest",
 *       command: ["python", "train.py"],
 *       memoryLimit: "4g",
 *       gpu: true,
 *     },
 *     runtime: new DockerRuntime(),
 *   }),
 * );
 * ```
 */
export function containerStep(
  config: ContainerStepConfig,
): [StepHandler, WorkerStepOptions | undefined] {
  const { spec, runtime, options } = config;

  const handler: StepHandler = (ctx) => {
    const inputJson = JSON.stringify({
      input: ctx.input,
      prev: ctx.prev,
      deps: ctx.deps,
      workflowId: ctx.workflowId,
      stepName: ctx.stepName,
      attempt: ctx.attempt,
    });

    return Pipeline.fromPromise(async () => {
      const result = await runtime.run({
        spec,
        input: inputJson,
        stepName: ctx.stepName,
        workflowId: ctx.workflowId,
      });

      if (result.exitCode !== 0) {
        throw new Error(
          `Container step "${ctx.stepName}" failed (exit ${result.exitCode}): ${result.stderr || result.stdout}`,
        );
      }

      // Try to parse output JSON, fall back to stdout
      if (result.output !== undefined) return result.output;

      try {
        return JSON.parse(result.stdout);
      } catch {
        return result.stdout.trim();
      }
    });
  };

  return [handler, options];
}
