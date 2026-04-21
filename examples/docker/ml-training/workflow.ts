// ---------------------------------------------------------------------------
// ML training workflow — workflow versioning demo.
//
// Shape (v1):
//   prepare-data  (needs: cpu)
//         │
//         ▼
//   train-model   (needs: gpu, concurrency=1)
//         │
//         ▼
//   evaluate      (needs: cpu)
//
// Shape (v2 — adds post-evaluation optimization step):
//   prepare-data  (needs: cpu)
//         │
//         ▼
//   train-model   (needs: gpu)
//         │
//         ▼
//   evaluate      (needs: cpu)
//         │
//         ▼
//   optimize      (needs: cpu)
//
// Versioning: both v1 and v2 are registered in the coordinator. The
// coordinator alternates submitting v1 and v2 experiments — workers handle
// whichever version is dispatched without needing to know the version.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, type StepHandler } from "@promin/workflow";

export interface ModelInput {
  readonly experimentId: string;
  readonly datasetPath: string;
  readonly epochs: number;
}

export interface PreparedData {
  readonly trainSize: number;
  readonly testSize: number;
  readonly featureCount: number;
}

export interface TrainedModel {
  readonly modelId: string;
  readonly trainAccuracy: number;
  readonly epochsCompleted: number;
}

export interface EvalResult {
  readonly modelId: string;
  readonly testAccuracy: number;
  readonly passed: boolean;
}

export interface OptimizeResult {
  readonly bestLearningRate: number;
  readonly bestAccuracy: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function buildModelWorkflow(version: "1" | "2") {
  const base = workflow<ModelInput>({
    name: "ml-training",
    version,
  })
    .stepAsync(
      "prepare-data",
      async (): Promise<PreparedData> => {
        throw new Error("placeholder — worker runs the real handler");
      },
      { needs: ["cpu"] },
    )
    .stepAsync(
      "train-model",
      { dependsOn: ["prepare-data"] },
      async (): Promise<TrainedModel> => {
        throw new Error("placeholder");
      },
      { needs: ["gpu"] },
    )
    .stepAsync(
      "evaluate",
      { dependsOn: ["train-model"] },
      async (): Promise<EvalResult> => {
        throw new Error("placeholder");
      },
      { needs: ["cpu"] },
    );

  if (version === "2") {
    return base
      .stepAsync(
        "optimize",
        { dependsOn: ["evaluate"] },
        async (): Promise<OptimizeResult> => {
          throw new Error("placeholder");
        },
        { needs: ["cpu"] },
      )
      .build();
  }

  return base.build();
}

// ---------------------------------------------------------------------------
// Step handlers — registered in the appropriate worker.
// ---------------------------------------------------------------------------

export const prepareDataHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { datasetPath, epochs } = ctx.input as ModelInput;
    console.log(`[prepare-data] ${ctx.workflowId} — loading ${datasetPath} for ${epochs} epochs`);
    await sleep(400);
    const total = 10_000;
    const trainSize = Math.floor(total * 0.8);
    return {
      trainSize,
      testSize: total - trainSize,
      featureCount: 128,
    } satisfies PreparedData;
  });

export const trainModelHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { experimentId, epochs } = ctx.input as ModelInput;
    const prep = (ctx.deps as { "prepare-data"?: PreparedData })["prepare-data"];
    console.log(
      `[train-model] ${ctx.workflowId} — training ${experimentId} ` +
        `(${epochs} epochs, ${prep?.trainSize} samples, ${prep?.featureCount} features)`,
    );
    // Simulate epoch-by-epoch training with progress logs
    for (let e = 1; e <= epochs; e++) {
      await sleep(200);
      const loss = +(1.0 / e).toFixed(3);
      console.log(`[train-model] ${ctx.workflowId} — epoch ${e}/${epochs} loss=${loss}`);
    }
    const modelId = `model-${experimentId}-${Date.now()}`;
    return {
      modelId,
      trainAccuracy: 0.85 + Math.random() * 0.1,
      epochsCompleted: epochs,
    } satisfies TrainedModel;
  });

export const evaluateHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const trained = (ctx.deps as { "train-model"?: TrainedModel })["train-model"];
    console.log(`[evaluate] ${ctx.workflowId} — evaluating model ${trained?.modelId}`);
    await sleep(300);
    const testAccuracy = (trained?.trainAccuracy ?? 0.8) - 0.02 + Math.random() * 0.04;
    const passed = testAccuracy >= 0.8;
    console.log(
      `[evaluate] ${ctx.workflowId} — accuracy=${testAccuracy.toFixed(3)} passed=${passed}`,
    );
    return {
      modelId: trained?.modelId ?? "unknown",
      testAccuracy,
      passed,
    } satisfies EvalResult;
  });

export const optimizeHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const evaled = (ctx.deps as { evaluate?: EvalResult })["evaluate"];
    console.log(`[optimize] ${ctx.workflowId} — tuning hyperparams for ${evaled?.modelId}`);
    await sleep(250);
    const bestLearningRate = 0.001 * (1 + Math.random());
    const bestAccuracy = (evaled?.testAccuracy ?? 0.8) + 0.01;
    console.log(
      `[optimize] ${ctx.workflowId} — best lr=${bestLearningRate.toFixed(5)} acc=${bestAccuracy.toFixed(3)}`,
    );
    return { bestLearningRate, bestAccuracy } satisfies OptimizeResult;
  });
