/**
 * Kubernetes ML Pipeline — realistic production scenario
 *
 * Business flow:
 * 1. Data scientist kicks off a training experiment with a dataset and hyperparameters
 * 2. Training data is downloaded, cleaned, and split into train/test sets on cheap CPU nodes
 * 3. Model is trained on expensive GPU nodes with configurable batch size and learning rate
 * 4. Trained model is evaluated against accuracy and F1 thresholds on CPU nodes
 * 5. If the model meets quality gates, it is promoted to production; otherwise it is rejected
 * 6. On failure at any step, partial artifacts are cleaned up and the team is alerted
 *
 * Each step runs in its own Kubernetes pod with hardware matched to the workload.
 */

import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  MapStepRegistry,
  InMemoryStepQueue,
  createWorker,
  createCoordinator,
} from "@promin/core";
import { containerStep, K8sRuntime } from "@promin/container";

const storage = new InMemoryWorkflowStorage();
const stepQueue = new InMemoryStepQueue();

// ---------------------------------------------------------------------------
// Runtimes — different hardware for different steps
// ---------------------------------------------------------------------------

// GPU nodes for training
const gpuRuntime = new K8sRuntime({
  namespace: "ml-training",
  nodeSelector: { "nvidia.com/gpu": "true", "node-type": "p3.2xlarge" },
  serviceAccount: "ml-trainer",
  imagePullSecrets: ["ecr-creds"],
  ttlAfterFinished: 3600,
});

// CPU nodes for data prep and evaluation
const cpuRuntime = new K8sRuntime({
  namespace: "ml-training",
  nodeSelector: { "node-type": "m5.xlarge" },
  serviceAccount: "ml-trainer",
  imagePullSecrets: ["ecr-creds"],
});

// ---------------------------------------------------------------------------
// Step implementations — each runs in its own container
// ---------------------------------------------------------------------------

const registry = new MapStepRegistry();

// Data preparation: download dataset, clean, split into train/test
// Runs on CPU — data processing doesn't need GPU
const [prepHandler, prepOptions] = containerStep({
  spec: {
    image: "company-registry.com/ml-data-prep:v3",
    command: ["python", "prepare_data.py"],
    memoryLimit: "4Gi",
    cpuLimit: "2",
    timeoutMs: 1800_000, // 30 min
    env: {
      S3_BUCKET: "ml-datasets",
      TRAIN_SPLIT: "0.8",
    },
  },
  runtime: cpuRuntime,
  options: {
    retry: { maxRetries: 2 },
  },
});
registry.register("prepare-data", prepHandler, prepOptions);

// Model training: fine-tune on prepared dataset
// Runs on GPU — needs CUDA
const [trainHandler, trainOptions] = containerStep({
  spec: {
    image: "company-registry.com/model-trainer:v5",
    command: ["python", "train.py"],
    memoryLimit: "16Gi",
    cpuLimit: "4",
    gpu: true,
    timeoutMs: 7200_000, // 2 hours
    env: {
      WANDB_PROJECT: "product-recommendations",
      MIXED_PRECISION: "true",
      BATCH_SIZE: "32",
    },
  },
  runtime: gpuRuntime,
  options: {
    retry: { maxRetries: 1 },
    onFailure: { fallback: () => ({ status: "failed", modelPath: null, reason: "training crashed" }) },
    compensate: ({ result }) =>
      Pipeline.fromPromise(async () => {
        // Clean up partial model artifacts from S3
        console.log(`Cleaning up model artifacts: ${(result as any)?.modelPath}`);
      }),
  },
});
registry.register("train-model", trainHandler, trainOptions);

// Evaluation: run test suite against trained model
// Runs on CPU — inference is lighter than training
const [evalHandler, evalOptions] = containerStep({
  spec: {
    image: "company-registry.com/model-evaluator:v2",
    command: ["python", "evaluate.py"],
    memoryLimit: "8Gi",
    cpuLimit: "4",
    timeoutMs: 1800_000,
    env: {
      MIN_ACCURACY: "0.85",
      MIN_F1: "0.80",
    },
  },
  runtime: cpuRuntime,
});
registry.register("evaluate-model", evalHandler, evalOptions);

// ---------------------------------------------------------------------------
// ML Pipeline workflow
// ---------------------------------------------------------------------------

const mlPipeline = workflow<{
  experimentId: string;
  dataset: string;
  modelType: string;
  hyperparams: Record<string, unknown>;
}>({
  name: "ml-training-pipeline",
  storage,
  type: "ml",
  metadata: { team: "data-science" },
  retry: { maxRetries: 1, baseDelayMs: 60_000 },
  compensate: {
    trigger: "after-retries",
    onComplete: ({ compensatedSteps, error }) =>
      Pipeline.fromPromise(async () => {
        console.log(`Pipeline failed. Compensated: ${compensatedSteps.join(", ")}`);
        // await slack.notify("#ml-alerts", `Training failed: ${error}`);
      }),
  },
  dispatch: {
    stepQueue,
    routing: {
      "prepare-data": "cpu",
      "train-model": "gpu",
      "evaluate-model": "cpu",
    },
  },
})
  // Local: validate experiment config
  .step("validate", ({ input }) =>
    Pipeline.succeed({
      experimentId: input.experimentId,
      dataset: input.dataset,
      modelType: input.modelType,
      hyperparams: input.hyperparams,
      startedAt: new Date().toISOString(),
    }),
  )

  // Container (CPU): prepare training data
  .step("prepare-data", { dependsOn: ["validate"] }, ({ deps }) =>
    Pipeline.succeed({
      trainPath: `s3://ml-data/${deps.validate.dataset}/train.parquet`,
      testPath: `s3://ml-data/${deps.validate.dataset}/test.parquet`,
      rows: 1_000_000,
    }),
  )

  // Container (GPU): train model
  .step("train-model", { dependsOn: ["prepare-data"] }, ({ deps }) =>
    Pipeline.succeed({
      modelPath: `s3://ml-models/${deps["prepare-data"].trainPath}/model.pt`,
      trainLoss: 0.05,
      epochs: 10,
      status: "completed",
    }),
  )

  // Container (CPU): evaluate model
  .step("evaluate-model", { dependsOn: ["train-model"] }, ({ deps }) =>
    Pipeline.succeed({
      accuracy: 0.92,
      f1: 0.89,
      modelPath: deps["train-model"].modelPath,
      passed: true,
    }),
  )

  // Local: decide deploy or reject
  .branch("deploy-decision", {
    condition: (result) => result.passed && result.accuracy > 0.85,
    ifTrue: ({ prev }): Pipeline<{ deployed: boolean; modelPath: string | null }, never> =>
      Pipeline.fromPromise(async () => {
        console.log(`Deploying model with accuracy ${prev.accuracy}`);
        // await modelRegistry.promote(prev.modelPath, "production");
        return { deployed: true, modelPath: prev.modelPath };
      }),
    ifFalse: ({ prev }): Pipeline<{ deployed: boolean; modelPath: string | null }, never> =>
      Pipeline.fromPromise(async () => {
        console.log(`Model rejected: accuracy ${prev.accuracy} below threshold`);
        return { deployed: false, modelPath: null };
      }),
  })
  .build();

// ---------------------------------------------------------------------------
// Production setup
// ---------------------------------------------------------------------------

async function startPipeline() {
  // CPU workers — run on standard compute nodes
  const cpuWorker = createWorker({
    storage,
    stepQueue,
    registry,
    queues: ["cpu"],
    concurrency: 5,
    pollIntervalMs: 2000,
  });

  // GPU workers — run on GPU nodes (expensive, fewer)
  const gpuWorker = createWorker({
    storage,
    stepQueue,
    registry,
    queues: ["gpu"],
    concurrency: 2, // limited by GPU count
    pollIntervalMs: 2000,
  });

  // Coordinator — runs on API server
  const coordinator = createCoordinator({
    storage,
    stepQueue,
    routing: {
      "prepare-data": "cpu",
      "train-model": "gpu",
      "evaluate-model": "cpu",
    },
    pollIntervalMs: 5000,
  });

  // Start all
  void cpuWorker.start();
  void gpuWorker.start();
  void coordinator.start();

  // Submit experiment
  await coordinator.submit({
    workflow: mlPipeline,
    workflowId: `exp-${Date.now()}`,
    input: {
      experimentId: "exp-042",
      dataset: "user-interactions-2026-q1",
      modelType: "transformer",
      hyperparams: { learningRate: 0.001, batchSize: 32, epochs: 10 },
    },
  });
}

export { mlPipeline, registry, startPipeline };
