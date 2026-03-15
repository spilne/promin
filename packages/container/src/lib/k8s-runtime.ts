// ---------------------------------------------------------------------------
// K8sRuntime — runs steps as Kubernetes Jobs
//
// Creates a K8s Job with input mounted via ConfigMap, waits for completion,
// reads output from pod logs or a shared volume.
// ---------------------------------------------------------------------------

import * as k8s from "@kubernetes/client-node";
import type { ContainerRuntime, ContainerSpec, ContainerResult } from "./container-runtime.ts";

export interface K8sRuntimeConfig {
  /** Kubernetes namespace. Default: "default". */
  namespace?: string;
  /** Path to kubeconfig. Default: auto-detect (in-cluster or ~/.kube/config). */
  kubeConfigPath?: string;
  /** Use in-cluster config. Default: false (auto-detect). */
  inCluster?: boolean;
  /** Node selector for scheduling (e.g. { "nvidia.com/gpu": "true" }). */
  nodeSelector?: Record<string, string>;
  /** Service account for the job. */
  serviceAccount?: string;
  /** Image pull secrets. */
  imagePullSecrets?: string[];
  /** Job TTL after completion (seconds). Default: 600. */
  ttlAfterFinished?: number;
  /** How often to poll for job completion (ms). Default: 2000. */
  pollIntervalMs?: number;
}

export class K8sRuntime implements ContainerRuntime {
  private readonly namespace: string;
  private readonly kc: k8s.KubeConfig;
  private readonly batchApi: k8s.BatchV1Api;
  private readonly coreApi: k8s.CoreV1Api;
  private readonly nodeSelector?: Record<string, string>;
  private readonly serviceAccount?: string;
  private readonly imagePullSecrets?: string[];
  private readonly ttlAfterFinished: number;
  private readonly pollIntervalMs: number;

  constructor(config?: K8sRuntimeConfig) {
    this.namespace = config?.namespace ?? "default";
    this.ttlAfterFinished = config?.ttlAfterFinished ?? 600;
    this.pollIntervalMs = config?.pollIntervalMs ?? 2000;
    this.nodeSelector = config?.nodeSelector;
    this.serviceAccount = config?.serviceAccount;
    this.imagePullSecrets = config?.imagePullSecrets;

    this.kc = new k8s.KubeConfig();
    if (config?.inCluster) {
      this.kc.loadFromCluster();
    } else if (config?.kubeConfigPath) {
      this.kc.loadFromFile(config.kubeConfigPath);
    } else {
      this.kc.loadFromDefault();
    }

    this.batchApi = this.kc.makeApiClient(k8s.BatchV1Api);
    this.coreApi = this.kc.makeApiClient(k8s.CoreV1Api);
  }

  async run(params: {
    spec: ContainerSpec;
    input: string;
    stepName: string;
    workflowId: string;
  }): Promise<ContainerResult> {
    const { spec, input, stepName, workflowId } = params;
    const startTime = Date.now();
    const jobName = sanitizeName(`promin-${workflowId}-${stepName}-${Date.now()}`);

    // Create ConfigMap with input
    const configMapName = `${jobName}-input`;
    await this.coreApi.createNamespacedConfigMap({
      namespace: this.namespace,
      body: {
        metadata: {
          name: configMapName,
          labels: { "promin/workflow": workflowId, "promin/step": stepName },
        },
        data: { "input.json": input },
      },
    });

    try {
      // Build Job spec
      const envVars: k8s.V1EnvVar[] = [
        { name: "PIPELINE_INPUT_PATH", value: "/pipeline/input/input.json" },
        { name: "PIPELINE_OUTPUT_PATH", value: "/pipeline/output/output.json" },
        { name: "PIPELINE_STEP_NAME", value: stepName },
        { name: "PIPELINE_WORKFLOW_ID", value: workflowId },
      ];

      if (spec.env) {
        for (const [k, v] of Object.entries(spec.env)) {
          envVars.push({ name: k, value: v });
        }
      }

      const resources: k8s.V1ResourceRequirements = {};
      if (spec.memoryLimit || spec.cpuLimit || spec.gpu) {
        resources.limits = {};
        if (spec.memoryLimit) resources.limits["memory"] = spec.memoryLimit;
        if (spec.cpuLimit) resources.limits["cpu"] = spec.cpuLimit;
        if (spec.gpu) resources.limits["nvidia.com/gpu"] = "1";
      }

      const job: k8s.V1Job = {
        metadata: {
          name: jobName,
          labels: { "promin/workflow": workflowId, "promin/step": stepName },
        },
        spec: {
          ttlSecondsAfterFinished: this.ttlAfterFinished,
          backoffLimit: 0,
          activeDeadlineSeconds: spec.timeoutMs ? Math.ceil(spec.timeoutMs / 1000) : undefined,
          template: {
            spec: {
              restartPolicy: "Never",
              serviceAccountName: this.serviceAccount,
              nodeSelector: this.nodeSelector,
              imagePullSecrets: this.imagePullSecrets?.map((name) => ({ name })),
              containers: [
                {
                  name: "step",
                  image: spec.image,
                  command: spec.command,
                  workingDir: spec.workDir,
                  env: envVars,
                  resources,
                  volumeMounts: [
                    { name: "input", mountPath: "/pipeline/input", readOnly: true },
                    { name: "output", mountPath: "/pipeline/output" },
                  ],
                },
              ],
              volumes: [
                {
                  name: "input",
                  configMap: { name: configMapName },
                },
                {
                  name: "output",
                  emptyDir: {},
                },
              ],
            },
          },
        },
      };

      // Create the Job
      await this.batchApi.createNamespacedJob({ namespace: this.namespace, body: job });

      // Poll for completion
      let succeeded = false;

      while (true) {
        await new Promise((r) => setTimeout(r, this.pollIntervalMs));

        const jobStatus = (await this.batchApi.readNamespacedJob({
          namespace: this.namespace,
          name: jobName,
        })) as unknown as k8s.V1Job;

        if (jobStatus.status?.succeeded && jobStatus.status.succeeded > 0) {
          succeeded = true;
          break;
        }
        if (jobStatus.status?.failed && jobStatus.status.failed > 0) {
          break;
        }
      }

      // Get pod logs
      const podList = (await this.coreApi.listNamespacedPod({
        namespace: this.namespace,
        labelSelector: `job-name=${jobName}`,
      })) as unknown as k8s.V1PodList;

      let stdout = "";
      let stderr = "";

      if (podList.items.length > 0) {
        const podName = podList.items[0]!.metadata!.name!;
        try {
          const logs = await this.coreApi.readNamespacedPodLog({
            namespace: this.namespace,
            name: podName,
            container: "step",
          });
          stdout = typeof logs === "string" ? logs : "";
        } catch {
          // Logs may not be available
        }
      }

      return {
        exitCode: succeeded ? 0 : 1,
        stdout,
        stderr,
        output: undefined, // K8s can't easily read output file — use stdout or external storage
        durationMs: Date.now() - startTime,
      };
    } finally {
      // Cleanup ConfigMap
      await this.coreApi
        .deleteNamespacedConfigMap({ namespace: this.namespace, name: configMapName })
        .catch(() => {});
    }
  }
}

function sanitizeName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 63);
}
