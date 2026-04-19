// ---------------------------------------------------------------------------
// Video pipeline workflow — shared DAG used by the coordinator.
//
// Shape:
//   decode → [transcode (gpu) + metadata (default)]
//   transcode → thumbnail (cpu)
//   [thumbnail + metadata] → notify
//
// Coordinator reads the DAG from this definition and routes each step to
// its queue based on `routing` (see coordinator.ts). Workers run the real
// handlers — see steps.ts — matched by step name via a MapStepRegistry.
//
// The stepAsync bodies here are DAG placeholders; for distributed execution
// the coordinator never calls them, so they're just a declaration that
// "this step exists, these are its dependencies, here's the output type."
// In local (in-process) runs the body IS called — `bun run --mode local`
// would use these directly.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, type StepHandler } from "@promin/workflow";

export interface VideoInput {
  readonly videoId: string;
  readonly sourceUrl: string;
}

export interface DecodeResult {
  readonly format: string;
  readonly durationSec: number;
}

export interface TranscodeResult {
  readonly resolutions: ReadonlyArray<{ readonly label: string; readonly path: string }>;
}

export interface ThumbnailResult {
  readonly thumbnails: ReadonlyArray<string>;
}

export interface MetadataResult {
  readonly tags: ReadonlyArray<string>;
  readonly title: string;
}

export interface NotifyResult {
  readonly videoId: string;
  readonly deliveredAt: string;
}

/**
 * Build the video pipeline workflow as a pure `Workflow` definition —
 * no storage bound. The coordinator (or a registry) is responsible for
 * binding to a storage; submitters just hold the def.
 */
export function buildVideoWorkflow() {
  return (
    workflow<VideoInput>({ name: "video-pipeline", version: "1" })
      // Routing is per-step via `needs`. Only steps with hardware /
      // capability requirements declare them. Steps without `needs` run on
      // any worker — including the specialized GPU / CPU workers when
      // those are idle. That's a feature: the GPU box can help clear the
      // decode backlog when no transcode work is queued. Use a worker
      // "taint" (v2) if you ever need to reserve a specialized worker
      // exclusively for its specialty.
      .stepAsync("decode", async (): Promise<DecodeResult> => {
        throw new Error("decode body is placeholder — worker runs the real handler");
      })
      .stepAsync(
        "transcode",
        { dependsOn: ["decode"] },
        async (): Promise<TranscodeResult> => {
          throw new Error("transcode body is placeholder");
        },
        { needs: ["gpu"] },
      )
      .stepAsync(
        "thumbnail",
        { dependsOn: ["transcode"] },
        async (): Promise<ThumbnailResult> => {
          throw new Error("thumbnail body is placeholder");
        },
        { needs: ["cpu"] },
      )
      .stepAsync("metadata", { dependsOn: ["decode"] }, async (): Promise<MetadataResult> => {
        throw new Error("metadata body is placeholder");
      })
      .stepAsync(
        "notify",
        { dependsOn: ["thumbnail", "metadata"] },
        async (): Promise<NotifyResult> => {
          throw new Error("notify body is placeholder");
        },
      )
      .build()
  );
}

// ---------------------------------------------------------------------------
// Step handlers — shared by every worker. Each worker registers only the
// handlers for its queue (see worker-*.ts); tasks routed to a queue the
// worker doesn't cover stay claimable by another worker.
//
// Bodies are intentionally trivial sleep + log so the example runs
// anywhere without real GPUs, ffmpeg, storage, etc. Swap them out with
// real logic per worker image.
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** `decode` handler — default queue. */
export const decodeHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { videoId } = ctx.input as VideoInput;
    console.log(`[decode] ${ctx.workflowId} — probing ${videoId}`);
    await sleep(150);
    return { format: "mp4", durationSec: 90 } satisfies DecodeResult;
  });

/** `transcode` handler — GPU queue. */
export const transcodeHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    console.log(`[transcode] ${ctx.workflowId} — encoding 3 resolutions on GPU`);
    await sleep(800);
    return {
      resolutions: [
        { label: "1080p", path: "/out/1080p.mp4" },
        { label: "720p", path: "/out/720p.mp4" },
        { label: "480p", path: "/out/480p.mp4" },
      ],
    } satisfies TranscodeResult;
  });

/** `thumbnail` handler — CPU queue. */
export const thumbnailHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const transcode = (ctx.deps as { transcode?: TranscodeResult }).transcode ?? {
      resolutions: [],
    };
    console.log(
      `[thumbnail] ${ctx.workflowId} — extracting thumbs for ${transcode.resolutions.length} renditions`,
    );
    await sleep(250);
    return {
      thumbnails: transcode.resolutions.map((r) => `/thumbs/${r.label}.jpg`),
    } satisfies ThumbnailResult;
  });

/** `metadata` handler — default queue. */
export const metadataHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { videoId } = ctx.input as VideoInput;
    console.log(`[metadata] ${ctx.workflowId} — tagging ${videoId}`);
    await sleep(200);
    return { tags: ["demo", "promin"], title: `Video ${videoId}` } satisfies MetadataResult;
  });

/** `notify` handler — default queue. */
export const notifyHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { videoId } = ctx.input as VideoInput;
    console.log(`[notify] ${ctx.workflowId} — video ready: ${videoId}`);
    await sleep(50);
    return { videoId, deliveredAt: new Date().toISOString() } satisfies NotifyResult;
  });
