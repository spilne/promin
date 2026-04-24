// ---------------------------------------------------------------------------
// video-transcode workflow — fan-out (one input, N parallel renders), fan-in
// (publish requires all formats complete).
//
// DAG:
//    download ──▶ analyze ─┬─▶ transcode-1080p ─┐
//                          ├─▶ transcode-720p ──┼─▶ publish ─▶ notify
//                          └─▶ transcode-480p ──┘
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface VideoTranscodeInput {
  videoId: string;
  url?: string;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

export const videoTranscodeWorkflow = workflow<VideoTranscodeInput>({
  name: "video-transcode",
  type: "media",
})
  .stepAsync("download", async ({ input }) => {
    await sleep(delay(3_000, 10_000));
    return { videoId: input.videoId, bytes: Math.floor(Math.random() * 500_000_000) };
  })
  .stepAsync("analyze", { dependsOn: ["download"] }, async ({ deps }) => {
    await sleep(delay(2_000, 6_000));
    return {
      videoId: deps.download.videoId,
      duration: 60 + Math.floor(Math.random() * 600),
      codec: "h264",
    };
  })
  .stepAsync("transcode-1080p", { dependsOn: ["analyze"] }, async ({ deps }) => {
    await sleep(delay(8_000, 25_000));
    if (Math.random() < 0.08) throw new Error("FFmpeg crashed on 1080p");
    return { format: "1080p", videoId: deps.analyze.videoId, bytes: 120_000_000 };
  })
  .stepAsync("transcode-720p", { dependsOn: ["analyze"] }, async ({ deps }) => {
    await sleep(delay(5_000, 18_000));
    return { format: "720p", videoId: deps.analyze.videoId, bytes: 70_000_000 };
  })
  .stepAsync("transcode-480p", { dependsOn: ["analyze"] }, async ({ deps }) => {
    await sleep(delay(3_000, 12_000));
    return { format: "480p", videoId: deps.analyze.videoId, bytes: 40_000_000 };
  })
  .stepAsync(
    "publish",
    { dependsOn: ["transcode-1080p", "transcode-720p", "transcode-480p"] },
    async ({ deps }) => {
      await sleep(delay(2_000, 8_000));
      return {
        videoId: deps["transcode-1080p"].videoId,
        formats: ["1080p", "720p", "480p"],
        publishedAt: new Date().toISOString(),
      };
    },
  )
  .stepAsync("notify", { dependsOn: ["publish"] }, async ({ deps }) => {
    await sleep(delay(1_000, 4_000));
    return { videoId: deps.publish.videoId, notified: ["creator", "subscribers"] };
  })
  .build();
