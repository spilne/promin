import type { DurableScheduleConfig } from "@promin/workflow";

export const videoTranscodesEvery45s: DurableScheduleConfig = {
  id: "video-transcodes-every-45s",
  name: "Video transcodes every 45s",
  intervalMs: 45_000,
  enabled: true,
  namespace: "tenant-b",
  metadata: {
    workflowName: "video-transcode",
    namespace: "tenant-b",
    input: { videoId: "vid-demo-1", url: "https://example.com/video.mp4" },
  },
};
