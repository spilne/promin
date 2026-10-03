/**
 * Batch processing with mapOver — fan-out with per-element retry.
 *
 * If the workflow crashes, only unprocessed items are retried.
 */

import { TaggedError, succeed, tryPromise } from "@spilne/perfect-core";
import { flow } from "@promin/workflow";

interface Image {
  id: string;
  url: string;
}

class ResizeError extends TaggedError("ResizeError")<{ message: string }>() {}

const processImages = flow<{ images: Image[] }>("image-batch")
  .step("validate", ({ input }) =>
    succeed(input.images.filter((img) => img.url.startsWith("https://"))),
  )
  .mapOver("resize", { array: "validate", concurrency: 10 }, (image) =>
    tryPromise(
      async () => {
        const resized = await resizeImage(image.url, { width: 800 });
        return { id: image.id, resizedUrl: resized.url };
      },
      (e) => new ResizeError({ message: String(e) }),
    ).retry({ times: 2 }),
  );

const results = await processImages.execute({
  images: [
    { id: "1", url: "https://cdn.example.com/photo1.jpg" },
    { id: "2", url: "https://cdn.example.com/photo2.jpg" },
    { id: "3", url: "https://cdn.example.com/photo3.jpg" },
  ],
});

console.log(results); // [{ id: "1", resizedUrl: "..." }, ...]

// Stubs
async function resizeImage(_url: string, _opts: { width: number }) {
  return { url: "https://cdn.example.com/resized.jpg" };
}
