/**
 * Process a batch of images with bounded concurrency.
 * mapOver fans out over the array — each item runs as a separate task.
 * If the workflow crashes, only unprocessed items are retried.
 */

import { flow } from "@promin/workflow";
import { Pipeline } from "@promin/core";

interface Image {
  id: string;
  url: string;
}

const processImages = flow<{ images: Image[] }>("image-batch")
  .step("validate", ({ input }) =>
    Pipeline.succeed(input.images.filter((img) => img.url.startsWith("https://"))),
  )
  .mapOver("resize", { array: "validate", concurrency: 10 }, (image) =>
    Pipeline.fn(async () => {
      const resized = await resizeImage(image.url, { width: 800 });
      return { id: image.id, resizedUrl: resized.url };
    }).retry(2),
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
