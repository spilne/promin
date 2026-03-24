/**
 * File downloads and binary streaming
 *
 * getResponse() returns an HttpResponse<T> where the body type depends
 * on the decoder. The default decoder returns a ReadableStream for
 * zero-copy streaming. Use arrayBufferDecoder, blobDecoder, or custom
 * decoders for other use cases.
 *
 * Key patterns:
 *   1. Stream download to disk (no memory buffering)
 *   2. Stream binary between services (restream)
 *   3. Download to memory, transform, re-upload
 *   4. Check content-length before downloading
 *   5. Custom decoder (protobuf, msgpack, etc.)
 *   6. Parallel downloads with progress
 */

import {
  DefaultHttpClient,
  HttpPipeline,
  arrayBufferDecoder,
  blobDecoder,
  textDecoder,
  type ResponseDecoder,
  type HttpResponse,
} from "@promin/http";
import { z } from "zod";

const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
});

const UploadResultSchema = z.object({ id: z.string(), url: z.string() });

// ---------------------------------------------------------------------------
// Pattern 1: Stream download to disk — no memory buffering
//
// Business flow: A nightly job downloads a large CSV export from a
// data provider and saves it locally for batch processing. The file
// is too large to fit in memory, so we stream directly to disk.
// ---------------------------------------------------------------------------

async function streamToDisk() {
  const { body: stream, contentLength } = await client
    .getResponse("/exports/daily-transactions.csv")
    .runPromise();

  console.log(`Downloading ${contentLength ?? "unknown"} bytes...`);

  // Bun.write accepts ReadableStream directly — zero-copy to disk
  await Bun.write("/tmp/transactions.csv", stream);
}

// ---------------------------------------------------------------------------
// Pattern 1b: Stream CSV → parse rows → DataFrame
//
// Business flow: Download a large CSV from an analytics API, parse it
// row-by-row as a stream (no full file in memory), then load into a
// DataFrame for aggregation and analysis. The CSV is never fully
// buffered — rows flow through the pipeline as they arrive.
// ---------------------------------------------------------------------------

async function csvToDataFrame() {
  const { body: stream } = await client
    .getResponse("/exports/daily-transactions.csv")
    .runPromise();

  // Parse CSV stream into typed rows using StreamPipeline
  const { StreamPipeline, DataFrame } = await import("@promin/core");

  // Convert binary ReadableStream → text lines → parsed CSV rows
  const rows = await StreamPipeline.fromAsyncIterable(
    parseCSVStream(stream),
    (err) => ({ _tag: "CSVParseError" as const, cause: err }),
  )
    // Skip header (already handled by parser)
    // Filter out invalid rows as they stream through
    .filter((row) => row.amount > 0)
    // Collect into array for DataFrame
    .collect();

  // Build DataFrame from parsed rows — now we can query, aggregate, join
  const df = DataFrame.fromArray(rows);

  // Aggregate: revenue per region
  const byRegion = await df
    .groupBy(["region"])
    .agg({ totalRevenue: { column: "amount", fn: "sum" } })
    .execute();

  console.log("Revenue by region:", byRegion);

  // Filter high-value transactions
  const highValue = await df
    .filter((row) => row.amount > 10_000)
    .sort("amount", "desc")
    .limit(10)
    .execute();

  console.log("Top 10 high-value transactions:", highValue);

  // Profile the dataset — column stats, null rates, distributions
  const profile = await df.profile();
  console.log("Dataset profile:", profile);
}

// ---------------------------------------------------------------------------
// Pattern 1c: Stream CSV → process in StreamPipeline → sink to DB
//
// Business flow: A daily ETL job downloads transactions, enriches each
// row with geo-IP data, and inserts into a database — all streaming,
// no full file materialization. Backpressure ensures we don't overwhelm
// the database with inserts.
// ---------------------------------------------------------------------------

async function csvStreamToDb() {
  const { body: stream } = await client
    .getResponse("/exports/daily-transactions.csv")
    .runPromise();

  const { StreamPipeline } = await import("@promin/core");

  let inserted = 0;

  await StreamPipeline.fromAsyncIterable(
    parseCSVStream(stream),
    (err) => ({ _tag: "CSVParseError" as const, cause: err }),
  )
    // Enrich with geo-IP lookup (parallel, bounded concurrency)
    .parAsyncMap(10, async (row) => ({
      ...row,
      country: await geoIpLookup(row.ip),
    }))
    // Batch inserts for efficiency (100 rows or 1 second)
    .groupWithin(100, 1_000)
    // Insert batch into database
    .mapAsync(async (batch) => {
      await insertBatch(batch);
      inserted += batch.length;
      if (inserted % 1000 === 0) console.log(`Inserted ${inserted} rows...`);
      return batch.length;
    })
    .drain();

  console.log(`ETL complete: ${inserted} rows inserted`);
}

/** Parse a ReadableStream<Uint8Array> as CSV rows (async generator). */
async function* parseCSVStream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<{ date: string; region: string; amount: number; ip: string }> {
  const decoder = new TextDecoder();
  const reader = stream.getReader();
  let buffer = "";
  let headers: string[] = [];
  let isFirst = true;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop()!; // keep incomplete last line

    for (const line of lines) {
      if (!line.trim()) continue;
      const values = line.split(",").map((v) => v.trim());

      if (isFirst) {
        headers = values;
        isFirst = false;
        continue;
      }

      const row: Record<string, string> = {};
      headers.forEach((h, i) => (row[h] = values[i] ?? ""));

      yield {
        date: row.date ?? "",
        region: row.region ?? "",
        amount: Number(row.amount ?? 0),
        ip: row.ip ?? "",
      };
    }
  }

  // Flush remaining buffer
  if (buffer.trim() && headers.length > 0) {
    const values = buffer.split(",").map((v) => v.trim());
    const row: Record<string, string> = {};
    headers.forEach((h, i) => (row[h] = values[i] ?? ""));
    yield {
      date: row.date ?? "",
      region: row.region ?? "",
      amount: Number(row.amount ?? 0),
      ip: row.ip ?? "",
    };
  }
}

async function geoIpLookup(_ip: string): Promise<string> {
  return "US";
}

async function insertBatch(_rows: unknown[]): Promise<void> {}

// ---------------------------------------------------------------------------
// Pattern 2: Restream binary between services — proxy pattern
//
// Business flow: A user uploads a profile photo. Our API downloads it
// from the user's OAuth provider and re-uploads to our CDN without
// ever buffering the full image in memory.
// ---------------------------------------------------------------------------

async function restreamBetweenServices() {
  const cdn = new DefaultHttpClient({ baseUrl: "https://cdn.example.com" });

  // Download from OAuth provider as stream
  const { body: stream, contentType } = await client
    .getResponse("/oauth/google/avatar/user-123")
    .runPromise();

  // Re-upload to CDN — stream passes through without buffering
  const result = await cdn
    .post("/images/upload", UploadResultSchema, {
      body: stream,
      headers: { "Content-Type": contentType ?? "image/jpeg" },
    })
    .runPromise();

  console.log(`Uploaded to CDN: ${result.url}`);
}

// ---------------------------------------------------------------------------
// Pattern 3: Download to memory → transform → re-upload
//
// Business flow: Download a product image, resize it in memory using
// a Bun-native image library, then upload the thumbnail. We use
// arrayBufferDecoder since we need the full file for transformation.
// ---------------------------------------------------------------------------

async function downloadTransformUpload() {
  // Download full image into memory
  const { body: buffer } = await client
    .getResponse("/products/123/image.png", { decoder: arrayBufferDecoder })
    .runPromise();

  // Transform (resize, watermark, etc.)
  const thumbnail = await resizeImage(buffer, { width: 200, height: 200 });

  // Re-upload as Blob via multipart form
  const result = await client
    .postMultipart("/products/123/thumbnail", UploadResultSchema, {
      file: new File([thumbnail], "thumb.png", { type: "image/png" }),
    })
    .runPromise();

  console.log(`Thumbnail uploaded: ${result.url}`);
}

// ---------------------------------------------------------------------------
// Pattern 4: Check content-length before downloading
//
// Business flow: Users can download attachments from a ticketing system.
// Before downloading, we check the file size to prevent abuse —
// reject files over 100MB without wasting bandwidth.
// ---------------------------------------------------------------------------

async function guardedDownload() {
  const MAX_SIZE = 100 * 1024 * 1024; // 100MB

  const { body } = await client
    .getResponse("/tickets/456/attachment", { decoder: arrayBufferDecoder })
    .map((response) => {
      if (response.contentLength && response.contentLength > MAX_SIZE) {
        throw new Error(`File too large: ${response.contentLength} bytes (max ${MAX_SIZE})`);
      }
      return response;
    })
    .runPromise();

  await Bun.write("/tmp/attachment.pdf", body);
}

// ---------------------------------------------------------------------------
// Pattern 5: Download as Blob for FormData re-upload
//
// Business flow: Migrate files between storage providers. Download from
// legacy S3, re-upload to new provider using standard multipart form.
// Blob preserves the binary data and integrates with FormData natively.
// ---------------------------------------------------------------------------

async function migrateFiles() {
  const legacy = new DefaultHttpClient({ baseUrl: "https://legacy-s3.example.com" });
  const modern = new DefaultHttpClient({ baseUrl: "https://storage.example.com" });

  const files = ["doc1.pdf", "doc2.pdf", "image.png"];

  for (const filename of files) {
    const { body: blob } = await legacy
      .getResponse(`/bucket/files/${filename}`, { decoder: blobDecoder })
      .runPromise();

    await modern
      .postMultipart(`/v2/files`, UploadResultSchema, {
        file: new File([blob], filename),
        fields: { bucket: "migrated" },
      })
      .runPromise();

    console.log(`Migrated: ${filename}`);
  }
}

// ---------------------------------------------------------------------------
// Pattern 6: Custom decoder — protobuf messages
//
// Business flow: Internal gRPC-web service returns protobuf-encoded
// responses. We use a custom decoder to deserialize directly.
// ---------------------------------------------------------------------------

async function protobufDownload() {
  // Custom decoder for any binary format
  const protoDecoder: ResponseDecoder<{ users: { id: number; name: string }[] }> = async (
    response,
  ) => {
    const buffer = await response.arrayBuffer();
    // In real code: return MyProtoMessage.decode(new Uint8Array(buffer));
    return JSON.parse(new TextDecoder().decode(buffer));
  };

  const { body: message } = await client
    .getResponse("/internal/users.pb", { decoder: protoDecoder })
    .runPromise();

  console.log(`Got ${message.users.length} users`);
}

// ---------------------------------------------------------------------------
// Pattern 7: Parallel downloads with retry
//
// Business flow: Download 10 report segments in parallel, retry on
// failure, collect all into a single merged file.
// ---------------------------------------------------------------------------

async function parallelDownloads() {
  const segments = Array.from({ length: 10 }, (_, i) => `/reports/monthly/segment-${i}.csv`);

  const results = await HttpPipeline.all(
    ...segments.map((path) =>
      client
        .getResponse(path, { decoder: textDecoder })
        .map((r) => r.body)
        .retry({ maxRetries: 3, baseDelayMs: 500 }),
    ),
  ).runPromise();

  // Merge all segments
  const merged = (results as string[]).join("\n");
  await Bun.write("/tmp/monthly-report.csv", merged);
  console.log(`Merged ${results.length} segments`);
}

// ---------------------------------------------------------------------------
// Pattern 8: Stream download with progress tracking
//
// Business flow: Large file download with progress callback for UI.
// We wrap the ReadableStream with a TransformStream that tracks bytes.
// ---------------------------------------------------------------------------

async function downloadWithProgress() {
  const { body: stream, contentLength } = await client
    .getResponse("/exports/full-backup.tar.gz")
    .runPromise();

  let bytesRead = 0;

  const progress = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      bytesRead += chunk.length;
      if (contentLength) {
        const pct = ((bytesRead / contentLength) * 100).toFixed(1);
        console.log(`Progress: ${pct}% (${bytesRead}/${contentLength})`);
      }
      controller.enqueue(chunk);
    },
  });

  const tracked = stream.pipeThrough(progress);
  await Bun.write("/tmp/backup.tar.gz", tracked);
}

// ---------------------------------------------------------------------------
// Stubs for compilation
// ---------------------------------------------------------------------------

async function resizeImage(
  _buffer: ArrayBuffer,
  _opts: { width: number; height: number },
): Promise<ArrayBuffer> {
  return _buffer;
}

export {
  streamToDisk,
  csvToDataFrame,
  csvStreamToDb,
  restreamBetweenServices,
  downloadTransformUpload,
  guardedDownload,
  migrateFiles,
  protobufDownload,
  parallelDownloads,
  downloadWithProgress,
};
