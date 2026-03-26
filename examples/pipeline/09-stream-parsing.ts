/**
 * Stream parsing — fs2-style pipe composition
 *
 * Reusable pipes compose via .through() to build streaming parsers.
 * Data flows through the pipeline without full buffering — each stage
 * processes one chunk/line/row at a time.
 *
 * Key patterns:
 *   1. HTTP → CSV → typed rows → DataFrame
 *   2. HTTP → JSONL → typed events → filter → sink
 *   3. HTTP → XML → SAX events → extract fields
 *   4. HTTP → regex log parser → structured records
 *   5. HTTP → binary → length-prefixed protobuf → typed messages
 *   6. HTTP → CSV → enrich → batch insert (full ETL)
 *   7. Base64 encode/decode in a pipeline
 *   8. Fixed-width mainframe data parsing
 */

import {
  DefaultHttpClient,
  textDecoder,
  arrayBufferDecoder,
} from "@promin/http";
import {
  StreamPipeline,
  DataFrame,
  utf8Decode,
  lines,
  csv,
  tsv,
  ssv,
  jsonl,
  jsonlAs,
  xml,
  regex,
  fixedWidth,
  parseAs,
  parseAsLenient,
  binaryDecode,
  lengthPrefixed,
  base64Encode,
  base64Decode,
  type ResponseDecoder,
} from "@promin/core";
import { z } from "zod";

const client = new DefaultHttpClient({ baseUrl: "https://api.example.com" });

// ---------------------------------------------------------------------------
// Pattern 1: HTTP → CSV → typed rows → DataFrame analytics
//
// Business flow: Download a daily sales export from the analytics API,
// parse it as a stream, validate with Zod, load into DataFrame for
// groupBy/aggregation. The CSV is never fully buffered — rows flow
// through one at a time until they reach the DataFrame.
// ---------------------------------------------------------------------------

async function csvToDataFrame() {
  const SaleSchema = z.object({
    date: z.string(),
    region: z.string(),
    amount: z.coerce.number(),
    product: z.string(),
  });

  const rows = await client
    .getResponse("/exports/sales.csv", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise()
    .then((text) =>
      StreamPipeline.fromIterable([text])
        .through(lines())
        .through(csv())
        .through(parseAs(SaleSchema))
        .filter((sale) => sale.amount > 0)
        .collect(),
    );

  // Analyze with DataFrame
  const df = DataFrame.fromArray(rows);

  const byRegion = await df
    .groupBy("region")
    .agg({ amount: "sum" })
    .collect();

  const topProducts = await df
    .sort("amount", "desc")
    .limit(10)
    .collect();

  console.log("Revenue by region:", byRegion);
  console.log("Top 10 products:", topProducts);
}

// ---------------------------------------------------------------------------
// Pattern 2: HTTP → JSONL → typed events → filter → sink
//
// Business flow: Consume a real-time event feed (NDJSON) from a
// monitoring API. Parse each line as JSON, validate against schema,
// filter for error events, and forward to an alerting system.
// ---------------------------------------------------------------------------

async function jsonlEventProcessing() {
  const EventSchema = z.object({
    type: z.enum(["info", "warn", "error"]),
    service: z.string(),
    message: z.string(),
    ts: z.number(),
  });

  const errors = await client
    .getResponse("/events/stream", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise()
    .then((text) =>
      StreamPipeline.fromIterable([text])
        .through(lines())
        .through(jsonlAs(EventSchema))
        .filter((e) => e.type === "error")
        .collect(),
    );

  for (const error of errors) {
    await sendAlert(error.service, error.message);
  }
}

// ---------------------------------------------------------------------------
// Pattern 3: HTTP → XML → SAX events → extract RSS items
//
// Business flow: Fetch an RSS feed, parse as XML stream, extract
// article titles and links without building a full DOM tree.
// ---------------------------------------------------------------------------

async function rssParser() {
  const feed = await client
    .getResponse("/blog/feed.xml", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise();

  type Article = { title: string; link: string };
  const articles: Article[] = [];
  let current: Partial<Article> = {};
  let inItem = false;
  let captureField: "title" | "link" | null = null;

  await StreamPipeline.fromIterable([feed])
    .through(xml())
    .forEach((event) => {
      if (event.type === "open" && event.tag === "item") inItem = true;
      if (event.type === "close" && event.tag === "item") {
        if (current.title && current.link) articles.push(current as Article);
        current = {};
        inItem = false;
      }
      if (inItem && event.type === "open" && (event.tag === "title" || event.tag === "link")) {
        captureField = event.tag;
      }
      if (captureField && event.type === "text") {
        current[captureField] = event.text!;
        captureField = null;
      }
    });

  console.log(`Found ${articles.length} articles`);
  for (const a of articles.slice(0, 5)) {
    console.log(`  ${a.title}: ${a.link}`);
  }
}

// ---------------------------------------------------------------------------
// Pattern 4: HTTP → regex log parser → structured records
//
// Business flow: Download nginx access logs, parse each line with a
// regex to extract IP, method, path, status, and response time.
// Filter for slow requests (>1s) and group by path.
// ---------------------------------------------------------------------------

async function logAnalytics() {
  const nginxPattern = regex(
    /^(?<ip>\S+) - - \[(?<date>[^\]]+)\] "(?<method>\S+) (?<path>\S+) \S+" (?<status>\d+) \d+ "-" ".*" (?<responseTime>[\d.]+)/,
  );

  const slowRequests = await client
    .getResponse("/logs/access.log", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise()
    .then((text) =>
      StreamPipeline.fromIterable([text])
        .through(lines())
        .through(nginxPattern)
        .filter((r) => Number(r.responseTime) > 1.0) // > 1 second
        .collect(),
    );

  const df = DataFrame.fromArray(
    slowRequests.map((r) => ({
      path: r.path,
      status: Number(r.status),
      responseTime: Number(r.responseTime),
    })),
  );

  const byPath = await df
    .groupBy("path")
    .agg({ responseTime: "avg" })
    .collect();

  console.log("Slow endpoints:", byPath);
}

// ---------------------------------------------------------------------------
// Pattern 5: HTTP → binary → length-prefixed protobuf
//
// Business flow: Internal service returns a stream of length-prefixed
// protobuf messages (4-byte big-endian length + message bytes).
// Decode each message without buffering the full response.
// ---------------------------------------------------------------------------

async function protobufStream() {
  interface UserEvent {
    userId: string;
    action: string;
    ts: number;
  }

  // Custom decoder for your protobuf library
  const decodeProto = (buf: Uint8Array): UserEvent => {
    // In reality: return UserEventProto.decode(buf);
    return JSON.parse(new TextDecoder().decode(buf));
  };

  const { body: stream } = await client
    .getResponse("/internal/events.pb")
    .runPromise();

  const events = await StreamPipeline.fromAsyncIterable(
    streamToChunks(stream),
    (err) => ({ _tag: "StreamError" as const, cause: err }),
  )
    .through(lengthPrefixed(decodeProto))
    .filter((e) => e.action === "purchase")
    .collect();

  console.log(`${events.length} purchase events`);
}

// ---------------------------------------------------------------------------
// Pattern 6: HTTP → CSV → enrich → batch insert (full ETL)
//
// Business flow: Download a CSV of customer records, enrich each row
// with external data (geo-IP, risk score), batch them into groups of
// 100, and insert into the database. Zero full-file buffering —
// backpressure from the DB insert controls download speed.
// ---------------------------------------------------------------------------

async function csvEtlPipeline() {
  const CustomerSchema = z.object({
    email: z.string(),
    name: z.string(),
    ip: z.string(),
    signupDate: z.string(),
  });

  let total = 0;

  await client
    .getResponse("/exports/customers.csv", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise()
    .then((text) =>
      StreamPipeline.fromIterable([text])
        .through(lines())
        .through(csv())
        .through(parseAs(CustomerSchema))
        // Enrich with geo-IP (bounded concurrency)
        .parAsyncMap(10, async (customer) => ({
          ...customer,
          country: await geoIpLookup(customer.ip),
          riskScore: await computeRiskScore(customer.email),
        }))
        // Batch for efficient DB inserts
        .groupWithin(100, 1_000) // 100 rows or 1 second
        .mapAsync(async (batch) => {
          await insertBatch(batch);
          total += batch.length;
          if (total % 1000 === 0) console.log(`Inserted ${total} rows...`);
        })
        .drain(),
    );

  console.log(`ETL complete: ${total} customers imported`);
}

// ---------------------------------------------------------------------------
// Pattern 7: Base64 encode/decode in a pipeline
//
// Business flow: Download binary data, base64-encode it for transport
// in a JSON payload, then decode on the other side.
// ---------------------------------------------------------------------------

async function base64Pipeline() {
  // Encode: binary → base64
  const { body } = await client
    .getResponse("/files/image.png", { decoder: arrayBufferDecoder })
    .runPromise();

  const encoded = await StreamPipeline.fromIterable([new Uint8Array(body)])
    .through(base64Encode())
    .collect();

  const payload = { filename: "image.png", data: encoded[0] };
  console.log("Base64 payload size:", payload.data!.length);

  // Decode: base64 → binary
  const decoded = await StreamPipeline.fromIterable([payload.data!])
    .through(base64Decode())
    .collect();

  console.log("Decoded size:", decoded[0]!.length, "bytes");
}

// ---------------------------------------------------------------------------
// Pattern 8: Fixed-width mainframe data
//
// Business flow: A bank exports transaction data in fixed-width COBOL
// format. Each field occupies a fixed number of characters. Parse
// positionally, validate, and convert to modern JSON format.
// ---------------------------------------------------------------------------

async function fixedWidthParsing() {
  const columns = [
    { name: "accountId", start: 0, end: 10 },
    { name: "transDate", start: 10, end: 18 },
    { name: "amount", start: 18, end: 30 },
    { name: "description", start: 30, end: 60 },
    { name: "code", start: 60, end: 64 },
  ];

  const TransactionSchema = z.object({
    accountId: z.string(),
    transDate: z.string(),
    amount: z.coerce.number(),
    description: z.string(),
    code: z.string(),
  });

  const transactions = await client
    .getResponse("/mainframe/daily-extract.dat", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise()
    .then((text) =>
      StreamPipeline.fromIterable([text])
        .through(lines())
        .through(fixedWidth(columns))
        .through(parseAs(TransactionSchema))
        .filter((t) => t.amount !== 0) // skip zero-amount records
        .collect(),
    );

  console.log(`Parsed ${transactions.length} transactions from mainframe`);
}

// ---------------------------------------------------------------------------
// Pattern 9: TSV + lenient parsing with error reporting
//
// Business flow: User uploads a TSV spreadsheet export. Parse it
// leniently — report errors per row without dropping the entire file.
// Good rows proceed to processing, bad rows go to an error report.
// ---------------------------------------------------------------------------

async function lenientTsvParsing() {
  const RowSchema = z.object({
    name: z.string().min(1),
    email: z.string().email(),
    age: z.coerce.number().positive(),
  });

  const text = await client
    .getResponse("/uploads/users.tsv", { decoder: textDecoder })
    .map((r) => r.body)
    .runPromise();

  const results = await StreamPipeline.fromIterable([text])
    .through(lines())
    .through(tsv())
    .through(parseAsLenient(RowSchema))
    .collect();

  const good = results.filter((r) => r.data !== null).map((r) => r.data!);
  const bad = results.filter((r) => r.error !== null);

  console.log(`Valid rows: ${good.length}, Invalid rows: ${bad.length}`);
  for (const err of bad.slice(0, 5)) {
    console.log("  Error:", err.error);
  }
}

// ---------------------------------------------------------------------------
// Pattern 10: SSV (space-separated) access log → DataFrame
//
// Business flow: Parse whitespace-separated server logs into
// structured records for analysis.
// ---------------------------------------------------------------------------

async function ssvLogToDataFrame() {
  // Space-separated log: timestamp level service message
  const logLines = [
    "2025-01-01T00:00:00Z INFO  auth     User login successful",
    "2025-01-01T00:00:01Z ERROR payment  Payment failed: timeout",
    "2025-01-01T00:00:02Z WARN  auth     Rate limit approaching",
    "2025-01-01T00:00:03Z ERROR auth     Authentication failed",
  ];

  const records = await StreamPipeline.fromIterable(logLines)
    .through(ssv())
    .map(([ts, level, service, ...messageParts]) => ({
      timestamp: ts!,
      level: level!,
      service: service!,
      message: messageParts.join(" "),
    }))
    .collect();

  const df = DataFrame.fromArray(records);
  const errors = await df.filter((r) => r.level === "ERROR").collect();
  console.log(`${errors.length} error events`);
}

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

async function sendAlert(_service: string, _message: string): Promise<void> {}
async function geoIpLookup(_ip: string): Promise<string> { return "US"; }
async function computeRiskScore(_email: string): Promise<number> { return 0.1; }
async function insertBatch(_rows: unknown[]): Promise<void> {}

async function* streamToChunks(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<Uint8Array> {
  const reader = stream.getReader();
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    yield value;
  }
}

export {
  csvToDataFrame,
  jsonlEventProcessing,
  rssParser,
  logAnalytics,
  protobufStream,
  csvEtlPipeline,
  base64Pipeline,
  fixedWidthParsing,
  lenientTsvParsing,
  ssvLogToDataFrame,
};
