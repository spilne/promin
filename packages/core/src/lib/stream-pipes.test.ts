import { describe, it, expect } from "bun:test";
import { StreamPipeline } from "./stream-pipeline.ts";
import { utf8Decode, lines, csv, jsonl, jsonlAs, parseAs, parseAsLenient } from "./stream-pipes.ts";
import { z } from "zod";

// ---------------------------------------------------------------------------
// utf8Decode
// ---------------------------------------------------------------------------

describe("utf8Decode — binary chunks to text", () => {
  it("decodes Uint8Array chunks to strings", async () => {
    const chunks = [new TextEncoder().encode("Hello "), new TextEncoder().encode("World")];

    const result = await StreamPipeline.fromIterable(chunks).through(utf8Decode()).collect();

    expect(result.join("")).toBe("Hello World");
  });
});

// ---------------------------------------------------------------------------
// lines
// ---------------------------------------------------------------------------

describe("lines — split text into lines", () => {
  it("splits text with \\n", async () => {
    const result = await StreamPipeline.fromIterable(["a\nb\nc"]).through(lines()).collect();

    expect(result).toEqual(["a", "b", "c"]);
  });

  it("handles lines split across chunks", async () => {
    const result = await StreamPipeline.fromIterable(["hel", "lo\nwor", "ld\n"])
      .through(lines())
      .collect();

    expect(result).toEqual(["hello", "world"]);
  });

  it("handles \\r\\n line endings", async () => {
    const result = await StreamPipeline.fromIterable(["a\r\nb\r\nc"]).through(lines()).collect();

    expect(result).toEqual(["a", "b", "c"]);
  });
});

// ---------------------------------------------------------------------------
// csv — parse CSV lines to objects or arrays
// ---------------------------------------------------------------------------

describe("csv — parse CSV to objects", () => {
  it("parses with header (default)", async () => {
    const result = await StreamPipeline.fromIterable(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
      .through(csv())
      .collect();

    expect(result).toEqual([
      { name: "Alice", age: "30", city: "NYC" },
      { name: "Bob", age: "25", city: "SF" },
    ]);
  });

  it("parses without header", async () => {
    const result = await StreamPipeline.fromIterable(["Alice,30,NYC", "Bob,25,SF"])
      .through(csv({ header: false }))
      .collect();

    expect(result).toEqual([
      ["Alice", "30", "NYC"],
      ["Bob", "25", "SF"],
    ]);
  });

  it("handles quoted fields with commas", async () => {
    const result = await StreamPipeline.fromIterable([
      "name,address",
      'Alice,"123 Main St, Apt 4"',
      'Bob,"456 Oak Ave"',
    ])
      .through(csv())
      .collect();

    expect(result[0]!.address).toBe("123 Main St, Apt 4");
    expect(result[1]!.address).toBe("456 Oak Ave");
  });

  it("handles escaped quotes", async () => {
    const result = await StreamPipeline.fromIterable(["name,note", 'Alice,"She said ""hello"""'])
      .through(csv())
      .collect();

    expect(result[0]!.note).toBe('She said "hello"');
  });

  it("supports custom separator", async () => {
    const result = await StreamPipeline.fromIterable(["name;age", "Alice;30"])
      .through(csv({ separator: ";" }))
      .collect();

    expect(result).toEqual([{ name: "Alice", age: "30" }]);
  });
});

// ---------------------------------------------------------------------------
// parseAs — typed schema validation
// ---------------------------------------------------------------------------

describe("parseAs — validate rows with Zod schema", () => {
  const UserSchema = z.object({
    name: z.string(),
    age: z.coerce.number(),
  });

  it("parses and coerces CSV rows to typed objects", async () => {
    const result = await StreamPipeline.fromIterable(["name,age", "Alice,30", "Bob,25"])
      .through(csv())
      .through(parseAs(UserSchema))
      .collect();

    expect(result).toEqual([
      { name: "Alice", age: 30 },
      { name: "Bob", age: 25 },
    ]);
  });

  it("silently drops invalid rows", async () => {
    const result = await StreamPipeline.fromIterable([
      "name,age",
      "Alice,30",
      "Bob,not-a-number", // invalid — age can't coerce
      "Charlie,40",
    ])
      .through(csv())
      .through(parseAs(z.object({ name: z.string(), age: z.coerce.number().int().positive() })))
      .collect();

    // Bob is dropped, Alice and Charlie pass
    expect(result).toHaveLength(2);
    expect(result.map((r) => r.name)).toEqual(["Alice", "Charlie"]);
  });
});

// ---------------------------------------------------------------------------
// parseAsLenient — validation with error reporting
// ---------------------------------------------------------------------------

describe("parseAsLenient — validate with error reporting", () => {
  it("emits data and error for each row", async () => {
    const Schema = z.object({ name: z.string(), age: z.coerce.number().positive() });

    const result = await StreamPipeline.fromIterable([
      "name,age",
      "Alice,30",
      "Bob,-5", // fails positive()
    ])
      .through(csv())
      .through(parseAsLenient(Schema))
      .collect();

    expect(result[0]!.data).toEqual({ name: "Alice", age: 30 });
    expect(result[0]!.error).toBeNull();
    expect(result[1]!.data).toBeNull();
    expect(result[1]!.error).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// jsonl — parse JSONL/NDJSON
// ---------------------------------------------------------------------------

describe("jsonl — parse newline-delimited JSON", () => {
  it("parses valid JSON lines", async () => {
    const result = await StreamPipeline.fromIterable([
      '{"id": 1, "name": "Alice"}',
      '{"id": 2, "name": "Bob"}',
    ])
      .through(jsonl())
      .collect();

    expect(result).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
  });

  it("skips invalid JSON lines", async () => {
    const result = await StreamPipeline.fromIterable(['{"id": 1}', "not json", '{"id": 2}', ""])
      .through(jsonl())
      .collect();

    expect(result).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("works with schema validation via jsonlAs", async () => {
    const EventSchema = z.object({ type: z.string(), ts: z.number() });

    const result = await StreamPipeline.fromIterable([
      '{"type": "click", "ts": 1000}',
      '{"type": "view", "ts": 2000}',
      '{"bad": "data"}', // fails schema
    ])
      .through(jsonlAs(EventSchema))
      .collect();

    expect(result).toEqual([
      { type: "click", ts: 1000 },
      { type: "view", ts: 2000 },
    ]);
  });

  it("full pipeline: binary → lines → jsonl → typed", async () => {
    const content = [
      '{"userId": "u1", "action": "login"}',
      '{"userId": "u2", "action": "purchase"}',
    ].join("\n");

    const bytes = new TextEncoder().encode(content);

    const result = await StreamPipeline.fromIterable([bytes])
      .through(utf8Decode())
      .through(lines())
      .through(jsonlAs(z.object({ userId: z.string(), action: z.string() })))
      .filter((e) => e.action === "purchase")
      .collect();

    expect(result).toEqual([{ userId: "u2", action: "purchase" }]);
  });
});

// ---------------------------------------------------------------------------
// Full pipeline: bytes → text → lines → csv → typed → filter → collect
// ---------------------------------------------------------------------------

describe("full pipeline — fs2-style composition", () => {
  it("binary stream → utf8 → lines → csv → typed → filter → collect", async () => {
    const csvContent = [
      "date,region,amount,status",
      "2025-01-01,US,100.50,completed",
      "2025-01-02,EU,200.00,pending",
      "2025-01-03,US,50.00,completed",
      "2025-01-04,AP,-10.00,refunded",
      "2025-01-05,EU,300.00,completed",
    ].join("\n");

    const TransactionSchema = z.object({
      date: z.string(),
      region: z.string(),
      amount: z.coerce.number(),
      status: z.string(),
    });

    const bytes = new TextEncoder().encode(csvContent);

    const result = await StreamPipeline.fromIterable([bytes])
      .through(utf8Decode())
      .through(lines())
      .through(csv())
      .through(parseAs(TransactionSchema))
      .filter((t) => t.status === "completed" && t.amount > 0)
      .map((t) => ({ region: t.region, amount: t.amount }))
      .collect();

    expect(result).toEqual([
      { region: "US", amount: 100.5 },
      { region: "US", amount: 50 },
      { region: "EU", amount: 300 },
    ]);
  });
});
