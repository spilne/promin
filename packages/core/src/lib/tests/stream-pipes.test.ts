import { describe, it, expect } from "bun:test";
import { StreamPipeline } from "../stream-pipeline.ts";
import {
  utf8Decode,
  lines,
  csv,
  tsv,
  ssv,
  fixedWidth,
  regex,
  xml,
  jsonl,
  jsonlAs,
  parseAs,
  parseAsLenient,
  binaryDecode,
  lengthPrefixed,
  base64Encode,
  base64Decode,
} from "../stream-pipes.ts";
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
// tsv — tab-separated values
// ---------------------------------------------------------------------------

describe("tsv — tab-separated values", () => {
  it("parses TSV with headers", async () => {
    const result = await StreamPipeline.fromIterable(["name\tage", "Alice\t30", "Bob\t25"])
      .through(tsv())
      .collect();

    expect(result).toEqual([
      { name: "Alice", age: "30" },
      { name: "Bob", age: "25" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// ssv — space-separated values
// ---------------------------------------------------------------------------

describe("ssv — space-separated values", () => {
  it("splits by whitespace (multiple spaces collapsed)", async () => {
    const result = await StreamPipeline.fromIterable([
      "192.168.1.1   GET  /api/users  200",
      "10.0.0.1      POST /api/login  401",
    ])
      .through(ssv())
      .collect();

    expect(result).toEqual([
      ["192.168.1.1", "GET", "/api/users", "200"],
      ["10.0.0.1", "POST", "/api/login", "401"],
    ]);
  });
});

// ---------------------------------------------------------------------------
// fixedWidth — positional column parsing
// ---------------------------------------------------------------------------

describe("fixedWidth — positional columns", () => {
  it("extracts fields by position", async () => {
    const columns = [
      { name: "id", start: 0, end: 5 },
      { name: "name", start: 5, end: 20 },
      { name: "amount", start: 20, end: 30 },
    ];

    const result = await StreamPipeline.fromIterable([
      "00001Alice              100.50    ",
      "00002Bob                200.00    ",
    ])
      .through(fixedWidth(columns))
      .collect();

    expect(result[0]!.id).toBe("00001");
    expect(result[0]!.name).toBe("Alice");
    expect(result[0]!.amount).toBe("100.50");
    expect(result[1]!.id).toBe("00002");
  });
});

// ---------------------------------------------------------------------------
// regex — log parsing with named groups
// ---------------------------------------------------------------------------

describe("regex — named capture group parsing", () => {
  it("parses Apache-style log lines", async () => {
    const apacheLog = regex(/^(?<ip>\S+) \S+ \S+ \[(?<date>[^\]]+)\] "(?<method>\S+) (?<path>\S+)/);

    const result = await StreamPipeline.fromIterable([
      '127.0.0.1 - - [04/Apr/2026:10:00:00] "GET /api/users HTTP/1.1" 200',
      '10.0.0.1 - - [04/Apr/2026:10:01:00] "POST /api/login HTTP/1.1" 401',
    ])
      .through(apacheLog)
      .collect();

    expect(result[0]!.ip).toBe("127.0.0.1");
    expect(result[0]!.method).toBe("GET");
    expect(result[0]!.path).toBe("/api/users");
    expect(result[1]!.method).toBe("POST");
  });

  it("skips non-matching lines", async () => {
    const result = await StreamPipeline.fromIterable([
      "valid: key=value",
      "invalid line",
      "valid: key=other",
    ])
      .through(regex(/^valid: (?<key>\w+)=(?<value>\w+)/))
      .collect();

    expect(result).toEqual([
      { key: "key", value: "value" },
      { key: "key", value: "other" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// xml — SAX-style event stream
// ---------------------------------------------------------------------------

describe("xml — SAX-style event parsing", () => {
  it("parses open, close, and text events", async () => {
    const result = await StreamPipeline.fromIterable([
      '<root><item id="1">Hello</item><item id="2">World</item></root>',
    ])
      .through(xml())
      .collect();

    const opens = result.filter((e) => e.type === "open");
    const texts = result.filter((e) => e.type === "text");
    const closes = result.filter((e) => e.type === "close");

    expect(opens.map((e) => e.tag)).toEqual(["root", "item", "item"]);
    expect(texts.map((e) => e.text)).toEqual(["Hello", "World"]);
    expect(closes.map((e) => e.tag)).toEqual(["item", "item", "root"]);
    expect(opens[1]!.attributes).toEqual({ id: "1" });
  });

  it("handles self-closing tags", async () => {
    const result = await StreamPipeline.fromIterable(['<items><item id="1"/></items>'])
      .through(xml())
      .collect();

    const selfClose = result.find((e) => e.type === "selfClose");
    expect(selfClose?.tag).toBe("item");
    expect(selfClose?.attributes).toEqual({ id: "1" });
  });
});

// ---------------------------------------------------------------------------
// binaryDecode — custom binary format
// ---------------------------------------------------------------------------

describe("binaryDecode — custom binary decoder", () => {
  it("decodes binary chunks with custom function", async () => {
    const chunks = [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])];

    const result = await StreamPipeline.fromIterable(chunks)
      .through(binaryDecode((buf) => Array.from(buf)))
      .collect();

    expect(result).toEqual([
      [1, 2, 3],
      [4, 5, 6],
    ]);
  });
});

// ---------------------------------------------------------------------------
// lengthPrefixed — framed binary messages
// ---------------------------------------------------------------------------

describe("lengthPrefixed — length-prefixed binary messages", () => {
  it("decodes length-prefixed messages", async () => {
    // Encode two messages with 4-byte big-endian length prefix
    const msg1 = new TextEncoder().encode('{"id":1}');
    const msg2 = new TextEncoder().encode('{"id":2}');

    const frame = (msg: Uint8Array) => {
      const buf = new Uint8Array(4 + msg.length);
      new DataView(buf.buffer).setUint32(0, msg.length, false);
      buf.set(msg, 4);
      return buf;
    };

    // Concatenate both frames into one chunk (simulates network)
    const combined = new Uint8Array(frame(msg1).length + frame(msg2).length);
    combined.set(frame(msg1));
    combined.set(frame(msg2), frame(msg1).length);

    const result = await StreamPipeline.fromIterable([combined])
      .through(lengthPrefixed((buf) => JSON.parse(new TextDecoder().decode(buf))))
      .collect();

    expect(result).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("handles messages split across chunks", async () => {
    const msg = new TextEncoder().encode("hello");
    const buf = new Uint8Array(4 + msg.length);
    new DataView(buf.buffer).setUint32(0, msg.length, false);
    buf.set(msg, 4);

    // Split into two chunks mid-message
    const chunk1 = buf.slice(0, 3); // partial header
    const chunk2 = buf.slice(3); // rest of header + body

    const result = await StreamPipeline.fromIterable([chunk1, chunk2])
      .through(lengthPrefixed((b) => new TextDecoder().decode(b)))
      .collect();

    expect(result).toEqual(["hello"]);
  });
});

// ---------------------------------------------------------------------------
// base64 encode / decode
// ---------------------------------------------------------------------------

describe("base64Encode / base64Decode — round-trip", () => {
  it("encodes binary to base64 strings", async () => {
    const data = new TextEncoder().encode("Hello World");
    const result = await StreamPipeline.fromIterable([data]).through(base64Encode()).collect();

    expect(result[0]).toBe(Buffer.from("Hello World").toString("base64"));
  });

  it("decodes base64 strings to binary", async () => {
    const b64 = Buffer.from("Hello World").toString("base64");
    const result = await StreamPipeline.fromIterable([b64]).through(base64Decode()).collect();

    expect(new TextDecoder().decode(result[0])).toBe("Hello World");
  });

  it("round-trips: encode → decode preserves data", async () => {
    const original = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a]); // PNG header
    const result = await StreamPipeline.fromIterable([original])
      .through(base64Encode())
      .through(base64Decode())
      .collect();

    expect(result[0]).toEqual(original);
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
