# Stream Pipes

Reusable `.through()` transformations for `StreamPipeline`. Each pipe is a function `StreamPipeline<A> -> StreamPipeline<B>` that you compose by chaining:

```ts
stream.through(utf8Decode()).through(lines()).through(csv()).through(parseAs(MySchema));
```

All imports are from `@promin/core`.

## Text

### utf8Decode

Decode binary `Uint8Array` chunks into text strings.

```ts
import { utf8Decode } from "@promin/core";

const textStream = binaryStream.through(utf8Decode());
```

---

### lines

Split a text stream into individual lines. Handles `\n`, `\r\n`, and `\r`. Correctly joins lines split across chunks.

```ts
import { lines } from "@promin/core";

const lineStream = textStream.through(lines());
```

---

## Structured

### csv

Parse CSV lines into objects (header mode, default) or string arrays. Handles quoted fields with commas and escaped quotes.

```ts
import { csv } from "@promin/core";

// Header mode -- yields Record<string, string>
const rows = textStream.through(lines()).through(csv());

// No header -- yields string[]
const arrays = textStream.through(lines()).through(csv({ header: false }));

// Custom separator
const piped = textStream.through(lines()).through(csv({ separator: "|" }));
```

**Options:** `header?` (default `true`), `separator?` (default `","`), `quote?` (default `"`)

---

### tsv

Parse tab-separated values. Shorthand for `csv({ separator: "\t" })`.

```ts
import { tsv } from "@promin/core";

const rows = textStream.through(lines()).through(tsv());
```

---

### ssv

Parse space-separated values (multiple spaces collapsed). Common for log files.

```ts
import { ssv } from "@promin/core";

const fields = textStream.through(lines()).through(ssv({ header: false }));
```

**Options:** `header?` (default uses no header, yields `string[]`)

---

### fixedWidth

Parse fixed-width positional columns. Common for legacy mainframe data and COBOL exports.

```ts
import { fixedWidth } from "@promin/core";

const columns = [
  { name: "id", start: 0, end: 5 },
  { name: "name", start: 5, end: 25 },
  { name: "amount", start: 25, end: 35 },
];
const rows = textStream.through(lines()).through(fixedWidth(columns));
// yields: { id: "00001", name: "Alice", amount: "100.50" }
```

**Column config:** `name`, `start`, `end`, `trim?` (default `true`)

---

### xml

Parse XML text into SAX-style events. Lightweight -- no DOM tree in memory.

```ts
import { xml } from "@promin/core";

const items = textStream.through(xml()).filter((e) => e.type === "open" && e.tag === "item");
// yields: { type: "open", tag: "item", attributes: { id: "1" } }
```

**Event types:** `open`, `close`, `selfClose`, `text`

**Event fields:** `type`, `tag?`, `attributes?`, `text?`

---

## JSON

### jsonl

Parse newline-delimited JSON (JSONL/NDJSON). Invalid lines are silently skipped.

```ts
import { jsonl } from "@promin/core";

const objects = textStream.through(lines()).through(jsonl());
```

---

### jsonlAs

Parse JSONL with schema validation in one step. Combines `jsonl()` + `parseAs()`.

```ts
import { jsonlAs } from "@promin/core";

const events = textStream.through(lines()).through(jsonlAs(EventSchema));
```

---

### parseAs

Validate and coerce rows using a schema (Zod, etc.). Invalid rows are silently dropped.

```ts
import { parseAs } from "@promin/core";

const typed = textStream.through(lines()).through(csv()).through(parseAs(TransactionSchema));
```

---

### parseAsLenient

Like `parseAs`, but emits `{ data, error }` for every row -- no silent drops.

```ts
import { parseAsLenient } from "@promin/core";

const results = textStream
  .through(lines())
  .through(csv())
  .through(parseAsLenient(TransactionSchema))
  .tap(({ error }) => {
    if (error) console.warn(error);
  })
  .filterMap(({ data }) => data);
```

---

## Binary

### binaryDecode

Decode binary chunks using a custom decoder. Use for protobuf, msgpack, avro, or any binary format.

```ts
import { binaryDecode } from "@promin/core";

// Protobuf
stream.through(binaryDecode((buf) => MyMessage.decode(buf)));

// MessagePack
stream.through(binaryDecode((buf) => decode(buf)));
```

---

### lengthPrefixed

Decode length-prefixed binary messages (4-byte big-endian uint32 length prefix). Common for protobuf streaming and gRPC.

```ts
import { lengthPrefixed } from "@promin/core";

const messages = stream.through(lengthPrefixed((buf) => MyProto.decode(buf)));
```

---

### base64Encode

Encode binary chunks to base64 strings.

```ts
import { base64Encode } from "@promin/core";

const encoded = binaryStream.through(base64Encode());
```

---

### base64Decode

Decode base64 strings to binary chunks.

```ts
import { base64Decode } from "@promin/core";

const binary = base64Stream.through(base64Decode());
```

---

## Regex

### regex

Parse lines using a regex with named capture groups. Lines that don't match are skipped.

```ts
import { regex } from "@promin/core";

const apacheLog = regex(
  /^(?<ip>\S+) \S+ \S+ \[(?<date>[^\]]+)\] "(?<method>\S+) (?<path>\S+) \S+" (?<status>\d+) (?<size>\d+)/,
);
const parsed = textStream.through(lines()).through(apacheLog);
// yields: { ip: "1.2.3.4", date: "...", method: "GET", path: "/", status: "200", size: "1234" }
```
