// ---------------------------------------------------------------------------
// Tiny JSON-Schema-native schema builder.
//
// Why this and not Zod / Valibot / ArkType: signals persist their schema to
// the journal as a snapshot, so the on-wire format has to be JSON Schema
// regardless. A Zod -> JSON Schema converter is one more dep + one more
// translation step + one more thing that can drift. Producing JSON Schema
// directly makes the snapshot trivially equal to the author's source of
// truth and keeps the @promin/workflow runtime dep-free.
//
// Scope: the common payload shapes — string / number / integer / boolean /
// null / enum / array / object / union — plus `.optional()` and
// `.describe()`. Not a Zod replacement: no transforms, refinements,
// parse/safeParse, default values. If authors need more, the adapter path
// (a separate @promin/workflow-zod package, planned) lets them drop in a
// Zod schema via `zodSchema(...)`.
//
// Type inference: each builder returns `Schema<T>`. Use `Infer<typeof S>`
// to derive the parsed payload type. `s.object` preserves optionality via
// `{ ... } & { ...? }`.
// ---------------------------------------------------------------------------

/** JSON Schema subset this builder produces. */
export type JsonSchema =
  | { type: "string"; description?: string; enum?: string[]; format?: string }
  | { type: "number"; description?: string; minimum?: number; maximum?: number }
  | { type: "integer"; description?: string; minimum?: number; maximum?: number }
  | { type: "boolean"; description?: string }
  | { type: "null"; description?: string }
  | { type: "array"; description?: string; items: JsonSchema }
  | {
      type: "object";
      description?: string;
      properties: Record<string, JsonSchema>;
      required?: string[];
      additionalProperties?: boolean;
    }
  | { anyOf: JsonSchema[]; description?: string }
  | { const: unknown; description?: string }
  /** Open / unknown — empty schema matches everything (JSON Schema convention). */
  | { description?: string };

export interface Schema<T = unknown> {
  /** The JSON Schema this builder produced — the snapshot persisted on suspend. */
  readonly jsonSchema: JsonSchema;
  /** Phantom — carries the parsed-payload type. Never read at runtime. */
  readonly _t?: T;
  /** Mark this schema as optional in an enclosing `s.object({...})`. */
  optional(): OptionalSchema<T>;
  /** Set the JSON Schema `description` field — also used as the form label. */
  describe(description: string): Schema<T>;
}

export interface OptionalSchema<T> extends Schema<T | undefined> {
  /** Discriminator the `s.object` builder uses to compute the required[] list. */
  readonly isOptional: true;
}

/** Extract the TypeScript type carried by a `Schema<T>`. */
export type Infer<S> = S extends Schema<infer T> ? T : never;

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

function makeSchema<T>(jsonSchema: JsonSchema, isOptional = false): Schema<T> {
  const base: Schema<T> & { isOptional?: true } = {
    jsonSchema,
    optional(): OptionalSchema<T> {
      return makeSchema<T | undefined>(jsonSchema, true) as OptionalSchema<T>;
    },
    describe(description: string): Schema<T> {
      // Spread+overwrite preserves any pre-existing fields on the JSON Schema
      // (e.g. enum, minimum) — only `description` changes. Preserves the
      // optional flag so `.optional().describe()` and `.describe().optional()`
      // produce the same shape (otherwise authors get a surprising "I called
      // .describe() and my field became required" footgun).
      return makeSchema<T>({ ...jsonSchema, description }, isOptional);
    },
  };
  if (isOptional) base.isOptional = true;
  return base;
}

/**
 * Helper: collapse `description?: string` onto a typed JSON Schema node.
 * Typed as `JsonSchema -> JsonSchema` to sidestep variance issues — every
 * variant of `JsonSchema` accepts `description`, and the schema builder
 * callsites pass in concrete shapes the union accepts.
 */
function withDescription(node: JsonSchema, opts?: { description?: string }): JsonSchema {
  if (opts?.description !== undefined) {
    return { ...node, description: opts.description };
  }
  return node;
}

// ---------------------------------------------------------------------------
// Object inference
//
// Split keys into required / optional based on the OptionalSchema discriminator.
// Produces { req: T; ...; opt?: U; ... } — matches how JSON Schema's
// `required` array partitions properties.
// ---------------------------------------------------------------------------

type RequiredKeys<S extends Record<string, Schema<unknown>>> = {
  [K in keyof S]: S[K] extends OptionalSchema<unknown> ? never : K;
}[keyof S];

type OptionalKeys<S extends Record<string, Schema<unknown>>> = {
  [K in keyof S]: S[K] extends OptionalSchema<unknown> ? K : never;
}[keyof S];

type ObjectInfer<S extends Record<string, Schema<unknown>>> = {
  [K in RequiredKeys<S>]: Infer<S[K]>;
} & {
  [K in OptionalKeys<S>]?: Infer<S[K]>;
};

// ---------------------------------------------------------------------------
// Public `s.*` builder
// ---------------------------------------------------------------------------

export const s = {
  string(opts?: { description?: string }): Schema<string> {
    return makeSchema<string>(withDescription({ type: "string" } as const, opts));
  },

  number(opts?: { description?: string; minimum?: number; maximum?: number }): Schema<number> {
    const node: JsonSchema = {
      type: "number",
      ...(opts?.minimum !== undefined && { minimum: opts.minimum }),
      ...(opts?.maximum !== undefined && { maximum: opts.maximum }),
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<number>(node);
  },

  integer(opts?: { description?: string; minimum?: number; maximum?: number }): Schema<number> {
    const node: JsonSchema = {
      type: "integer",
      ...(opts?.minimum !== undefined && { minimum: opts.minimum }),
      ...(opts?.maximum !== undefined && { maximum: opts.maximum }),
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<number>(node);
  },

  boolean(opts?: { description?: string }): Schema<boolean> {
    return makeSchema<boolean>(withDescription({ type: "boolean" } as const, opts));
  },

  null(opts?: { description?: string }): Schema<null> {
    return makeSchema<null>(withDescription({ type: "null" } as const, opts));
  },

  /**
   * Open slot — matches any JSON value. JSON Schema convention: an empty
   * schema (no `type`) accepts anything. Use for callsites that want an
   * intentionally-typeless field (metadata bags, opaque payloads).
   */
  unknown(opts?: { description?: string }): Schema<unknown> {
    const node: JsonSchema =
      opts?.description !== undefined ? { description: opts.description } : {};
    return makeSchema<unknown>(node);
  },

  /**
   * String literal union. `s.enum(["a", "b"] as const)` yields `Schema<"a" | "b">`.
   * `as const` is what lets TS narrow the element type.
   */
  enum<const T extends readonly string[]>(
    values: T,
    opts?: { description?: string },
  ): Schema<T[number]> {
    const node: JsonSchema = {
      type: "string",
      enum: [...values],
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<T[number]>(node);
  },

  array<T>(item: Schema<T>, opts?: { description?: string }): Schema<T[]> {
    const node: JsonSchema = {
      type: "array",
      items: item.jsonSchema,
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<T[]>(node);
  },

  /**
   * Object with named properties. Optional properties (via `.optional()`)
   * land outside `required` — the validator and the form renderer both
   * read `required` to know which fields are mandatory.
   *
   * `additionalProperties: false` is intentional: signal payloads should
   * be closed shapes, and extra fields hint at a schema-version mismatch
   * the validator should catch.
   */
  object<S extends Record<string, Schema<unknown>>>(
    properties: S,
    opts?: { description?: string },
  ): Schema<ObjectInfer<S>> {
    const props: Record<string, JsonSchema> = {};
    const required: string[] = [];
    for (const key of Object.keys(properties)) {
      const child = properties[key]!;
      props[key] = child.jsonSchema;
      if (!isOptionalSchema(child)) required.push(key);
    }
    const node: JsonSchema = {
      type: "object",
      properties: props,
      additionalProperties: false,
      ...(required.length > 0 && { required }),
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<ObjectInfer<S>>(node);
  },

  /**
   * Tagged union — produces `{ anyOf: [...] }`. The validator accepts the
   * first branch that matches; the form renderer falls back to the generic
   * JSON editor for unions (rendering a branch-picker is out of scope here).
   */
  union<T>(options: ReadonlyArray<Schema<T>>, opts?: { description?: string }): Schema<T> {
    const node: JsonSchema = {
      anyOf: options.map((o) => o.jsonSchema),
      ...(opts?.description !== undefined && { description: opts.description }),
    };
    return makeSchema<T>(node);
  },
};

function isOptionalSchema(schema: Schema<unknown>): schema is OptionalSchema<unknown> {
  return (schema as { isOptional?: boolean }).isOptional === true;
}
