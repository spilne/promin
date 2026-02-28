// ---------------------------------------------------------------------------
// Codec<T> — Serializable typeclass
// "I can serialize and deserialize values of T"
// ---------------------------------------------------------------------------

/** Typeclass for step result serialization. */
export interface Codec<T> {
  encode(value: T): unknown;
  decode(raw: unknown): T;
}

/** Identity codec — assumes T is already JSON-serializable. */
export const JsonCodec: Codec<unknown> = {
  encode: (v) => v,
  decode: (v) => v,
};

/** Derive a Codec from a Zod schema. encode = identity, decode = schema.parse. */
export function codecFromSchema<T>(schema: { parse(data: unknown): T }): Codec<T> {
  return {
    encode: (v) => v,
    decode: (raw) => schema.parse(raw),
  };
}

/** Compose two codecs into a tuple codec. */
export function codecTuple<A, B>(a: Codec<A>, b: Codec<B>): Codec<[A, B]> {
  return {
    encode: (value) => [a.encode(value[0]), b.encode(value[1])],
    decode: (raw) => {
      const arr = raw as [unknown, unknown];
      return [a.decode(arr[0]), b.decode(arr[1])];
    },
  };
}

/** Compose codecs for a record type. */
export function codecRecord<V>(value: Codec<V>): Codec<Record<string, V>> {
  return {
    encode: (rec) => {
      const result: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(rec)) {
        result[k] = value.encode(v);
      }
      return result;
    },
    decode: (raw) => {
      const obj = raw as Record<string, unknown>;
      const result: Record<string, V> = {};
      for (const [k, v] of Object.entries(obj)) {
        result[k] = value.decode(v);
      }
      return result;
    },
  };
}

/** Compose a codec for arrays. */
export function codecArray<T>(item: Codec<T>): Codec<T[]> {
  return {
    encode: (arr) => arr.map((v) => item.encode(v)),
    decode: (raw) => (raw as unknown[]).map((v) => item.decode(v)),
  };
}
