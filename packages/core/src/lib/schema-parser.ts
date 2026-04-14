// ---------------------------------------------------------------------------
// SchemaParser — library-agnostic validation interface
// ---------------------------------------------------------------------------
//
// Anything that can validate `unknown` into a typed value satisfies this.
// Zod, Valibot, ArkType, @effect/Schema, and plain hand-rolled validators
// all match this shape.
//
// Usage with Zod (works directly):
//   const UserSchema = z.object({ id: z.string() });
//   const parser: SchemaParser<User> = UserSchema;
//
// Usage with custom validators:
//   const parser: SchemaParser<User> = {
//     safeParse: (data) => isUser(data)
//       ? { success: true, data }
//       : { success: false, error: "not a user" },
//   };
// ---------------------------------------------------------------------------

export interface SchemaParser<T> {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}
