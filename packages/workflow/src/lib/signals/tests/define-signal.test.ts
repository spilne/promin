import { describe, expect, it } from "bun:test";
import { s } from "../../schema/builder.ts";
import { validate } from "../../schema/validator.ts";
import {
  APPROVAL_NAME_PREFIX,
  ApprovalSchema,
  approvalSignal,
  defineSignal,
} from "../define-signal.ts";

describe("defineSignal", () => {
  it("returns an artifact carrying name + schema", () => {
    const ReviewSignal = defineSignal({
      name: "review",
      schema: s.object({
        approved: s.boolean(),
      }),
    });
    expect(ReviewSignal.name).toBe("review");
    expect(ReviewSignal.schema.jsonSchema).toMatchObject({
      type: "object",
      properties: { approved: { type: "boolean" } },
    });
  });

  it("preserves optional version field", () => {
    const Sig = defineSignal({
      name: "x",
      schema: s.string(),
      version: "v2",
    });
    expect(Sig.version).toBe("v2");
  });

  it("rejects empty name", () => {
    expect(() => defineSignal({ name: "", schema: s.string() })).toThrow(/non-empty string/);
  });
});

describe("approvalSignal — canonical preset", () => {
  it("prefixes name with approve:", () => {
    const sig = approvalSignal("call-123");
    expect(sig.name).toBe(`${APPROVAL_NAME_PREFIX}call-123`);
  });

  it("uses ApprovalSchema", () => {
    const sig = approvalSignal("x");
    expect(sig.schema.jsonSchema).toEqual(ApprovalSchema.jsonSchema);
  });

  it("ApprovalSchema validates a minimal approval", () => {
    expect(validate({ approved: true }, ApprovalSchema.jsonSchema).ok).toBe(true);
    expect(validate({ approved: false }, ApprovalSchema.jsonSchema).ok).toBe(true);
  });

  it("ApprovalSchema accepts the full shape", () => {
    const ok = validate(
      {
        approved: true,
        by: "alice",
        reason: "looks good",
        metadata: { cost: 0.42 },
      },
      ApprovalSchema.jsonSchema,
    );
    expect(ok.ok).toBe(true);
  });

  it("ApprovalSchema rejects missing approved", () => {
    const result = validate({ by: "bob" }, ApprovalSchema.jsonSchema);
    expect(result.ok).toBe(false);
    expect(result.errors[0]?.path).toBe("approved");
  });

  it("ApprovalSchema rejects unexpected top-level fields", () => {
    const result = validate({ approved: true, badField: 1 }, ApprovalSchema.jsonSchema);
    expect(result.ok).toBe(false);
    expect(result.errors.some((e) => e.path === "badField")).toBe(true);
  });
});
