import { describe, it, expect } from "bun:test";
import { parseSearchQuery, serializeQuery, hasAnyFilter } from "../smart-search.ts";

describe("parseSearchQuery", () => {
  it("returns empty for a blank string", () => {
    expect(parseSearchQuery("")).toEqual({});
    expect(parseSearchQuery("   ")).toEqual({});
  });

  it("parses bare tokens as freeText", () => {
    expect(parseSearchQuery("hello world")).toEqual({ freeText: "hello world" });
  });

  it("parses field:value tokens", () => {
    expect(parseSearchQuery("name:onboarding type:webhook version:v2 namespace:team-a")).toEqual({
      name: "onboarding",
      type: "webhook",
      version: "v2",
      namespace: "team-a",
    });
  });

  it("parses key=value tokens into metadata, with JSON value coercion", () => {
    expect(parseSearchQuery("userId=u_42 retries=3 dryRun=true region=us-east")).toEqual({
      metadata: { userId: "u_42", retries: 3, dryRun: true, region: "us-east" },
    });
  });

  it("supports double-quoted values containing spaces", () => {
    expect(parseSearchQuery('name:"my workflow" customer="Acme Corp"')).toEqual({
      name: "my workflow",
      metadata: { customer: "Acme Corp" },
    });
  });

  it("mixes structured filters and free text", () => {
    expect(parseSearchQuery("hello name:onboarding userId=u_42")).toEqual({
      name: "onboarding",
      metadata: { userId: "u_42" },
      freeText: "hello",
    });
  });

  it("ignores unknown field prefixes (treats them as metadata when `=` present, else free text)", () => {
    expect(parseSearchQuery("foo:bar")).toEqual({ freeText: "foo:bar" });
  });

  it("treats `name:foo=bar` as a name with a literal `=` since `:` comes first", () => {
    expect(parseSearchQuery("name:foo=bar")).toEqual({ name: "foo=bar" });
  });

  it("drops empty values like `name:` from output", () => {
    expect(parseSearchQuery("name:")).toEqual({});
  });
});

describe("serializeQuery", () => {
  it("round-trips a parsed query", () => {
    const text = "name:onboarding type:webhook userId=u_42 retries=3 hello";
    const parsed = parseSearchQuery(text);
    const back = parseSearchQuery(serializeQuery(parsed));
    expect(back).toEqual(parsed);
  });

  it("quotes values with whitespace", () => {
    expect(serializeQuery({ name: "my workflow" })).toBe('name:"my workflow"');
  });

  it("returns empty string for an empty query", () => {
    expect(serializeQuery({})).toBe("");
  });
});

describe("hasAnyFilter", () => {
  it("is false for an empty query", () => {
    expect(hasAnyFilter({})).toBe(false);
  });

  it("is true when any field is set", () => {
    expect(hasAnyFilter({ name: "x" })).toBe(true);
    expect(hasAnyFilter({ freeText: "x" })).toBe(true);
    expect(hasAnyFilter({ metadata: { k: "v" } })).toBe(true);
  });

  it("is false when metadata is set to an empty object", () => {
    expect(hasAnyFilter({ metadata: {} })).toBe(false);
  });
});
