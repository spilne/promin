import { describe, it, expect } from "bun:test";
import { parseMarkdownSkill, slugify } from "../parse-markdown-skill.ts";

function ok(text: string, fallbackId?: string) {
  const r = parseMarkdownSkill({ text, ...(fallbackId !== undefined && { fallbackId }) });
  if (r === null) throw new Error("expected a parse result, got null");
  if ("warning" in r) throw new Error(`expected a skill, got warning: ${r.warning}`);
  return r.skill;
}

describe("parseMarkdownSkill", () => {
  it("parses frontmatter + body into a skill with a slugified id", () => {
    const skill = ok(
      [
        "---",
        "name: Structured Debugging",
        "description: A disciplined debugging loop.",
        "---",
        "",
        "# Structured debugging",
        "",
        "Reproduce first.",
      ].join("\n"),
    );
    expect(skill.id).toBe("structured-debugging");
    expect(skill.description).toBe("A disciplined debugging loop.");
    expect(skill.body).toBe("# Structured debugging\n\nReproduce first.");
    expect(skill.whenToUse).toBeUndefined(); // folded into description by convention
  });

  it("reads an explicit when-to-use key", () => {
    const skill = ok(
      ["---", "name: x", "description: d", "when-to-use: a bug resists a fix", "---", "body"].join(
        "\n",
      ),
    );
    expect(skill.whenToUse).toBe("a bug resists a fix");
  });

  it("reads version, tags, and capabilities (inline list)", () => {
    const skill = ok(
      [
        "---",
        "name: x",
        "description: d",
        "version: v3",
        "tags: [engineering, debugging]",
        'capabilities: ["skills"]',
        "---",
        "body",
      ].join("\n"),
    );
    expect(skill.version).toBe("v3");
    expect(skill.metadata?.tags).toEqual(["engineering", "debugging"]);
    expect(skill.metadata?.capabilities).toEqual(["skills"]);
  });

  it("strips surrounding quotes on scalar values", () => {
    const skill = ok(["---", 'name: "Quoted Name"', "description: 'd'", "---", "body"].join("\n"));
    expect(skill.id).toBe("quoted-name");
    expect(skill.description).toBe("d");
  });

  it("falls back to the provided id when frontmatter omits name", () => {
    const skill = ok(["---", "description: d", "---", "body"].join("\n"), "my-skill");
    expect(skill.id).toBe("my-skill");
  });

  it("returns null when there is no frontmatter (e.g. a README)", () => {
    expect(parseMarkdownSkill({ text: "# Just a readme\n\nNothing here." })).toBeNull();
  });

  it("warns when frontmatter is present but description is missing", () => {
    const r = parseMarkdownSkill({ text: ["---", "name: x", "---", "body"].join("\n") });
    expect(r && "warning" in r).toBe(true);
  });

  it("warns when there is no resolvable id", () => {
    const r = parseMarkdownSkill({ text: ["---", "description: d", "---", "body"].join("\n") });
    expect(r && "warning" in r).toBe(true);
  });

  it("warns on an empty body", () => {
    const r = parseMarkdownSkill({
      text: ["---", "name: x", "description: d", "---", ""].join("\n"),
    });
    expect(r && "warning" in r).toBe(true);
  });

  it("tolerates CRLF line endings", () => {
    const skill = ok("---\r\nname: x\r\ndescription: d\r\n---\r\nbody text");
    expect(skill.id).toBe("x");
    expect(skill.body).toBe("body text");
  });
});

describe("slugify", () => {
  it("lowercases and hyphenates", () => {
    expect(slugify("Structured Debugging")).toBe("structured-debugging");
    expect(slugify("  Hello, World!  ")).toBe("hello-world");
    expect(slugify("already-slug")).toBe("already-slug");
  });
});
