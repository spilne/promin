import { describe, it, expect } from "bun:test";
import { MarkdownRenderer, osc8 } from "../terminal-markdown.ts";

// Strip all ANSI codes for plain-text assertions
// eslint-disable-next-line no-control-regex
const strip = (s: string) => s.replace(/\x1b\][^\x07]*\x07|\x1b\[[0-9;]*[A-Za-z]/g, "");

describe("osc8", () => {
  it("wraps text in OSC 8 escape sequence", () => {
    const r = osc8("https://example.com", "link");
    expect(r).toBe("\x1b]8;;https://example.com\x07link\x1b]8;;\x07");
  });
});

describe("MarkdownRenderer — code fences", () => {
  it("renders opening fence with language label", () => {
    const r = new MarkdownRenderer({ width: 40 });
    const out = r.push("```typescript\n");
    expect(out).toContain("typescript");
    expect(out).toContain("┌");
    expect(out).toContain("─");
  });

  it("renders closing fence", () => {
    const r = new MarkdownRenderer({ width: 40 });
    r.push("```\n");
    const out = r.push("```\n");
    expect(out).toContain("└");
  });

  it("prefixes code lines with │", () => {
    const r = new MarkdownRenderer({ width: 40 });
    r.push("```\n");
    const out = r.push("const x = 1;\n");
    expect(strip(out)).toContain("│  const x = 1;");
  });

  it("flush closes unclosed fence", () => {
    const r = new MarkdownRenderer({ width: 40 });
    r.push("```ts\n");
    r.push("code\n");
    const tail = r.flush();
    expect(tail).toContain("└");
  });
});

describe("MarkdownRenderer — headers", () => {
  it("renders h1 with clay color", () => {
    const r = new MarkdownRenderer();
    const out = r.push("# Hello\n");
    expect(strip(out)).toContain("Hello");
    expect(out).toContain("\x1b[38;2;217;119;87m"); // CLAY
    expect(out).toContain("\x1b[1m"); // BOLD
  });

  it("renders h2 bold", () => {
    const r = new MarkdownRenderer();
    const out = r.push("## Section\n");
    expect(strip(out)).toContain("Section");
    expect(out).toContain("\x1b[1m");
  });

  it("renders h3 dim+bold", () => {
    const r = new MarkdownRenderer();
    const out = r.push("### Sub\n");
    expect(strip(out)).toContain("Sub");
    expect(out).toContain("\x1b[2m");
  });
});

describe("MarkdownRenderer — inline", () => {
  it("renders **bold**", () => {
    const r = new MarkdownRenderer();
    const out = r.push("this is **bold** text\n");
    expect(strip(out)).toContain("bold");
    expect(out).toContain("\x1b[1m");
  });

  it("renders *italic*", () => {
    const r = new MarkdownRenderer();
    const out = r.push("this is *italic* text\n");
    expect(strip(out)).toContain("italic");
    expect(out).toContain("\x1b[3m");
  });

  it("renders `inline code`", () => {
    const r = new MarkdownRenderer();
    const out = r.push("use `foo()` here\n");
    expect(strip(out)).toContain("`foo()`");
    expect(out).toContain("\x1b[2m");
  });

  it("renders [text](url) as OSC 8 link", () => {
    const r = new MarkdownRenderer();
    const out = r.push("[click here](https://example.com)\n");
    expect(out).toContain("\x1b]8;;https://example.com\x07");
    expect(out).toContain("click here");
  });

  it("auto-links bare https URLs", () => {
    const r = new MarkdownRenderer();
    const out = r.push("see https://example.com for details\n");
    expect(out).toContain("\x1b]8;;https://example.com\x07");
  });

  it("leaves plain text unchanged", () => {
    const r = new MarkdownRenderer();
    const out = r.push("just a plain sentence\n");
    expect(strip(out)).toContain("just a plain sentence");
  });
});

describe("MarkdownRenderer — lists and blockquotes", () => {
  it("renders unordered list items with bullet", () => {
    const r = new MarkdownRenderer();
    const out = r.push("- item one\n");
    expect(strip(out)).toContain("• item one");
  });

  it("renders ordered list items", () => {
    const r = new MarkdownRenderer();
    const out = r.push("1. first\n");
    expect(strip(out)).toContain("1. first");
  });

  it("renders blockquote with │ prefix", () => {
    const r = new MarkdownRenderer();
    const out = r.push("> quoted text\n");
    expect(strip(out)).toContain("│ quoted text");
  });
});

describe("MarkdownRenderer — streaming", () => {
  it("buffers partial lines and flushes on newline", () => {
    const r = new MarkdownRenderer();
    expect(r.push("hel")).toBe("");
    expect(r.push("lo\n")).toContain("hello");
  });

  it("handles multiple lines in a single chunk", () => {
    const r = new MarkdownRenderer();
    const out = r.push("line1\nline2\nline3\n");
    expect(strip(out)).toContain("line1");
    expect(strip(out)).toContain("line2");
    expect(strip(out)).toContain("line3");
  });

  it("flush returns last partial line without trailing newline", () => {
    const r = new MarkdownRenderer();
    r.push("partial");
    const tail = r.flush();
    expect(strip(tail)).toBe("partial");
    expect(tail.endsWith("\n")).toBe(false);
  });

  it("flush on empty buffer returns empty string", () => {
    const r = new MarkdownRenderer();
    r.push("done\n");
    expect(r.flush()).toBe("");
  });
});

describe("MarkdownRenderer — file path OSC 8 links", () => {
  it("wraps ./relative paths in file:// links when workspace set", () => {
    const r = new MarkdownRenderer({ workspace: "/home/user/project" });
    const out = r.push("see ./src/foo.ts for details\n");
    expect(out).toContain("\x1b]8;;file:///home/user/project/src/foo.ts\x07");
    expect(out).toContain("./src/foo.ts");
  });

  it("wraps /absolute paths without workspace", () => {
    const r = new MarkdownRenderer();
    const out = r.push("edit /etc/config.json please\n");
    expect(out).toContain("\x1b]8;;file:///etc/config.json\x07");
  });

  it("does not link paths when workspace is not set for relative", () => {
    const r = new MarkdownRenderer();
    const out = r.push("see ./src/foo.ts\n");
    // Without workspace, relative paths are left as plain text
    expect(out).not.toContain("\x1b]8;;file://");
  });
});
