import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { markdownToMdx, renderEmbed, rewriteEmbeds } from "../../documentation/build.ts";

test("README prose is safe MDX while code and relative source links stay useful", () => {
  const content =
    "Use {input} and <T>. `Map<A>`\n```ts\nconst input = { n: 1 };\n```\n[example](./example.ts)";
  const converted = markdownToMdx(content, join(import.meta.dir, "README.md"));
  expect(converted).toContain("&#123;input&#125; and &lt;T&gt;");
  expect(converted).toContain("`Map<A>`");
  expect(converted).toContain("const input = { n: 1 };");
  expect(converted).toContain("https://github.com/spilne/promin/blob/main/scripts/test/example.ts");
});

test("embedded regions fail if renamed and code fences cannot be closed by the example", () => {
  expect(
    renderEmbed("// >>> example: main\nconst n = 1;\n// <<< example", "example.ts", "main"),
  ).toContain("const n = 1;");
  expect(() => renderEmbed("const n = 1;", "example.ts", "main")).toThrow("Missing");
  expect(renderEmbed('const text = "```";', "example.ts")).toStartWith("````ts");
});

test("docs regenerate from examples, stay stable, and reject malformed or missing references", async () => {
  const root = await mkdtemp(join(tmpdir(), "promin-docs-test-"));
  try {
    await writeFile(join(root, "example.ts"), "export const n = 1;\n");
    const original = "{/* @embed example.ts */}\nold\n{/* @end */}";
    const generated = await rewriteEmbeds(original, root);
    expect(generated).toContain("export const n = 1;");
    expect(await rewriteEmbeds(generated, root)).toBe(generated);
    await writeFile(join(root, "example.ts"), "export const n = 2;\n");
    expect(await rewriteEmbeds(generated, root)).toContain("export const n = 2;");
    await expect(rewriteEmbeds("{/* @embed missing.ts */}", root)).rejects.toThrow("Malformed");
    await expect(
      rewriteEmbeds(original.replace("example.ts", "missing.ts"), root),
    ).rejects.toThrow();
    await expect(
      rewriteEmbeds(original.replace("example.ts", "../outside.ts"), root),
    ).rejects.toThrow("escapes");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
