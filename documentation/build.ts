import { readFile, readdir, writeFile } from "node:fs/promises";
import { relative, resolve, dirname, sep } from "node:path";
import { ROOT, workspacePackages } from "../scripts/workspace.ts";

const DOCS = resolve(ROOT, "documentation/docs");
const GITHUB = "https://github.com/spilne/promin/blob/main/";

export function markdownForSite(markdown: string, file: string): string {
  let fence: string | undefined;
  return markdown
    .split("\n")
    .map((line) => {
      const marker = line.match(/^\s*(`{3,}|~{3,})/);
      if (marker) {
        if (!fence) fence = marker[1]!;
        else if (marker[1]![0] === fence[0] && marker[1]!.length >= fence.length) fence = undefined;
        return line;
      }
      if (fence) return line;
      // VitePress renders Markdown through Vue. Preserve code spans while
      // escaping type parameters and template expressions in prose.
      const quote = line.match(/^(\s*(?:> ?)+)/)?.[0] ?? "";
      return (
        quote +
        line
          .slice(quote.length)
          .split(/(`+[^`]*`+)/g)
          .map((part, i) => {
            if (i % 2) return part;
            return part
              .replace(
                /\]\((\.{1,2}\/[^\s)]+)([^)]*)\)/g,
                (_all, target: string, title: string) => {
                  const path = relative(ROOT, resolve(dirname(file), target))
                    .split(sep)
                    .join("/");
                  return `](${GITHUB}${path}${title})`;
                },
              )
              .replaceAll("{", "&#123;")
              .replaceAll("}", "&#125;")
              .replaceAll("<", "&lt;")
              .replaceAll(">", "&gt;");
          })
          .join("")
      );
    })
    .join("\n");
}

export function renderEmbed(source: string, file: string, region?: string): string {
  let content = source;
  if (region) {
    const escaped = region.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = source.match(
      new RegExp(`// >>> example: ${escaped}\\r?\\n([\\s\\S]*?)// <<< example`),
    );
    if (!match) throw new Error(`Missing example region ${region} in ${file}`);
    content = match[1]!;
  }
  content = content.trim();
  if (/\.tsx?$/.test(file)) {
    const fence = "`".repeat(
      Math.max(3, ...[...content.matchAll(/`+/g)].map((m) => m[0].length + 1)),
    );
    return `${fence}ts\n${content}\n${fence}`;
  }
  return markdownForSite(content, file);
}

export async function rewriteEmbeds(markdown: string, root = ROOT): Promise<string> {
  const pattern = /<!-- @embed ([^\s]+) -->[\s\S]*?<!-- @end -->/g;
  const matches = [...markdown.matchAll(pattern)];
  if (
    matches.length !== [...markdown.matchAll(/@embed\b/g)].length ||
    matches.length !== [...markdown.matchAll(/@end\b/g)].length
  ) {
    throw new Error("Malformed @embed block");
  }
  for (const match of matches) {
    const [path, region] = match[1]!.split("#");
    const file = resolve(root, path!);
    if (!file.startsWith(`${root}${sep}`)) throw new Error(`Embed escapes repository: ${path}`);
    const content = renderEmbed(await readFile(file, "utf8"), file, region);
    markdown = markdown.replace(
      match[0],
      `<!-- @embed ${match[1]} -->\n\n${content}\n\n<!-- @end -->`,
    );
  }
  return markdown;
}

async function* pages(dir: string): AsyncGenerator<string> {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = resolve(dir, entry.name);
    if (entry.isDirectory()) yield* pages(path);
    else if (entry.name.endsWith(".md")) yield path;
  }
}

async function main(): Promise<void> {
  const check = process.argv.includes("--check");
  const coverage = await readFile(resolve(DOCS, "index.md"), "utf8");
  const packages = await workspacePackages();
  for (const { manifest, directory } of packages) {
    if (!coverage.includes(`\`${manifest.name}\``))
      throw new Error(`${manifest.name} missing from documentation package map`);
    await readFile(resolve(directory, "README.md"), "utf8");
  }
  let changed = 0;
  for await (const file of pages(DOCS)) {
    const original = await readFile(file, "utf8");
    if (original.includes("<include>"))
      throw new Error(`${file}: replace legacy <include> with @embed`);
    const content = await rewriteEmbeds(original);
    if (content === original) continue;
    changed++;
    console.log(`${check ? "stale" : "updated"}: ${relative(ROOT, file)}`);
    if (!check) await writeFile(file, content);
  }
  if (check && changed)
    throw new Error(
      `${changed} stale page(s); run bun run documentation:build and commit the results`,
    );
  console.log(
    `Documentation checked: ${packages.length} packages, ${changed} page(s) ${check ? "out of sync" : "updated"}`,
  );
}

if (import.meta.main) await main();
