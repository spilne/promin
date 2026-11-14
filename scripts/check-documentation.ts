import { stat, readFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import type { DefaultTheme } from "vitepress";
import { sidebar } from "../documentation/.vitepress/sidebar.ts";
import { ROOT } from "./workspace.ts";

const source = join(ROOT, "documentation");
const output = join(source, ".vitepress/dist");
const base = "/promin/";

async function outputFile(path: string): Promise<string> {
  const target = resolve(output, `.${path}`);
  if (target !== output && !target.startsWith(`${output}/`))
    throw new Error(`Link escapes site: ${path}`);
  const candidates = path.endsWith("/")
    ? [join(target, "index.html")]
    : [target, `${target}.html`, join(target, "index.html")];
  for (const candidate of candidates) {
    try {
      if ((await stat(candidate)).isFile()) return candidate;
    } catch {
      /* Try the clean-URL alternatives. */
    }
  }
  throw new Error(`Missing documentation target: ${path}`);
}

const links = new Set<string>();
const queue: DefaultTheme.SidebarItem[] = [...sidebar];
while (queue.length) {
  const item = queue.shift()!;
  if (item.link) links.add(item.link);
  if (item.items) queue.push(...item.items);
}
let pageCount = 0;
for await (const path of new Bun.Glob("docs/**/*.md").scan({ cwd: source })) {
  const route = `/${path.replace(/index\.md$/, "").replace(/\.md$/, "")}`;
  if (!links.has(route)) throw new Error(`Documentation page absent from sidebar: ${path}`);
  await outputFile(route);
  pageCount++;
}
for (const link of links) await outputFile(link);

const htmlCache = new Map<string, string>();
async function html(file: string): Promise<string> {
  let value = htmlCache.get(file);
  if (value === undefined) {
    value = await readFile(file, "utf8");
    htmlCache.set(file, value);
  }
  return value;
}

let checked = 0;
for await (const path of new Bun.Glob("**/*.html").scan({ cwd: output })) {
  const file = join(output, path);
  const page = new URL(`${base}${path.replace(/index\.html$/, "")}`, "https://docs.test");
  for (const match of (await html(file)).matchAll(
    /<(?:a|link|script|img)\b[^>]*?\b(?:href|src)="([^"]+)"/g,
  )) {
    const target = new URL(match[1]!.replaceAll("&amp;", "&"), page);
    if (target.origin !== page.origin) continue;
    if (!target.pathname.startsWith(base))
      throw new Error(`${path}: link omits Pages base: ${target.pathname}`);
    const destination = await outputFile(target.pathname.slice(base.length - 1));
    if (target.hash && destination.endsWith(".html")) {
      const id = decodeURIComponent(target.hash.slice(1));
      if (!(await html(destination)).includes(`id="${id}"`)) {
        throw new Error(
          `${path}: missing anchor ${target.hash} in ${relative(output, destination)}`,
        );
      }
    }
    checked++;
  }
}
if ([...new Bun.Glob("assets/**/*localSearchIndex*").scanSync({ cwd: output })].length === 0) {
  throw new Error("VitePress local search index is missing");
}
console.log(
  `Verified ${pageCount} documentation pages, ${checked} local links/assets, sidebar coverage and local search`,
);
