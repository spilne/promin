import { describe, it, expect, afterEach } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createFileToolRegistry } from "../tool-registry.ts";

// Self-contained: no local imports so it resolves correctly from tmpdir
function toolFile(name: string): string {
  return [
    `export default {`,
    `  name: "${name}",`,
    `  description: "test tool ${name}",`,
    `  parameters: { parse: (x) => x },`,
    `  execute: async () => "${name}",`,
    `};`,
  ].join("\n");
}

async function tempDir(): Promise<string> {
  const dir = join(tmpdir(), `promin-registry-test-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  return dir;
}

const dirs: string[] = [];
afterEach(async () => {
  for (const dir of dirs.splice(0)) {
    await rm(dir, { recursive: true, force: true });
  }
});

describe("createFileToolRegistry — flat directory", () => {
  it("loads tools from the directory on init", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await writeFile(join(dir, "alpha.ts"), toolFile("alpha"));
    await writeFile(join(dir, "beta.ts"), toolFile("beta"));

    const registry = await createFileToolRegistry({ dir, watch: false });
    const tools = registry.getTools();
    expect(Object.keys(tools).sort()).toEqual(["alpha", "beta"]);
    registry.close();
  });

  it("ignores non-ts/js files", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await writeFile(join(dir, "alpha.ts"), toolFile("alpha"));
    await writeFile(join(dir, "readme.md"), "# docs");

    const registry = await createFileToolRegistry({ dir, watch: false });
    expect(Object.keys(registry.getTools())).toEqual(["alpha"]);
    registry.close();
  });

  it("returns empty map for empty directory", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    const registry = await createFileToolRegistry({ dir, watch: false });
    expect(registry.getTools()).toEqual({});
    registry.close();
  });
});

describe("createFileToolRegistry — recursive / categories", () => {
  it("discovers tools in subdirectories with recursive:true (default)", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await mkdir(join(dir, "search"));
    await mkdir(join(dir, "data"));
    await writeFile(join(dir, "top-level.ts"), toolFile("top-level"));
    await writeFile(join(dir, "search", "google.ts"), toolFile("google"));
    await writeFile(join(dir, "data", "fetch.ts"), toolFile("fetch"));

    const registry = await createFileToolRegistry({ dir, watch: false });
    const names = Object.keys(registry.getTools()).sort();
    expect(names).toEqual(["fetch", "google", "top-level"]);
    registry.close();
  });

  it("does NOT scan subdirectories when recursive:false", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await mkdir(join(dir, "search"));
    await writeFile(join(dir, "root.ts"), toolFile("root"));
    await writeFile(join(dir, "search", "google.ts"), toolFile("google"));

    const registry = await createFileToolRegistry({ dir, watch: false, recursive: false });
    expect(Object.keys(registry.getTools())).toEqual(["root"]);
    registry.close();
  });

  it("onLoad receives the category name for nested tools", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await mkdir(join(dir, "search"));
    await writeFile(join(dir, "root.ts"), toolFile("root"));
    await writeFile(join(dir, "search", "google.ts"), toolFile("google"));

    const loaded: Array<{ name: string; category: string | undefined }> = [];
    const registry = await createFileToolRegistry({
      dir,
      watch: false,
      onLoad: (name, category) => loaded.push({ name, category }),
    });

    const rootEntry = loaded.find((e) => e.name === "root");
    const googleEntry = loaded.find((e) => e.name === "google");
    expect(rootEntry?.category).toBeUndefined();
    expect(googleEntry?.category).toBe("search");
    registry.close();
  });

  it("registers tools under their flat t.name, not the file path", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await mkdir(join(dir, "web"));
    // File path is web/my-fetcher.ts but tool name inside is "fetch-url"
    await writeFile(join(dir, "web", "my-fetcher.ts"), toolFile("fetch-url"));

    const registry = await createFileToolRegistry({ dir, watch: false });
    const tools = registry.getTools();
    expect(tools["fetch-url"]).toBeDefined();
    expect(tools["my-fetcher"]).toBeUndefined();
    registry.close();
  });
});

describe("createFileToolRegistry — onLoad / onUnload callbacks", () => {
  it("calls onLoad for each successfully loaded tool", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await writeFile(join(dir, "t1.ts"), toolFile("t1"));
    await writeFile(join(dir, "t2.ts"), toolFile("t2"));

    const loaded: string[] = [];
    const registry = await createFileToolRegistry({
      dir,
      watch: false,
      onLoad: (name) => loaded.push(name),
    });
    expect(loaded.sort()).toEqual(["t1", "t2"]);
    registry.close();
  });

  it("calls onError for files with invalid exports", async () => {
    const dir = await tempDir();
    dirs.push(dir);
    await writeFile(join(dir, "bad.ts"), `export default { notATool: true };`);

    const errors: string[] = [];
    const registry = await createFileToolRegistry({
      dir,
      watch: false,
      onError: (file) => errors.push(file),
    });
    expect(errors.length).toBe(1);
    expect(errors[0]).toContain("bad.ts");
    registry.close();
  });
});
