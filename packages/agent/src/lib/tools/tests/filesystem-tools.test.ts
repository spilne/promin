import { describe, it, expect, afterEach } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, writeFile, rm, symlink } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  createReadFileTool,
  createWriteFileTool,
  createListDirTool,
  createStatTool,
  createFilesystemTools,
} from "../filesystem-tools.ts";

async function tempDir(): Promise<string> {
  const dir = join(tmpdir(), `promin-fs-test-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  return dir;
}

const dirs: string[] = [];
afterEach(async () => {
  for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
});

// ---- readFile ----

describe("createReadFileTool", () => {
  it("reads file contents", async () => {
    const root = await tempDir();
    dirs.push(root);
    await writeFile(join(root, "hello.txt"), "hello world");
    const t = createReadFileTool({ rootDir: root });
    expect(await t.execute({ path: "hello.txt" })).toBe("hello world");
  });

  it("truncates large files", async () => {
    const root = await tempDir();
    dirs.push(root);
    await writeFile(join(root, "big.txt"), "x".repeat(200));
    const t = createReadFileTool({ rootDir: root, maxReadBytes: 100 });
    const result = await t.execute({ path: "big.txt" });
    expect(result).toContain("[truncated");
    expect(result).toContain("x".repeat(100));
  });

  it("rejects path traversal outside root", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createReadFileTool({ rootDir: root });
    await expect(t.execute({ path: "../etc/passwd" })).rejects.toThrow("escapes");
  });

  it("rejects double-dot traversal two levels up", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createReadFileTool({ rootDir: root });
    await expect(t.execute({ path: "../../etc/hosts" })).rejects.toThrow("escapes");
  });
});

// ---- writeFile ----

describe("createWriteFileTool", () => {
  it("writes file and returns confirmation", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createWriteFileTool({ rootDir: root });
    const result = await t.execute({ path: "out.txt", content: "hello" });
    expect(result).toContain("out.txt");
    const buf = await Bun.file(join(root, "out.txt")).text();
    expect(buf).toBe("hello");
  });

  it("creates parent directories as needed", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createWriteFileTool({ rootDir: root });
    await t.execute({ path: "a/b/c.ts", content: "// hi" });
    const buf = await Bun.file(join(root, "a/b/c.ts")).text();
    expect(buf).toBe("// hi");
  });

  it("rejects path traversal", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createWriteFileTool({ rootDir: root });
    await expect(t.execute({ path: "../../evil.txt", content: "x" })).rejects.toThrow("escapes");
  });

  it("has requireApproval set", () => {
    const root = "/tmp";
    const t = createWriteFileTool({ rootDir: root });
    expect(t.requireApproval).toBe(true);
  });
});

// ---- listDir ----

describe("createListDirTool", () => {
  it("lists files in the root", async () => {
    const root = await tempDir();
    dirs.push(root);
    await writeFile(join(root, "a.ts"), "");
    await writeFile(join(root, "b.ts"), "");
    const t = createListDirTool({ rootDir: root });
    const result = await t.execute({ path: ".", recursive: false });
    expect(result).toContain("a.ts");
    expect(result).toContain("b.ts");
  });

  it("returns (empty) for empty directory", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createListDirTool({ rootDir: root });
    const result = await t.execute({ path: ".", recursive: false });
    expect(result).toBe("(empty)");
  });

  it("includes subdirectory files when recursive:true", async () => {
    const root = await tempDir();
    dirs.push(root);
    await mkdir(join(root, "sub"));
    await writeFile(join(root, "top.ts"), "");
    await writeFile(join(root, "sub", "nested.ts"), "");
    const t = createListDirTool({ rootDir: root });
    const result = await t.execute({ path: ".", recursive: true });
    expect(result).toContain("nested.ts");
    expect(result).toContain("top.ts");
  });

  it("rejects traversal outside root", async () => {
    const root = await tempDir();
    dirs.push(root);
    const t = createListDirTool({ rootDir: root });
    await expect(t.execute({ path: "..", recursive: false })).rejects.toThrow("escapes");
  });
});

// ---- statFile ----

describe("createStatTool", () => {
  it("returns file metadata", async () => {
    const root = await tempDir();
    dirs.push(root);
    await writeFile(join(root, "x.txt"), "hello");
    const t = createStatTool({ rootDir: root });
    const result = JSON.parse(await t.execute({ path: "x.txt" }));
    expect(result.type).toBe("file");
    expect(result.size).toBe(5);
    expect(result.modified).toBeTruthy();
  });

  it("identifies directories", async () => {
    const root = await tempDir();
    dirs.push(root);
    await mkdir(join(root, "sub"));
    const t = createStatTool({ rootDir: root });
    const result = JSON.parse(await t.execute({ path: "sub" }));
    expect(result.type).toBe("directory");
  });
});

// ---- bundle ----

describe("createFilesystemTools", () => {
  it("returns all four tools", () => {
    const tools = createFilesystemTools({ rootDir: "/tmp" });
    expect(tools.readFile.name).toBe("readFile");
    expect(tools.writeFile.name).toBe("writeFile");
    expect(tools.listDir.name).toBe("listDir");
    expect(tools.statFile.name).toBe("statFile");
  });
});

// ---- symlink containment ----

describe("symlink containment", () => {
  /** A root holding `link` → a directory outside it that has `secret.txt`. */
  async function rootWithEscapeLink(): Promise<{ root: string; outside: string }> {
    const root = await tempDir();
    const outside = await tempDir();
    dirs.push(root, outside);
    await writeFile(join(outside, "secret.txt"), "top secret");
    await symlink(outside, join(root, "link"));
    return { root, outside };
  }

  it("refuses to read through a symlinked directory that leaves the root", async () => {
    const { root } = await rootWithEscapeLink();
    const t = createReadFileTool({ rootDir: root });
    await expect(t.execute({ path: "link/secret.txt" })).rejects.toThrow("escapes");
  });

  it("refuses to read a file symlink that points outside the root", async () => {
    const { root, outside } = await rootWithEscapeLink();
    await symlink(join(outside, "secret.txt"), join(root, "secret-link"));
    const t = createReadFileTool({ rootDir: root });
    await expect(t.execute({ path: "secret-link" })).rejects.toThrow("escapes");
  });

  it("refuses to create a new file under a symlinked directory outside the root", async () => {
    const { root, outside } = await rootWithEscapeLink();
    const t = createWriteFileTool({ rootDir: root });
    await expect(t.execute({ path: "link/new/dir/planted.txt", content: "x" })).rejects.toThrow(
      "escapes",
    );
    expect(existsSync(join(outside, "new"))).toBe(false);
  });

  it("refuses to write through a dangling symlink", async () => {
    const { root, outside } = await rootWithEscapeLink();
    await symlink(join(outside, "not-yet.txt"), join(root, "dangling"));
    const t = createWriteFileTool({ rootDir: root });
    await expect(t.execute({ path: "dangling", content: "x" })).rejects.toThrow("dangling");
    expect(existsSync(join(outside, "not-yet.txt"))).toBe(false);
  });

  it("refuses to list or stat through an escaping symlink", async () => {
    const { root } = await rootWithEscapeLink();
    await expect(
      createListDirTool({ rootDir: root }).execute({ path: "link", recursive: false }),
    ).rejects.toThrow("escapes");
    await expect(
      createStatTool({ rootDir: root }).execute({ path: "link/secret.txt" }),
    ).rejects.toThrow("escapes");
  });

  it("does not descend into an escaping symlink when listing recursively", async () => {
    const { root } = await rootWithEscapeLink();
    const result = await createListDirTool({ rootDir: root }).execute({
      path: ".",
      recursive: true,
    });
    expect(result).not.toContain("secret.txt");
  });

  it("allows symlinks that stay inside the root and a root that is itself a symlink", async () => {
    const real = await tempDir();
    const parent = await tempDir();
    dirs.push(real, parent);
    await mkdir(join(real, "data"));
    await writeFile(join(real, "data", "a.txt"), "inside");
    await symlink(join(real, "data"), join(real, "alias"));
    const rootLink = join(parent, "root-link");
    await symlink(real, rootLink);
    const t = createReadFileTool({ rootDir: rootLink });
    expect(await t.execute({ path: "alias/a.txt" })).toBe("inside");
    const w = createWriteFileTool({ rootDir: rootLink });
    await w.execute({ path: "fresh/b.txt", content: "new" });
    expect(await Bun.file(join(real, "fresh", "b.txt")).text()).toBe("new");
  });

  it("accepts names that merely start with two dots", async () => {
    const root = await tempDir();
    dirs.push(root);
    await writeFile(join(root, "..notes"), "ok");
    expect(await createReadFileTool({ rootDir: root }).execute({ path: "..notes" })).toBe("ok");
  });
});
