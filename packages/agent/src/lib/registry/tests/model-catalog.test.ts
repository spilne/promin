import { describe, it, expect } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InMemoryModelCatalog, type ModelCatalogItem } from "../model-catalog.ts";
import { createFileModelCatalog } from "../file-model-catalog.ts";
import type { LLMProvider } from "../../llm-provider.ts";

const stubLLM = (label: string): LLMProvider => ({
  chat: async () => ({ content: label, finishReason: "stop" }),
});

function item(
  provider: string,
  id: string,
  extra: Partial<ModelCatalogItem> = {},
): ModelCatalogItem {
  return {
    provider,
    id,
    llm: stubLLM(`${provider}:${id}`),
    ...extra,
  };
}

describe("InMemoryModelCatalog", () => {
  it("get/list returns the registered entries", () => {
    const a = item("anthropic", "claude-sonnet-4-6", { displayName: "Sonnet" });
    const b = item("openai", "gpt-4o", { costTier: "high" });
    const catalog = new InMemoryModelCatalog([a, b]);

    expect(catalog.get("anthropic", "claude-sonnet-4-6")).toBe(a);
    expect(catalog.get("openai", "gpt-4o")).toBe(b);
    expect(catalog.get("anthropic", "missing")).toBeUndefined();
    expect(catalog.list()).toEqual([a, b]);
  });

  it("serialize() strips the runtime llm field", () => {
    const catalog = new InMemoryModelCatalog([
      item("anthropic", "claude-sonnet-4-6", {
        displayName: "Sonnet",
        contextLimit: 200_000,
        capabilities: ["chat", "tools", "vision"],
        costTier: "mid",
      }),
    ]);

    const serialized = catalog.serialize();
    expect(serialized).toHaveLength(1);
    expect(serialized[0]).toEqual({
      provider: "anthropic",
      id: "claude-sonnet-4-6",
      displayName: "Sonnet",
      contextLimit: 200_000,
      capabilities: ["chat", "tools", "vision"],
      costTier: "mid",
    });
    // Round-trip must be JSON-safe — no runtime closure leaked.
    expect(() => JSON.stringify(serialized)).not.toThrow();
    expect(JSON.parse(JSON.stringify(serialized))[0]).toEqual(serialized[0]!);
    expect("llm" in (serialized[0] as Record<string, unknown>)).toBe(false);
  });

  it("rejects duplicate (provider, id) pairs at construction time", () => {
    expect(
      () =>
        new InMemoryModelCatalog([
          item("anthropic", "claude-sonnet-4-6"),
          item("anthropic", "claude-sonnet-4-6"),
        ]),
    ).toThrow(/duplicate entry/i);
  });

  it("preserves insertion order (deterministic UI dropdowns)", () => {
    const catalog = new InMemoryModelCatalog([
      item("z-provider", "z-model"),
      item("a-provider", "a-model"),
      item("m-provider", "m-model"),
    ]);
    expect(catalog.list().map((i) => i.id)).toEqual(["z-model", "a-model", "m-model"]);
  });

  it("empty catalog round-trips cleanly", () => {
    const catalog = new InMemoryModelCatalog();
    expect(catalog.list()).toEqual([]);
    expect(catalog.serialize()).toEqual([]);
    expect(catalog.get("any", "thing")).toBeUndefined();
  });
});

describe("createFileModelCatalog", () => {
  async function withDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
    const dir = await mkdtemp(join(tmpdir(), "model-catalog-"));
    try {
      return await fn(dir);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }

  // The file scanner imports model files directly. Tests write actual .ts
  // files into a temp dir and then construct the catalog over them. The
  // import URL is cache-busted so subsequent reloads see the new content.
  function writeModelFile(dir: string, name: string, body: string): Promise<void> {
    return writeFile(join(dir, name), body, "utf8");
  }

  function modelFileBody(provider: string, id: string, extra: string = ""): string {
    return `
const item = {
  provider: ${JSON.stringify(provider)},
  id: ${JSON.stringify(id)},
  ${extra}
  llm: { chat: async () => ({ content: ${JSON.stringify(`${provider}:${id}`)}, finishReason: "stop" }) },
};
export default item;
`;
  }

  it("loads default-exported model files at startup", async () => {
    await withDir(async (dir) => {
      await writeModelFile(dir, "sonnet.ts", modelFileBody("anthropic", "claude-sonnet-4-6"));
      await writeModelFile(dir, "haiku.ts", modelFileBody("anthropic", "claude-haiku-4-5"));

      const catalog = await createFileModelCatalog({ dir, watch: false });
      expect(
        catalog
          .list()
          .map((i) => `${i.provider}:${i.id}`)
          .sort(),
      ).toEqual(["anthropic:claude-haiku-4-5", "anthropic:claude-sonnet-4-6"]);
      expect(catalog.get("anthropic", "claude-sonnet-4-6")).toBeDefined();
      catalog.close();
    });
  });

  it("scans subdirectories when recursive (default)", async () => {
    await withDir(async (dir) => {
      await mkdir(join(dir, "anthropic"));
      await writeModelFile(
        join(dir, "anthropic"),
        "sonnet.ts",
        modelFileBody("anthropic", "claude-sonnet-4-6"),
      );
      await writeModelFile(dir, "openai.ts", modelFileBody("openai", "gpt-4o"));

      const catalog = await createFileModelCatalog({ dir, watch: false });
      const ids = catalog
        .list()
        .map((i) => `${i.provider}:${i.id}`)
        .sort();
      expect(ids).toEqual(["anthropic:claude-sonnet-4-6", "openai:gpt-4o"]);
      catalog.close();
    });
  });

  it("calls onError for malformed files (no default export, missing llm)", async () => {
    await withDir(async (dir) => {
      await writeModelFile(dir, "no-default.ts", `export const x = 1;`);
      await writeModelFile(
        dir,
        "no-llm.ts",
        `export default { provider: "anthropic", id: "claude-haiku-4-5" };`,
      );
      await writeModelFile(dir, "ok.ts", modelFileBody("anthropic", "claude-sonnet-4-6"));

      const errors: Array<{ file: string; message: string }> = [];
      const catalog = await createFileModelCatalog({
        dir,
        watch: false,
        onError: (file, err) => {
          errors.push({ file, message: err instanceof Error ? err.message : String(err) });
        },
      });
      expect(errors.length).toBeGreaterThanOrEqual(2);
      // Good file still loads despite siblings failing.
      expect(catalog.get("anthropic", "claude-sonnet-4-6")).toBeDefined();
      catalog.close();
    });
  });

  it("serialize() output matches InMemoryModelCatalog shape (cross-impl wire compat)", async () => {
    await withDir(async (dir) => {
      await writeModelFile(
        dir,
        "sonnet.ts",
        modelFileBody(
          "anthropic",
          "claude-sonnet-4-6",
          `displayName: "Claude Sonnet 4.6", contextLimit: 200000, capabilities: ["chat","tools"], costTier: "mid",`,
        ),
      );

      const fileCatalog = await createFileModelCatalog({ dir, watch: false });
      const memCatalog = new InMemoryModelCatalog([
        item("anthropic", "claude-sonnet-4-6", {
          displayName: "Claude Sonnet 4.6",
          contextLimit: 200_000,
          capabilities: ["chat", "tools"],
          costTier: "mid",
        }),
      ]);

      expect(fileCatalog.serialize()).toEqual(memCatalog.serialize());
      fileCatalog.close();
    });
  });
});
