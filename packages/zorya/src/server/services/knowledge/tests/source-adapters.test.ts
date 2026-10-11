import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtemp, mkdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { FakeWallClock } from "@promin/workflow";
import { KnowledgeSourceImportError } from "../errors.ts";
import {
  createFileKnowledgeSourceAdapter,
  createUrlKnowledgeSourceAdapter,
  htmlToText,
} from "../source-adapters.ts";

/** Public-looking resolution for every host, so the policy check passes without DNS. */
const PUBLIC_DNS = { resolveHost: async () => ["93.184.216.34"] };

function respond(params: {
  body?: string;
  status?: number;
  headers?: Record<string, string>;
}): Response {
  return new Response(params.body ?? "", {
    status: params.status ?? 200,
    headers: { "content-type": "text/plain", ...params.headers },
  });
}

async function importError(promise: Promise<unknown>): Promise<KnowledgeSourceImportError> {
  const err = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(KnowledgeSourceImportError);
  return err as KnowledgeSourceImportError;
}

describe("url source adapter", () => {
  it("fetches a text document with the caller's id, title and tags", async () => {
    const seen: string[] = [];
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      fetch: async (url) => {
        seen.push(url);
        return respond({ body: "Deploys need a rollback plan." });
      },
    });
    const [doc] = await adapter.load({
      config: { url: "https://docs.example.com/deploy", id: "deploy", tags: ["ops"] },
    });
    expect(seen).toEqual(["https://docs.example.com/deploy"]);
    expect(doc).toMatchObject({
      id: "deploy",
      text: "Deploys need a rollback plan.",
      title: "https://docs.example.com/deploy",
      uri: "https://docs.example.com/deploy",
      mimeType: "text/plain",
      tags: ["ops"],
    });
  });

  it("reduces HTML to paragraph text and takes the page title", async () => {
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      fetch: async () =>
        respond({
          headers: { "content-type": "text/html; charset=utf-8" },
          body:
            "<html><head><title>Guide &amp; FAQ</title><style>p{}</style></head>" +
            "<body><script>alert(1)</script><h1>Guide</h1><p>First&nbsp;step.</p><p>Second</p></body></html>",
        }),
    });
    const [doc] = await adapter.load({ config: { url: "https://docs.example.com/" } });
    expect(doc!.title).toBe("Guide & FAQ");
    expect(doc!.text).toBe("Guide\n\nFirst step.\n\nSecond");
  });

  it("refuses private and internal hosts before fetching", async () => {
    let fetched = false;
    const adapter = createUrlKnowledgeSourceAdapter({
      fetch: async () => {
        fetched = true;
        return respond({});
      },
    });
    for (const url of ["http://127.0.0.1/", "http://169.254.169.254/latest", "http://localhost/"]) {
      const err = await importError(adapter.load({ config: { url } }));
      expect(err.reason).toBe("invalid");
    }
    expect((await importError(adapter.load({ config: { url: "ftp://x/" } }))).reason).toBe(
      "invalid",
    );
    expect(fetched).toBe(false);
  });

  it("checks every redirect target against the policy", async () => {
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: {
        resolveHost: async (host) =>
          host === "evil.example.com" ? ["10.0.0.5"] : ["93.184.216.34"],
      },
      fetch: async (url) =>
        url.startsWith("https://docs.example.com")
          ? respond({ status: 302, headers: { location: "https://evil.example.com/meta" } })
          : respond({ body: "secret" }),
    });
    const err = await importError(adapter.load({ config: { url: "https://docs.example.com/a" } }));
    expect(err.reason).toBe("invalid");
  });

  it("follows allowed redirects up to the limit", async () => {
    let hops = 0;
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      maxRedirects: 2,
      fetch: async () => {
        hops += 1;
        return respond({ status: 301, headers: { location: `/hop-${hops}` } });
      },
    });
    const err = await importError(adapter.load({ config: { url: "https://docs.example.com/" } }));
    expect(err.reason).toBe("unavailable");
    expect(err.message).toContain("redirected more than 2 times");
    expect(hops).toBe(3);
  });

  it("reports an HTTP error as unavailable", async () => {
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      fetch: async () => respond({ status: 503 }),
    });
    const err = await importError(adapter.load({ config: { url: "https://docs.example.com/" } }));
    expect(err.reason).toBe("unavailable");
    expect(err.message).toContain("HTTP 503");
  });

  it("refuses binary content and bodies over the size cap", async () => {
    const binary = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      fetch: async () => respond({ headers: { "content-type": "application/pdf" }, body: "%PDF" }),
    });
    expect(
      (await importError(binary.load({ config: { url: "https://x.example.com/a.pdf" } }))).message,
    ).toContain("only text documents");

    const big = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      maxBytes: 10,
      fetch: async () => respond({ body: "x".repeat(11) }),
    });
    const err = await importError(big.load({ config: { url: "https://x.example.com/" } }));
    expect(err.reason).toBe("invalid");
  });

  it("gives up when the fetch outlives the deadline", async () => {
    const clock = FakeWallClock.create(0);
    let fetching = false;
    const adapter = createUrlKnowledgeSourceAdapter({
      policy: PUBLIC_DNS,
      clock,
      timeoutMs: 1_000,
      fetch: (_url, init) =>
        new Promise<Response>((_resolve, reject) => {
          fetching = true;
          init.signal?.addEventListener("abort", () => reject(new Error("aborted")));
        }),
    });
    const pending = adapter.load({ config: { url: "https://slow.example.com/" } });
    for (let i = 0; i < 100 && !fetching; i += 1) {
      await new Promise((r) => setTimeout(r, 0));
    }
    clock.advance(1_000);
    const err = await importError(pending);
    expect(err.reason).toBe("unavailable");
    expect(err.message).toContain("within 1000 ms");
    expect(clock.pendingCount()).toBe(0);
  });

  it("validates its config", async () => {
    const adapter = createUrlKnowledgeSourceAdapter({ policy: PUBLIC_DNS });
    for (const config of [undefined, {}, { url: "https://x.example.com", extra: 1 }]) {
      expect((await importError(adapter.load({ config }))).reason).toBe("invalid");
    }
  });
});

describe("file source adapter", () => {
  let dir: string;
  let root: string;

  beforeAll(async () => {
    dir = await mkdtemp(path.join(tmpdir(), "zorya-kb-"));
    root = path.join(dir, "root");
    await mkdir(path.join(root, "guides"), { recursive: true });
    await writeFile(path.join(root, "guides", "oncall.md"), "# On-call\n\nAck within 10 min.");
    await writeFile(path.join(root, "blob.bin"), new Uint8Array([1, 0, 2]));
    await writeFile(path.join(dir, "outside.txt"), "secret");
    await symlink(path.join(dir, "outside.txt"), path.join(root, "link.txt"));
  });

  afterAll(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("reads a text file under the root", async () => {
    const adapter = createFileKnowledgeSourceAdapter({ root });
    const [doc] = await adapter.load({ config: { path: "guides/oncall.md" } });
    expect(doc).toMatchObject({
      id: path.join("guides", "oncall.md"),
      text: "# On-call\n\nAck within 10 min.",
      mimeType: "text/markdown",
    });
  });

  it("refuses paths that escape the root, symlinks included", async () => {
    const adapter = createFileKnowledgeSourceAdapter({ root });
    for (const p of ["../outside.txt", "/etc/hosts", "link.txt", "missing.md"]) {
      expect((await importError(adapter.load({ config: { path: p } }))).reason).toBe("invalid");
    }
  });

  it("refuses binary files and files over the cap", async () => {
    expect(
      (
        await importError(
          createFileKnowledgeSourceAdapter({ root }).load({ config: { path: "blob.bin" } }),
        )
      ).message,
    ).toContain("not a text file");
    expect(
      (
        await importError(
          createFileKnowledgeSourceAdapter({ root, maxBytes: 5 }).load({
            config: { path: "guides/oncall.md" },
          }),
        )
      ).reason,
    ).toBe("invalid");
  });
});

describe("htmlToText", () => {
  it("decodes numeric entities and drops comments", () => {
    expect(htmlToText("<p>a&#39;b &#x263A;</p><!-- hidden -->")).toBe("a'b ☺");
  });
});
