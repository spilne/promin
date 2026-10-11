import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { createFetchUrlTool, fetchUrl } from "../web-tools.ts";

const PUBLIC_DNS = { resolveHost: async () => ["93.184.216.34"] };

describe("createFetchUrlTool", () => {
  it("requires approval by default, including the prebuilt tool", () => {
    expect(fetchUrl.requireApproval).toBe(true);
    expect(createFetchUrlTool().requireApproval).toBe(true);
    expect(createFetchUrlTool({ requireApproval: false }).requireApproval).toBe(false);
  });

  it("refuses cloud metadata and loopback targets without sending anything", async () => {
    let sent = 0;
    const tool = createFetchUrlTool({
      fetch: async () => {
        sent += 1;
        return new Response("secret");
      },
    });
    for (const url of [
      "http://169.254.169.254/latest/meta-data/",
      "http://127.0.0.1:6379/",
      "http://localhost/",
      "http://[::1]/",
    ]) {
      expect(await tool.execute({ url })).toStartWith("Refused:");
    }
    expect(sent).toBe(0);
  });

  it("refuses a redirect into a private network", async () => {
    const tool = createFetchUrlTool({
      policy: PUBLIC_DNS,
      fetch: async (url) =>
        url.includes("93.184.216.34")
          ? new Response(null, { status: 302, headers: { location: "http://10.0.0.1/admin" } })
          : new Response("internal"),
    });
    expect(await tool.execute({ url: "https://a.example/" })).toStartWith("Refused:");
  });

  it("connects to the vetted address", async () => {
    const seen: string[] = [];
    const tool = createFetchUrlTool({
      policy: PUBLIC_DNS,
      fetch: async (url) => {
        seen.push(url);
        return new Response("hello", { headers: { "content-type": "text/plain" } });
      },
    });
    expect(await tool.execute({ url: "https://a.example/page" })).toBe("hello");
    expect(seen).toEqual(["https://93.184.216.34/page"]);
  });

  it("reports a non-2xx status instead of returning the error body as content", async () => {
    const tool = createFetchUrlTool({
      policy: PUBLIC_DNS,
      fetch: async () => new Response("not here", { status: 404, statusText: "Not Found" }),
    });
    expect(await tool.execute({ url: "https://a.example/missing" })).toBe(
      "Error: GET https://a.example/missing answered HTTP 404 Not Found.",
    );
  });

  it("caps the body and refuses binary content", async () => {
    const big = createFetchUrlTool({
      policy: PUBLIC_DNS,
      maxBytes: 10,
      fetch: async () =>
        new Response("x".repeat(1_000), { headers: { "content-type": "text/plain" } }),
    });
    expect(await big.execute({ url: "https://a.example/" })).toBe(
      "xxxxxxxxxx\n[…response truncated at 10 bytes]",
    );
    const binary = createFetchUrlTool({
      policy: PUBLIC_DNS,
      fetch: async () => new Response("\u0000", { headers: { "content-type": "image/png" } }),
    });
    expect(await binary.execute({ url: "https://a.example/x.png" })).toStartWith("Error:");
  });

  it("times out through the injected clock", async () => {
    const clock = FakeWallClock.create(0);
    let connected = false;
    const tool = createFetchUrlTool({
      policy: PUBLIC_DNS,
      timeoutMs: 1_000,
      clock,
      fetch: (_url, init) =>
        new Promise((_resolve, reject) => {
          connected = true;
          init.signal?.addEventListener("abort", () => reject(new Error("aborted")));
        }),
    });
    const done = tool.execute({ url: "https://slow.example/" });
    for (let i = 0; i < 1_000 && !connected; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    clock.advance(1_000);
    expect(await done).toBe("Error: https://slow.example/ did not answer within 1000 ms.");
  });
});
