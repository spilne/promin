import { describe, expect, it } from "bun:test";
import { isPublicAddress } from "../address-ranges.ts";
import {
  OutboundRequestError,
  outboundFetch,
  type OutboundFetchTransport,
} from "../outbound-fetch.ts";
import { checkOutboundUrl } from "../outbound-policy.ts";

/** Fake DNS: every `*.example` host resolves to a public address. */
const PUBLIC_DNS = {
  resolveHost: async (host: string) => {
    if (host.endsWith(".example")) return ["93.184.216.34"];
    throw new Error("NXDOMAIN");
  },
};

interface Sent {
  url: string;
  host: string | null;
  authorization: string | null;
  serverName: string | undefined;
}

function recordingTransport(answer: (sent: Sent) => Response): {
  transport: OutboundFetchTransport;
  sent: Sent[];
} {
  const sent: Sent[] = [];
  return {
    sent,
    transport: async (url, init) => {
      const headers = new Headers(init.headers);
      const row = {
        url,
        host: headers.get("host"),
        authorization: headers.get("authorization"),
        serverName: init.tls?.serverName,
      };
      sent.push(row);
      return answer(row);
    },
  };
}

async function refusal(promise: Promise<unknown>): Promise<OutboundRequestError> {
  const err = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(OutboundRequestError);
  return err as OutboundRequestError;
}

describe("checkOutboundUrl", () => {
  it("refuses non-http schemes, embedded credentials and internal names", async () => {
    for (const raw of [
      "file:///etc/passwd",
      "ftp://a.example/",
      "https://user:pw@a.example/",
      "http://localhost:8080/",
      "http://metadata.google.internal/",
      "http://printer.local/",
    ]) {
      const check = await checkOutboundUrl({ raw, policy: PUBLIC_DNS });
      expect([raw, check.ok]).toEqual([raw, false]);
    }
  });

  it("refuses literal non-public addresses in every spelling", async () => {
    for (const raw of [
      "http://169.254.169.254/latest/meta-data/",
      "http://2130706433/",
      "http://0x7f.1/",
      "http://[::1]/",
      "http://[::ffff:127.0.0.1]/",
      "http://[::127.0.0.1]/",
      "http://[2002:7f00:1::]/",
      "http://[fd00::1]/",
    ]) {
      const check = await checkOutboundUrl({ raw });
      expect([raw, check.ok]).toEqual([raw, false]);
    }
  });

  it("refuses a hostname with any non-public address", async () => {
    const check = await checkOutboundUrl({
      raw: "https://mixed.example/",
      policy: { resolveHost: async () => ["93.184.216.34", "10.0.0.5"] },
    });
    expect(check).toMatchObject({ ok: false, error: "host_not_allowed" });
  });

  it("returns the vetted addresses and honors allowedHosts", async () => {
    const ok = await checkOutboundUrl({
      raw: "https://a.trusted.example/x",
      policy: { ...PUBLIC_DNS, allowedHosts: ["*.trusted.example"] },
    });
    expect(ok).toMatchObject({
      ok: true,
      hostname: "a.trusted.example",
      addresses: ["93.184.216.34"],
    });
    const narrow = await checkOutboundUrl({
      raw: "https://a.example/",
      policy: { ...PUBLIC_DNS, allowedHosts: ["*.trusted.example"] },
    });
    expect(narrow).toMatchObject({ ok: false, error: "host_not_allowed" });
  });

  it("classifies addresses", () => {
    expect(isPublicAddress("93.184.216.34")).toBe(true);
    expect(isPublicAddress("2606:2800:220:1::1")).toBe(true);
    for (const a of [
      "0.0.0.0",
      "100.64.0.1",
      "172.16.0.1",
      "192.168.1.1",
      "224.0.0.1",
      "fe80::1",
      "fec0::1",
    ]) {
      expect([a, isPublicAddress(a)]).toEqual([a, false]);
    }
  });
});

describe("outboundFetch", () => {
  it("connects to the vetted IP with the original Host header and TLS name", async () => {
    const { transport, sent } = recordingTransport(() => new Response("ok"));
    const { response, url } = await outboundFetch({
      url: "https://docs.example/a?b=1",
      policy: PUBLIC_DNS,
      fetch: transport,
    });
    expect(await response.text()).toBe("ok");
    expect(url).toBe("https://docs.example/a?b=1");
    expect(sent).toEqual([
      {
        url: "https://93.184.216.34/a?b=1",
        host: "docs.example",
        authorization: null,
        serverName: "docs.example",
      },
    ]);
  });

  it("resolves once per hop, so a rebinding answer is never connected to", async () => {
    let lookups = 0;
    const rebinding = {
      resolveHost: async () => (++lookups === 1 ? ["93.184.216.34"] : ["169.254.169.254"]),
    };
    const { transport, sent } = recordingTransport(() => new Response("ok"));
    await outboundFetch({ url: "http://rebind.example/", policy: rebinding, fetch: transport });
    expect(lookups).toBe(1);
    expect(sent.map((s) => s.url)).toEqual(["http://93.184.216.34/"]);
  });

  it("checks every redirect hop and refuses one into a private network", async () => {
    const { transport, sent } = recordingTransport(
      () => new Response(null, { status: 302, headers: { location: "http://169.254.169.254/" } }),
    );
    const err = await refusal(
      outboundFetch({ url: "https://docs.example/", policy: PUBLIC_DNS, fetch: transport }),
    );
    expect(err.code).toBe("host_not_allowed");
    expect(sent).toHaveLength(1);
  });

  it("drops credentials when a redirect changes origin", async () => {
    const { transport, sent } = recordingTransport((s) =>
      s.host === "a.example"
        ? new Response(null, { status: 301, headers: { location: "https://b.example/next" } })
        : new Response("done"),
    );
    await outboundFetch({
      url: "https://a.example/",
      init: { headers: { authorization: "Bearer t" } },
      policy: PUBLIC_DNS,
      fetch: transport,
    });
    expect(sent.map((s) => [s.host, s.authorization])).toEqual([
      ["a.example", "Bearer t"],
      ["b.example", null],
    ]);
  });

  it("caps redirects and does not follow them for non-GET requests", async () => {
    const loop = recordingTransport(
      () => new Response(null, { status: 302, headers: { location: "/again" } }),
    );
    const err = await refusal(
      outboundFetch({
        url: "https://a.example/",
        policy: PUBLIC_DNS,
        maxRedirects: 2,
        fetch: loop.transport,
      }),
    );
    expect(err.code).toBe("too_many_redirects");
    expect(loop.sent).toHaveLength(3);

    const post = recordingTransport(
      () => new Response(null, { status: 307, headers: { location: "/elsewhere" } }),
    );
    const { response } = await outboundFetch({
      url: "https://a.example/",
      init: { method: "POST", body: "{}" },
      policy: PUBLIC_DNS,
      fetch: post.transport,
    });
    expect(response.status).toBe(307);
    expect(post.sent).toHaveLength(1);
  });

  it("pins the real connection: the name never reaches the system resolver", async () => {
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: (req) =>
        new Response(`host=${req.headers.get("host")} path=${new URL(req.url).pathname}`),
    });
    try {
      // `pinned.invalid` cannot resolve through DNS; reaching the server
      // proves the connection went to the address the policy vetted.
      const { response } = await outboundFetch({
        url: `http://pinned.invalid:${server.port}/hello`,
        policy: { allowPrivateNetworks: true, resolveHost: async () => ["127.0.0.1"] },
      });
      expect(await response.text()).toBe(`host=pinned.invalid:${server.port} path=/hello`);
    } finally {
      await server.stop(true);
    }
  });
});
