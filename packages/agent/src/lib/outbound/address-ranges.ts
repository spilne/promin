// ---------------------------------------------------------------------------
// Address classification for outbound requests: which IPs are globally
// routable. Everything else (loopback, private, link-local incl. the cloud
// metadata address, carrier-grade NAT, multicast, documentation, reserved)
// is refused by the default outbound policy.
// ---------------------------------------------------------------------------

import { isIP } from "node:net";

/**
 * True for a globally routable unicast address. Loopback, private,
 * carrier-grade NAT, link-local (incl. the cloud metadata address),
 * unspecified, multicast, documentation and reserved ranges are not.
 */
export function isPublicAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return isPublicV4(parseV4(address));
  if (family === 6) return isPublicV6(address);
  return false;
}

function parseV4(address: string): [number, number, number, number] {
  const p = address.split(".").map((n) => Number.parseInt(n, 10));
  return [p[0]!, p[1]!, p[2]!, p[3]!];
}

function isPublicV4([a, b, c]: [number, number, number, number]): boolean {
  if (a === 0 || a === 10 || a === 127) return false; // this-net, private, loopback
  if (a === 100 && b >= 64 && b <= 127) return false; // carrier-grade NAT
  if (a === 169 && b === 254) return false; // link-local, cloud metadata
  if (a === 172 && b >= 16 && b <= 31) return false; // private
  if (a === 192 && b === 168) return false; // private
  if (a === 192 && b === 0 && (c === 0 || c === 2)) return false; // IETF, TEST-NET-1
  if (a === 198 && (b === 18 || b === 19)) return false; // benchmarking
  if (a === 198 && b === 51 && c === 100) return false; // TEST-NET-2
  if (a === 203 && b === 0 && c === 113) return false; // TEST-NET-3
  if (a >= 224) return false; // multicast, reserved, broadcast
  return true;
}

function isPublicV6(address: string): boolean {
  const words = expandV6(address);
  if (!words) return false;
  if (words.every((w) => w === 0)) return false; // ::
  if (words.slice(0, 7).every((w) => w === 0) && words[7] === 1) return false; // ::1
  // IPv4-mapped (::ffff:a.b.c.d), IPv4-compatible (::a.b.c.d) and NAT64
  // (64:ff9b::a.b.c.d) carry a v4 address.
  const embedsV4 =
    (words.slice(0, 5).every((w) => w === 0) && words[5] === 0xffff) ||
    words.slice(0, 6).every((w) => w === 0) ||
    (words[0] === 0x64 && words[1] === 0xff9b && words.slice(2, 6).every((w) => w === 0));
  if (embedsV4) {
    return isPublicV4([words[6]! >> 8, words[6]! & 0xff, words[7]! >> 8, words[7]! & 0xff]);
  }
  const first = words[0]!;
  if ((first & 0xfe00) === 0xfc00) return false; // unique local fc00::/7
  if ((first & 0xffc0) === 0xfe80) return false; // link-local fe80::/10
  if ((first & 0xffc0) === 0xfec0) return false; // site-local fec0::/10
  if ((first & 0xff00) === 0xff00) return false; // multicast
  if (first === 0x2001 && words[1] === 0x0db8) return false; // documentation
  if (first === 0x2002) {
    // 6to4 carries a v4 address in words 1-2.
    return isPublicV4([words[1]! >> 8, words[1]! & 0xff, words[2]! >> 8, words[2]! & 0xff]);
  }
  return true;
}

/** Expand an IPv6 literal into its eight 16-bit words. */
function expandV6(address: string): number[] | null {
  let addr = address.toLowerCase();
  const zone = addr.indexOf("%");
  if (zone >= 0) addr = addr.slice(0, zone);
  // Trailing dotted-quad → two hex words.
  const lastColon = addr.lastIndexOf(":");
  const tail = addr.slice(lastColon + 1);
  if (tail.includes(".")) {
    if (isIP(tail) !== 4) return null;
    const [a, b, c, d] = parseV4(tail);
    addr = `${addr.slice(0, lastColon + 1)}${((a << 8) | b).toString(16)}:${((c << 8) | d).toString(16)}`;
  }
  const halves = addr.split("::");
  if (halves.length > 2) return null;
  const head = halves[0] ? halves[0].split(":") : [];
  const rest = halves.length === 2 && halves[1] ? halves[1].split(":") : [];
  const missing = 8 - head.length - rest.length;
  if (halves.length === 1 ? missing !== 0 : missing < 1) return null;
  const parts = [...head, ...Array<string>(halves.length === 2 ? missing : 0).fill("0"), ...rest];
  const words = parts.map((p) => Number.parseInt(p, 16));
  return words.some((w) => !Number.isFinite(w) || w < 0 || w > 0xffff) ? null : words;
}
