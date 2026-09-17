// ---------------------------------------------------------------------------
// Router — minimal pattern-matching router for Bun.serve.
//
// Path patterns support colon-prefixed parameters: "/api/runs/:id". The
// matched params are passed to the handler alongside the Request.
// ---------------------------------------------------------------------------

export type Handler = (
  req: Request,
  params: Record<string, string>,
) => Response | Promise<Response>;

interface CompiledRoute {
  method: string;
  regex: RegExp;
  keys: string[];
  handler: Handler;
}

export class Router {
  private readonly routes: CompiledRoute[] = [];
  private staticHandler?: Handler;
  private notFoundHandler: Handler = () => jsonError(404, "not_found");

  add(method: string, pattern: string, handler: Handler): this {
    const keys: string[] = [];
    const regexSrc = pattern.replace(/:([^/]+)/g, (_m, k) => {
      keys.push(k);
      return "([^/]+)";
    });
    const regex = new RegExp(`^${regexSrc}$`);
    this.routes.push({ method: method.toUpperCase(), regex, keys, handler });
    return this;
  }

  get(pattern: string, handler: Handler): this {
    return this.add("GET", pattern, handler);
  }
  post(pattern: string, handler: Handler): this {
    return this.add("POST", pattern, handler);
  }
  delete(pattern: string, handler: Handler): this {
    return this.add("DELETE", pattern, handler);
  }
  patch(pattern: string, handler: Handler): this {
    return this.add("PATCH", pattern, handler);
  }

  /** Fallback handler when no route matches (e.g. to serve static UI). */
  setStatic(handler: Handler): this {
    this.staticHandler = handler;
    return this;
  }

  setNotFound(handler: Handler): this {
    this.notFoundHandler = handler;
    return this;
  }

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    for (const r of this.routes) {
      if (r.method !== req.method) continue;
      const m = r.regex.exec(path);
      if (!m) continue;
      const params: Record<string, string> = {};
      r.keys.forEach((k, i) => {
        params[k] = decodeURIComponent(m[i + 1]!);
      });
      return r.handler(req, params);
    }
    if (this.staticHandler) return this.staticHandler(req, {});
    return this.notFoundHandler(req, {});
  }
}

export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export function jsonError(status: number, error: string, message?: string): Response {
  return json(status, { error, message });
}

export async function readJson<T>(req: Request): Promise<T | null> {
  const ct = req.headers.get("content-type") ?? "";
  if (!ct.includes("application/json")) return null;
  try {
    return (await req.json()) as T;
  } catch {
    return null;
  }
}
