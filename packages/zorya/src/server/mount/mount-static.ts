// ---------------------------------------------------------------------------
// Dashboard UI: serves the compiled assets from `uiDir`, falling back to
// `index.html` for client-side routes. API-shaped paths never fall back.
// ---------------------------------------------------------------------------

import { jsonError, type Router } from "../router.ts";

export function mountStatic(params: { readonly router: Router; readonly uiDir: string }): void {
  const dir = params.uiDir;
  params.router.setStatic(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    if (/^\/(api|rpc|webhooks)(\/|$)/.test(url.pathname)) return jsonError(404, "not_found");
    const path = url.pathname === "/" ? "/index.html" : url.pathname;
    const file = Bun.file(`${dir}${path}`);
    if (await file.exists()) return new Response(file);
    const indexFile = Bun.file(`${dir}/index.html`);
    if (await indexFile.exists()) return new Response(indexFile);
    return new Response("Not Found", { status: 404 });
  });
}
