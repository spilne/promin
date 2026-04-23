import { tool } from "../../lib/index.ts";
import { z } from "zod";

export default tool({
  name: "web-search",
  description: "Search the web using DuckDuckGo and return results",
  parameters: z.object({ query: z.string() }),
  execute: async ({ query }) => {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const resp = await fetch(url);
    const data = await resp.json();
    const results = [
      data.AbstractText && `**Summary:** ${data.AbstractText}`,
      ...(data.RelatedTopics || [])
        .slice(0, 5)
        .map((t: any) => t.Text)
        .filter(Boolean),
    ].filter(Boolean);
    return results.length ? results.join("\n\n") : "No results found.";
  },
});
