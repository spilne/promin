import { tool } from "../../lib/index.ts";
import { z } from "zod";

const DuckDuckGoResponseSchema = z.object({
  AbstractText: z.string().optional(),
  RelatedTopics: z.array(z.object({ Text: z.string().optional() })).optional(),
});

export default tool({
  name: "web-search",
  description: "Search the web using DuckDuckGo and return results",
  parameters: z.object({ query: z.string() }),
  execute: async ({ query }) => {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const resp = await fetch(url);
    const data = DuckDuckGoResponseSchema.parse(await resp.json());
    const results = [
      data.AbstractText && `**Summary:** ${data.AbstractText}`,
      ...(data.RelatedTopics ?? []).slice(0, 5).map((t) => t.Text),
    ].filter(Boolean);
    return results.length ? results.join("\n\n") : "No results found.";
  },
});
