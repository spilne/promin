import { tool } from "../../lib/index.ts";
import { z } from "zod";

export default tool({
  name: "fetch-url",
  description: "Fetch the text content of a URL",
  parameters: z.object({ url: z.string().url() }),
  execute: async ({ url }) => {
    const resp = await fetch(url);
    return resp.text();
  },
});
