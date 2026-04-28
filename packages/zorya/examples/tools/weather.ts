// Stubbed weather lookup — deterministic values per city so the demo
// doesn't need a real API.
import { tool } from "@promin/agent";
import { z } from "zod";

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}

export default tool({
  name: "weather",
  description: "Look up the current weather for a city.",
  parameters: z.object({ city: z.string().min(1) }),
  execute: async ({ city }) => {
    const conditions = ["sunny", "cloudy", "rainy", "windy"];
    const idx = Math.abs(hashString(city)) % conditions.length;
    return {
      tempF: 60 + (Math.abs(hashString(city)) % 25),
      conditions: conditions[idx]!,
    };
  },
});
