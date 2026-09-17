// Tool with optional input. Uses Intl so any IANA timezone string works
// ("America/New_York", "Europe/Kyiv", etc.).
import { tool } from "@promin/agent";
import { z } from "zod";

export default tool({
  name: "currentTime",
  description:
    "Get the current date and time, optionally in a specific IANA timezone (e.g. 'America/New_York'). Defaults to UTC.",
  parameters: z.object({
    timezone: z
      .string()
      .optional()
      .describe("IANA timezone like 'America/New_York' or 'Europe/Kyiv'. Defaults to UTC."),
  }),
  execute: async ({ timezone }) => {
    const tz = timezone && timezone.length > 0 ? timezone : "UTC";
    const now = new Date();
    let formatted: string;
    try {
      formatted = new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        dateStyle: "full",
        timeStyle: "long",
      }).format(now);
    } catch {
      // Invalid timezone — fall back to UTC and tell the model.
      formatted = `${now.toISOString()} (invalid timezone ${tz}, used UTC)`;
    }
    return { iso: now.toISOString(), formatted, timezone: tz };
  },
});
