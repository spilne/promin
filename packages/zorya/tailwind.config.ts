import type { Config } from "tailwindcss";
import daisyui from "daisyui";

export default {
  content: ["./src/ui/**/*.{ts,tsx,html}"],
  theme: {
    extend: {},
  },
  plugins: [daisyui],
  daisyui: {
    themes: ["night"],
    logs: false,
  },
} satisfies Config;
