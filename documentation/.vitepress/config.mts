import { defineConfig } from "vitepress";
import { slug } from "github-slugger";
import { sidebar } from "./sidebar.ts";

const REPO = "https://github.com/spilne/promin";

export default defineConfig({
  title: "Promin",
  description: "Durable workflows, agents and data pipelines for TypeScript. Built on Perfect.",
  base: "/promin/",
  cleanUrls: true,
  lastUpdated: true,
  srcExclude: ["README.md"],
  ignoreDeadLinks: "localhostLinks",
  // Source READMEs use GitHub heading links in their tables of contents.
  markdown: { anchor: { slugify: slug }, headers: { slugify: slug } },
  head: [["meta", { name: "theme-color", content: "#3c8772" }]],
  themeConfig: {
    nav: [
      { text: "Guide", link: "/docs/getting-started/" },
      { text: "Workflows", link: "/docs/workflow/overview" },
      { text: "Agents", link: "/docs/agent/quickstart" },
      { text: "Perfect", link: "https://spilne.github.io/perfect/" },
    ],
    sidebar,
    socialLinks: [{ icon: "github", link: REPO }],
    editLink: {
      pattern: `${REPO}/edit/main/documentation/:path`,
      text: "Edit this page on GitHub",
    },
    search: { provider: "local" },
    outline: { level: [2, 3] },
    footer: { message: "Promin — durable workflows and agents, built on Perfect." },
  },
});
