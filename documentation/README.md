# Documentation

[Read the published documentation](https://spilne.github.io/promin/).

Promin uses VitePress (the same framework as Perfect) with checked-in Markdown content in `docs`.
The static production site is exported to `.vitepress/dist` under `/promin`.

```sh
bun run documentation:build         # regenerate embedded source content
bun run documentation:check         # fail if generated content is stale
bun run documentation:build:html    # regenerate, then build the static site
bun run docs:dev                    # regenerate, then start the dev server
bun run docs:serve                  # preview an existing static build
```

## Source-backed content

Embed an example or a package README using an HTML comment pair:

```md
<!-- @embed examples/docs-workflow.ts -->

Generated content goes here.

<!-- @end -->
```

Paths are relative to the repository root. Add `#name` to extract a region
bounded by `// >>> example: name` and `// <<< example` in a TypeScript file.
Without a region the entire file is included. TypeScript becomes a fenced
code block; README prose is escaped for Vue templates and its relative links
point back to the source repository.

Edit the original source, regenerate, and commit both source and Markdown.
CI checks missing sources, missing regions, malformed markers and stale
generated content. It also requires each workspace package to have a README
and appear in the package map in `docs/index.md`.

`examples/docs-workflow.ts` is a self-contained executable example. Its test
runs through `bun run test:release`; repository typechecking also checks the
other examples. Some examples need Postgres, Redis or API credentials to run;
the docs build reads their source without executing those integrations.

The static build also checks sidebar coverage, local links and assets, heading
anchors, the GitHub Pages base path, and the local search index.

## Deployment

Documentation PRs and main changes build and validate the site. Normal
GitHub Pages deployment happens after a successful tagged package release.
Use a manual Documentation workflow dispatch for a docs-only correction.
See [RELEASING.md](../RELEASING.md) for the full lifecycle.
