# Documentation

Promin uses Fumadocs/Next.js with checked-in MDX content in `content/docs`.
The static production site is exported to `.target/docs` under `/promin`.

```sh
bun run documentation:build         # regenerate embedded source content
bun run documentation:check         # fail if generated content is stale
bun run documentation:build:html    # regenerate, then build the static site
bun run docs:dev                    # regenerate, then start the dev server
bun run docs:serve                  # preview an existing static build
```

## Source-backed content

Embed an example or a package README using an MDX comment pair:

```mdx
{/* @embed examples/docs-workflow.ts */}

Generated content goes here.

{/* @end */}
```

Paths are relative to the repository root. Add `#name` to extract a region
bounded by `// >>> example: name` and `// <<< example` in a TypeScript file.
Without a region the entire file is included. TypeScript becomes a fenced
code block; Markdown is converted to safe MDX prose and its relative links
point back to the source repository.

Edit the original source, regenerate, and commit both source and MDX.
CI checks missing sources, missing regions, malformed markers and stale
generated content. It also requires each workspace package to have a README
and appear in the package map in `content/docs/index.mdx`.

`examples/docs-workflow.ts` is a self-contained executable example. Its test
runs through `bun run test:release`; repository typechecking also checks the
other examples. Some examples need Postgres, Redis or API credentials to run;
the docs build reads their source without executing those integrations.

## Deployment

Documentation PRs and main changes build and validate the site. Normal
GitHub Pages deployment happens after a successful tagged package release.
Use a manual Documentation workflow dispatch for a docs-only correction.
See [RELEASING.md](../RELEASING.md) for the full lifecycle.
