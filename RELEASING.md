# Releasing Promin

All public `@promin/*` packages share one version. Nx Release prepares a
reviewable commit containing package versions, internal dependency ranges,
the Bun lockfile and `CHANGELOG.md`, then tags that commit. Publication runs
only after a `vX.Y.Z` tag is pushed.

## Setup

Use Bun 1.4.4 and Node 22.23.5, then run `bun install --frozen-lockfile`.
Docker must be available for Postgres/Redis integration tests. Configure the
repository's `NPM_TOKEN` secret with publish access to `@promin`, and select
GitHub Actions as the GitHub Pages source.

The publisher uses Bun and passes the token through `NPM_CONFIG_TOKEN`.
It does not use npm trusted publishing or claim provenance.

## Prepare a release

Start from a clean, up-to-date `main` checkout:

```sh
git pull --ff-only
bun run release:dry                    # preview a patch bump
VERSION=minor bun run release:dry      # preview a minor bump
VERSION=0.2.0 bun run release          # prepare an explicit stable release
```

`bun run release` defaults to a patch bump; `VERSION` accepts `patch`,
`minor`, `major` or an explicit stable version. Prereleases are unsupported.
The bump starts after the newest shared package version or stable Git tag,
so old manifests cannot accidentally reuse an existing release number.

A real preparation runs CI, builds and checks package artifacts, and builds
the docs before changing versions. It then commits and tags the versioned
files without pushing or publishing. A dry run previews the Nx changes
without changing files, commits or tags and skips validation builds.

Review the resulting commit and changelog. The command prints the exact
atomic push to publish the release, for example:

```sh
git push --atomic origin main v0.2.0
```

Use descriptive `feat:`, `fix:` and `perf:` commits so the generated changelog
is useful. If branch protection requires a PR for the release commit, merge
that exact commit without squashing or rebasing before pushing its tag.

## After the tag is pushed

The release workflow verifies the tag matches HEAD and all public package
versions and internal dependency ranges. It runs CI and end-to-end tests,
builds packages and dashboard assets, checks every packed manifest and
entrypoint, checks Node consumer types, and migrates a real Postgres database
using the SQL in the extracted package. It builds the documentation before
publishing any packages.

Packages publish in dependency order from the validated tarballs. Bun
resolves `workspace:` and `catalog:` ranges when packing; the artifact check
rejects any unresolved protocol in dependencies, development dependencies,
peer dependencies or optional dependencies. Private workspaces never publish.

The workflow then creates a GitHub Release and deploys that release's docs.
Already-published versions and an existing GitHub Release are preserved when
a failed workflow is retried. Registry authentication/server failures stop
publication rather than being treated as missing packages. Retry the same
tag with GitHub Actions' **Re-run failed jobs**; never move a published tag.

To inspect artifacts locally without publishing or credentials:

```sh
bun run release:check
bun run test:packages    # requires Docker; uses the builds above
```

## Documentation

Fumadocs/Next.js renders the checked-in MDX pages as a static site in
`.target/docs`, served under `/promin`. Source READMEs and executable examples
are embedded by `documentation/build.ts`; see
[documentation/README.md](documentation/README.md) for authoring commands.

The Documentation workflow checks generated content and builds the site for
relevant PR and main changes. Normal deployment happens after publication,
so the live docs describe the released packages. A manual Documentation
workflow dispatch is the explicit escape hatch for a docs-only correction.
Both deployment paths share the `pages` concurrency group.
