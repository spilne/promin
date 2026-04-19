# ---------------------------------------------------------------------------
# Shared image for coordinator + workers + submit. Built from the monorepo
# root so we can `bun install` the workspace once and each container just
# flips its entry point. Not optimized for prod — just meant to be a
# reproducible way to run the example with `docker compose up`.
# ---------------------------------------------------------------------------

FROM oven/bun:1.3

WORKDIR /app

# Copy the monorepo in. Build context in docker-compose.yml is set to the
# monorepo root so this works. `documentation/` is a workspace member in
# the root package.json — we only need its package.json files for `bun
# install` to resolve the workspace, not the actual docs site content.
# .dockerignore already strips its node_modules + .next build output.
COPY package.json bun.lock ./
COPY packages packages
COPY examples examples
COPY documentation documentation

# Not `--frozen-lockfile` — the monorepo's top-level lockfile includes
# deps for workspaces we've omitted or whose package.json isn't fully
# present (e.g. the docs generator), so bun would flag a diff and bail.
# For a demo image, let bun resolve fresh; deterministic installs for
# production would pin a stripped-down lockfile alongside the Dockerfile.
RUN bun install

WORKDIR /app/examples/docker/video-pipeline

# `--conditions=@promin/source` resolves `@promin/*` workspace imports
# to each package's src/index.ts instead of dist/index.js. Saves a tsc
# build step in the image and matches the monorepo's own test/bench
# scripts. If you switch this to dist/ you'd also need to drop
# `**/dist` from the repo-root .dockerignore + add `bun run build`
# before the CMD.
#
# Default is the coordinator; each compose service overrides `command`
# with its own entry point.
CMD ["bun", "--conditions=@promin/source", "run", "coordinator.ts"]
