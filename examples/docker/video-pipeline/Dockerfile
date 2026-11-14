# ---------------------------------------------------------------------------
# Shared image for coordinator + workers + submit. Built from the monorepo
# root so we can `bun install` the workspace once and each container just
# flips its entry point. Not optimized for prod — just meant to be a
# reproducible way to run the example with `docker compose up`.
# ---------------------------------------------------------------------------

FROM oven/bun:1.3

WORKDIR /app

# Copy the complete workspace from the monorepo root build context.
COPY package.json bun.lock ./
COPY packages packages
COPY examples examples

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
