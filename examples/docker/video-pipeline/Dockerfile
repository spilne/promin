# ---------------------------------------------------------------------------
# Shared image for coordinator + workers + submit. Built from the monorepo
# root so we can `bun install` the workspace once and each container just
# flips its entry point. Not optimized for prod — just meant to be a
# reproducible way to run the example with `docker compose up`.
# ---------------------------------------------------------------------------

FROM oven/bun:1.1

WORKDIR /app

# Copy the monorepo in. Build context in docker-compose.yml is set to the
# monorepo root so this works.
COPY package.json bun.lock ./
COPY packages packages
COPY examples examples

RUN bun install --frozen-lockfile

WORKDIR /app/examples/docker/video-pipeline

# Default is the coordinator; each compose service overrides `command`
# with its own entry point. Using bun directly (no tsc build) keeps
# iteration fast for a demo.
CMD ["bun", "run", "coordinator.ts"]
