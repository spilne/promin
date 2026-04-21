# ---------------------------------------------------------------------------
# Shared image for coordinator + workers. Same pattern as order-fulfillment
# and video-pipeline — one base image, compose overrides `command` per service.
# ---------------------------------------------------------------------------

FROM oven/bun:1.3

WORKDIR /app

COPY package.json bun.lock ./
COPY packages packages
COPY examples examples
COPY documentation documentation

RUN bun install

WORKDIR /app/examples/docker/data-pipeline

CMD ["bun", "--conditions=@promin/source", "run", "coordinator.ts"]
