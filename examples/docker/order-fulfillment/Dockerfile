# ---------------------------------------------------------------------------
# Shared image for coordinator + workers. Same pattern as the video-pipeline
# example — one base image, compose overrides `command` per service.
# ---------------------------------------------------------------------------

FROM oven/bun:1.3

WORKDIR /app

COPY package.json bun.lock ./
COPY packages packages
COPY examples examples
COPY documentation documentation

RUN bun install

WORKDIR /app/examples/docker/order-fulfillment

CMD ["bun", "--conditions=@promin/source", "run", "coordinator.ts"]
