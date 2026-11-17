# Shared development image for the coordinator, workers, agent nodes, and
# smoke test. It intentionally runs workspace sources so this example always
# exercises the checkout it was built from.
FROM oven/bun:1.3.12-alpine

WORKDIR /app

COPY package.json bun.lock tsconfig.base.json tsconfig.json nx.json ./
COPY packages ./packages
COPY examples ./examples

RUN bun install --frozen-lockfile

WORKDIR /app/examples/docker/zorya-distributed

CMD ["bun", "--conditions=@promin/source", "run", "central.ts"]
