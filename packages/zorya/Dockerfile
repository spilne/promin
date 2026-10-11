# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Zorya — self-hostable workflow dashboard (demo server)
#
# Installs the monorepo, bundles the dashboard UI and runs the demo server
# from source under Bun. The demo discovers its workflows, tools, agents,
# skills and fragments by scanning folders under packages/zorya/examples at
# boot, so it runs from the source tree rather than a compiled binary.
#
# Build:
#   docker build -f packages/zorya/Dockerfile -t zorya:latest .
# Run:
#   docker run --rm -p 4100:4100 -v zorya-data:/data zorya:latest
# ---------------------------------------------------------------------------

FROM oven/bun:1.3.12-alpine

WORKDIR /app

# Copy repo metadata + all packages (bun.lock references every workspace).
# Dockerfile.dockerignore strips node_modules / dist / .nx so the context
# stays small.
COPY package.json bun.lock tsconfig.base.json tsconfig.json nx.json ./
COPY packages ./packages
COPY examples ./examples
COPY documentation ./documentation

RUN bun install --frozen-lockfile

# Build the UI (outputs packages/zorya/dist/public/).
RUN bun run build:ui

# The source tree stays root-owned and read-only; only /data is writable.
RUN mkdir -p /data && chown bun:bun /data

USER bun

ENV ZORYA_DB=/data/zorya.db \
    ZORYA_UI_DIR=/app/packages/zorya/dist/public \
    PORT=4100

EXPOSE 4100
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- --tries=1 --timeout=2 http://127.0.0.1:${PORT:-4100}/api/health \
      || exit 1

ENTRYPOINT ["bun", "--conditions=@promin/source", "packages/zorya/examples/demo.ts"]
