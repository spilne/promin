# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Zorya — self-hostable workflow dashboard
#
# Multi-stage build:
#   1. builder  — installs monorepo deps, bundles the UI, compiles the demo
#                 server into a single binary via `bun build --compile`.
#   2. runtime  — minimal alpine image with just the binary + UI assets.
#                 Writable /data volume for sqlite.
#
# Build:
#   docker build -f packages/zorya/Dockerfile -t zorya:latest .
# Run:
#   docker run --rm -p 4100:4100 -v zorya-data:/data zorya:latest
# ---------------------------------------------------------------------------

# -- stage 1 ----------------------------------------------------------------
FROM oven/bun:1.3.12-alpine AS builder

WORKDIR /build

# Copy repo metadata + all packages (bun.lock references every workspace).
# Dockerfile.dockerignore strips node_modules / dist / .nx so the context
# stays small.
COPY package.json bun.lock tsconfig.base.json tsconfig.json nx.json ./
COPY packages ./packages
COPY examples ./examples
COPY documentation ./documentation

RUN bun install --frozen-lockfile

# Build the UI (outputs packages/zorya/dist/public/).
RUN cd packages/zorya && bun --conditions=@promin/source scripts/build-ui.ts

# Compile the demo server into a single standalone binary. `--compile`
# statically links the Bun runtime so the output image doesn't need bun.
RUN cd packages/zorya \
 && bun build --compile --minify \
      --conditions=@promin/source \
      --target=bun-linux-$(uname -m | sed 's/x86_64/x64/;s/aarch64/arm64/') \
      --outfile=/build/zorya \
      examples/demo.ts

# -- stage 2 ----------------------------------------------------------------
FROM alpine:3.20 AS runtime

# Just libc + CA certs. `bun build --compile` statically links everything
# else the server needs.
RUN apk add --no-cache ca-certificates libstdc++ \
 && addgroup -S zorya \
 && adduser -S -G zorya -h /app zorya \
 && mkdir -p /app/public /data \
 && chown -R zorya:zorya /app /data

WORKDIR /app

COPY --from=builder --chown=zorya:zorya /build/zorya ./zorya
COPY --from=builder --chown=zorya:zorya /build/packages/zorya/dist/public ./public

USER zorya

ENV ZORYA_DB=/data/zorya.db \
    ZORYA_UI_DIR=/app/public \
    PORT=4100

EXPOSE 4100
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- --tries=1 --timeout=2 http://127.0.0.1:${PORT:-4100}/api/health \
      || exit 1

ENTRYPOINT ["./zorya"]
