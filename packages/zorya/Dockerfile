# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Zorya — self-hostable workflow dashboard (demo server)
#
# Multi-stage build:
#   build    installs the whole monorepo, bundles the dashboard UI and
#            writes the runtime workspace manifest
#   deps     production-only install of the workspaces the server loads
#   sources  their source trees, without tests
#   runtime  the three above on a clean Bun image
#
# The demo discovers its workflows, tools, agents, skills and fragments by
# importing files under packages/zorya/examples at boot, so it runs from
# source under Bun (those files must share module instances with the
# server) rather than as a single bundle. See DEPLOYMENT.md.
#
# Build:
#   docker build -f packages/zorya/Dockerfile -t zorya:latest .
# Run (the server refuses to start without both keys):
#   docker run --rm -p 4100:4100 -v zorya-data:/data --stop-timeout 30 \
#     -e ZORYA_API_KEY="$(openssl rand -hex 32)" \
#     -e ZORYA_WORKER_API_KEY="$(openssl rand -hex 32)" \
#     zorya:latest
#
#   ZORYA_API_KEY         operator key for the dashboard and /api
#                         (sent as `Authorization: Bearer <key>`)
#   ZORYA_WORKER_API_KEY  worker key for /ws/worker, /rpc/* and the
#                         worker-protocol routes (ZoryaClient `apiKey`)
#   ZORYA_INSECURE=1      run without keys — local development only
#
# HEALTHCHECK probes /api/ready (public): healthy once storage answers and
# every background service has started. /api/health is plain liveness.
# ---------------------------------------------------------------------------

ARG BUN_IMAGE=oven/bun:1.3.12-alpine

# --- build: full install, UI bundle, runtime manifest -----------------------
FROM ${BUN_IMAGE} AS build
WORKDIR /src

# bun.lock references every workspace, so --frozen-lockfile needs them all.
# Dockerfile.dockerignore strips node_modules / dist / .nx.
COPY package.json bun.lock tsconfig.base.json tsconfig.json nx.json ./
COPY packages ./packages
COPY examples ./examples
COPY documentation ./documentation

RUN bun install --frozen-lockfile
# Outputs packages/zorya/dist/public/.
RUN bun run build:ui
# Manifests of the runtime workspaces, without dev dependencies.
RUN bun packages/zorya/scripts/image-manifest.ts /runtime

# --- deps: runtime dependencies of the runtime workspaces only --------------
FROM ${BUN_IMAGE} AS deps
WORKDIR /app
COPY --from=build /runtime ./
# The repo lockfile pins every version. Bun trims it to the reduced
# workspace set, so the install can't be frozen.
COPY bun.lock ./
RUN bun install --ignore-scripts

# --- sources: the runtime workspaces' code, without tests -------------------
FROM ${BUN_IMAGE} AS sources
WORKDIR /app
COPY packages/agent/src packages/agent/src
COPY packages/evals/src packages/evals/src
COPY packages/postgres/src packages/postgres/src
COPY packages/postgres/drizzle packages/postgres/drizzle
COPY packages/sqlite/src packages/sqlite/src
COPY packages/workflow/src packages/workflow/src
COPY packages/workflow-remote/src packages/workflow-remote/src
COPY packages/zorya/src/index.ts packages/zorya/src/sqlite.ts packages/zorya/src/
COPY packages/zorya/src/server packages/zorya/src/server
COPY packages/zorya/examples packages/zorya/examples
RUN find packages -type d -name tests -prune -exec rm -rf {} + \
 && find packages \( -name '*.bench.ts' -o -name '*.type-fixture.ts' \) -delete

# --- runtime ----------------------------------------------------------------
FROM ${BUN_IMAGE} AS runtime
WORKDIR /app

COPY --from=deps /app ./
COPY --from=sources /app/packages ./packages
COPY --from=build /src/packages/zorya/dist/public ./packages/zorya/dist/public

# The code stays root-owned and read-only; only /data is writable.
RUN mkdir -p /data && chown bun:bun /data

USER bun

# ZORYA_API_KEY and ZORYA_WORKER_API_KEY are required at run time (see top).
ENV ZORYA_DB=/data/zorya.db \
    ZORYA_UI_DIR=/app/packages/zorya/dist/public \
    PORT=4100

EXPOSE 4100
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=4s --start-period=10s --retries=3 \
  CMD wget -qO- --tries=1 --timeout=3 http://127.0.0.1:${PORT:-4100}/api/ready \
      || exit 1

# `docker stop` sends SIGTERM; the server drains and exits (DEPLOYMENT.md).
STOPSIGNAL SIGTERM
ENTRYPOINT ["bun", "--conditions=@promin/source", "packages/zorya/examples/demo.ts"]
