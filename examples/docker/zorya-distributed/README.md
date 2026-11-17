# Distributed Zorya with four workers and two agents

This stack is a runnable topology example:

```text
                       ┌──────────────┐
                       │ Zorya central│
                       │  :4100       │
                       └──────┬───────┘
            worker RPC / WS  │  remote-agent HTTP
          ┌────────┬─────────┼──────────┬─────────┐
          ▼        ▼         ▼          ▼         ▼
       worker-1 worker-2  worker-3  worker-4   agent-one
                                                  + agent-two
```

- `central` persists workflow state, worker registrations, and remote-agent
  recipes in SQLite and exposes the Zorya API on <http://localhost:4100>.
- Four workers advertise the same `distributed-job` workflow. Each accepts
  four runs concurrently by default, so the pool has 16 execution slots.
- Two separately persisted Zorya agent nodes expose `research-agent` and
  `support-agent`. They self-register, heartbeat, and gracefully unregister.
- The coordinator validates remote endpoints at registration and on every
  call. Private-network access is limited to the two named Compose services.
- The agents use a deterministic local echo model. That makes the example and
  its load check reproducible without an OpenAI, Anthropic, or Ollama key.

## Run it

From this directory:

```bash
docker compose up --build -d
docker compose ps
```

Check the fleet and agents (the default keys are development-only):

```bash
curl -s http://localhost:4100/api/workers \
  -H 'Authorization: Bearer dev-operator-key-change-me'

curl -s http://localhost:4100/api/agents \
  -H 'Authorization: Bearer dev-operator-key-change-me'
```

Invoke an agent through the coordinator. The request is authenticated by the
coordinator, proxied to `agent-one`, and authenticated again there:

```bash
curl -s http://localhost:4100/api/agents/research-agent/invoke \
  -H 'Authorization: Bearer dev-operator-key-change-me' \
  -H 'Content-Type: application/json' \
  -d '{"task":"summarize the rollout","namespaceId":"demo","resourceId":"alice"}'
```

Trigger distributed work:

```bash
curl -s http://localhost:4100/api/workflows/distributed-job/runs \
  -H 'Authorization: Bearer dev-operator-key-change-me' \
  -H 'Content-Type: application/json' \
  -d '{"input":{"id":1,"delayMs":250}}'
```

## Smoke and performance check

The test waits for all services, invokes both remote agents concurrently,
submits 64 workflow runs, verifies every run completes, and verifies all four
workers handled work. It prints observed agent p50/p95 latency and completed
workflow throughput for the local machine:

```bash
docker compose --profile test run --rm smoke
```

These numbers are a topology smoke benchmark, not production capacity. Docker
Desktop CPU/memory limits, SQLite serialization, workflow duration, model
latency, tool calls, and network placement all materially affect capacity.

Tune the run without editing files:

```bash
SMOKE_WORKFLOW_RUNS=200 SMOKE_AGENT_INVOCATIONS=100 \
  docker compose --profile test run --rm smoke
```

## Production changes

The checked-in key defaults make a clone easy to run and are intentionally
unsafe outside local development. Set `ZORYA_API_KEY`,
`ZORYA_WORKER_API_KEY`, both agent keys, and
`PROMIN_SECRETS_PASSPHRASE` to secret-manager values before deployment.
Terminate TLS at an ingress or service mesh, remove host-published agent ports
when direct debugging is unnecessary, and replace the example SQLite stores
with the deployment's durable/shared storage strategy where horizontal
coordinator or agent replicas are required.

Stop the stack and remove its example data with:

```bash
docker compose down -v
```
