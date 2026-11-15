# @promin/zorya-client

Typed HTTP client for the Zorya workflow and agent server. Install with
`bun add @promin/zorya-client`.

`ZoryaClient` exposes workflow triggering, remote storage and worker
registration. `ZoryaWorker` drives queued workflow starts, and `ZoryaRunner`
adapts remote workflow execution. The package also exports the worker control
socket and the `runAgentTask` helper for running an agent task through Zorya.

See the [Zorya guide](https://spilne.github.io/promin/docs/agent/zorya/)
for server setup, authentication and API behavior.
