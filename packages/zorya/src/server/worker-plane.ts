/** Listener-level worker endpoint. It bypasses Router and must stay worker-key authenticated. */
export const WORKER_WEBSOCKET_PATH = "/ws/worker";

/** True only for the listener-owned worker WebSocket endpoint. */
export function isWorkerWebSocketPath(path: string): boolean {
  return path === WORKER_WEBSOCKET_PATH;
}
