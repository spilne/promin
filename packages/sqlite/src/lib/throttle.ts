/** Pluggable throttle interface — test against this, implement with any backend. */
export interface Throttle {
  acquireAsync(resource?: string): Promise<void>;
  tryAcquireAsync(resource?: string): Promise<boolean>;
  withPermitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T>;
}
