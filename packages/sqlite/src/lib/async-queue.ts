/** Pluggable queue interface — implement with any backend. */
export interface AsyncQueue<T> {
  offerAsync(item: T): Promise<void>;
  takeAsync(): Promise<T>;
  shutdownAsync(): Promise<void>;
  sizeAsync(): Promise<number>;
}
