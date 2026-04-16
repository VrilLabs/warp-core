/**
 * @kronos/warp-core — Reactive Store
 *
 * Proxy-based reactive state container. Wraps a plain object in a Proxy
 * that intercepts writes and notifies subscribers. Framework-agnostic,
 * works in any environment that supports Proxy.
 *
 * Usage:
 * ```ts
 * const store = createStore({ count: 0, name: 'KRONOS' });
 * store.on('count', (newVal, oldVal) => console.log(newVal));
 * store.state.count = 1; // triggers listener
 * ```
 */

/** Listener signature for state changes. */
export type StateListener<V> = (newValue: V, oldValue: V) => void;

export class ReactiveStore<T extends Record<string, unknown>> {
  readonly state: T;
  private readonly raw: T;
  private readonly listeners: Map<string, StateListener<unknown>[]>;
  private readonly globalListeners: StateListener<unknown>[];

  constructor(initial: T) {
    this.raw = { ...initial };
    this.listeners = new Map();
    this.globalListeners = [];

    this.state = new Proxy(this.raw, {
      set: (target, prop, value) => {
        const key = String(prop);
        const oldValue = target[key as keyof T];
        if (oldValue === value) return true;
        (target as Record<string, unknown>)[key] = value;
        this.notify(key, value, oldValue);
        return true;
      },
      get: (target, prop) => target[prop as keyof T],
    });
  }

  /** Subscribe to changes on a specific field. Returns unsubscribe function. */
  on<K extends string & keyof T>(field: K, listener: StateListener<T[K]>): () => void {
    if (!this.listeners.has(field)) {
      this.listeners.set(field, []);
    }
    this.listeners.get(field)!.push(listener as StateListener<unknown>);
    return () => {
      const arr = this.listeners.get(field);
      if (!arr) return;
      const idx = arr.indexOf(listener as StateListener<unknown>);
      if (idx !== -1) arr.splice(idx, 1);
    };
  }

  /** Subscribe to all state changes. */
  onAny(listener: StateListener<unknown>): () => void {
    this.globalListeners.push(listener);
    return () => {
      const idx = this.globalListeners.indexOf(listener);
      if (idx !== -1) this.globalListeners.splice(idx, 1);
    };
  }

  /** Get a snapshot of the current state (shallow copy). */
  snapshot(): T {
    return { ...this.raw };
  }

  /** Batch multiple updates — only notifies after all writes complete. */
  batch(updater: (state: T) => void): void {
    const pending: Array<{ key: string; newValue: unknown; oldValue: unknown }> = [];

    const batchProxy = new Proxy(this.raw, {
      set: (target, prop, value) => {
        const key = String(prop);
        const oldValue = target[key as keyof T];
        if (oldValue !== value) {
          (target as Record<string, unknown>)[key] = value;
          pending.push({ key, newValue: value, oldValue });
        }
        return true;
      },
      get: (target, prop) => target[prop as keyof T],
    });

    updater(batchProxy as T);

    for (const { key, newValue, oldValue } of pending) {
      this.notify(key, newValue, oldValue);
    }
  }

  private notify(key: string, newValue: unknown, oldValue: unknown): void {
    const fieldListeners = this.listeners.get(key);
    if (fieldListeners) {
      for (const listener of fieldListeners) {
        listener(newValue, oldValue);
      }
    }
    for (const listener of this.globalListeners) {
      listener(newValue, oldValue);
    }
  }
}

/** Convenience factory for creating a reactive store. */
export function createStore<T extends Record<string, unknown>>(initial: T): ReactiveStore<T> {
  return new ReactiveStore(initial);
}
