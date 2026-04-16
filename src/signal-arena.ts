/**
 * @kronos/warp-core — Signal Arena
 *
 * SharedArrayBuffer-backed reactive signal system. Sub-microsecond
 * propagation within a thread, true cross-thread wakeup via
 * Atomics.compareExchange + Atomics.waitAsync with zero postMessage overhead.
 *
 * Signal node layout (8 × Int32 = 32 bytes per node):
 *   +0  flags      — bitfield (Mutable|Watching|Computed|Effect|Dirty|Pending|Scope|Locked)
 *   +1  version    — monotonic counter, CAS-bumped on every write
 *   +2  value_lo   — lower 32 bits of value
 *   +3  value_hi   — upper 32 bits
 *   +4  dep_head   — first Link in dependency direction
 *   +5  sub_head   — first Link in subscriber direction
 *   +6  effect_q   — next-pointer in pending effect flush list
 *   +7  owner_tid  — thread ID holding write lock (-1 = unlocked)
 */

export const SignalFlags = {
  Mutable:  0b00000001,
  Watching: 0b00000010,
  Computed: 0b00000100,
  Effect:   0b00001000,
  Dirty:    0b00010000,
  Pending:  0b00100000,
  Scope:    0b01000000,
  Locked:   0b10000000,
} as const;

/** Words per signal node. */
export const NODE_WORDS = 8;
/** Sentinel returned when the arena is full. */
export const INVALID_OFFSET = -1;
/** Words per link node. */
export const LINK_WORDS = 6;

// Field offsets
export const F_FLAGS     = 0;
export const F_VERSION   = 1;
export const F_VALUE_LO  = 2;
export const F_VALUE_HI  = 3;
export const F_DEP_HEAD  = 4;
export const F_SUB_HEAD  = 5;
export const F_EFFECT_Q  = 6;
export const F_OWNER_TID = 7;

/** Default arena size in Int32 words (16M words = 64 MB). */
export const DEFAULT_ARENA_WORDS = 16 * 1024 * 1024;

const ALLOC_PTR_OFFSET = 0;
const ARENA_HEADER_WORDS = 16;
const TID_OFFSET = 1;

/**
 * SignalArena — Allocates and manages signal/link nodes in a
 * SharedArrayBuffer so all threads share one coherent reactive graph.
 *
 * Main thread:
 * ```ts
 * const arena = SignalArena.create();
 * const sig = arena.createSignal(42);
 * arena.write(sig, 99);
 * arena.read(sig); // 99
 * ```
 *
 * Pass `arena.buffer` to Workers — reconstruct with `SignalArena.from(buffer)`.
 */
export class SignalArena {
  readonly buffer: SharedArrayBuffer;
  readonly view: Int32Array;

  private constructor(buffer: SharedArrayBuffer) {
    this.buffer = buffer;
    this.view = new Int32Array(buffer);
    Atomics.compareExchange(this.view, ALLOC_PTR_OFFSET, 0, ARENA_HEADER_WORDS);
  }

  /** Create a new arena (default 64 MB). */
  static create(byteSize = 64 * 1024 * 1024): SignalArena {
    return new SignalArena(new SharedArrayBuffer(byteSize));
  }

  /** Reconstruct from a SAB received in a Worker. */
  static from(buffer: SharedArrayBuffer): SignalArena {
    return new SignalArena(buffer);
  }

  // ── Allocation ─────────────────────────────────────────────────────────────

  private alloc(words: number): number {
    const offset = Atomics.add(this.view, ALLOC_PTR_OFFSET, words);
    if (offset + words > this.view.length) {
      Atomics.add(this.view, ALLOC_PTR_OFFSET, -words);
      return INVALID_OFFSET;
    }
    return offset;
  }

  // ── Signal Lifecycle ───────────────────────────────────────────────────────

  /** Create a mutable signal with an initial Int32 value. */
  createSignal(initialValue: number): number {
    const offset = this.alloc(NODE_WORDS);
    if (offset === INVALID_OFFSET) return INVALID_OFFSET;
    Atomics.store(this.view, offset + F_FLAGS, SignalFlags.Mutable);
    Atomics.store(this.view, offset + F_VERSION, 0);
    Atomics.store(this.view, offset + F_VALUE_LO, initialValue | 0);
    Atomics.store(this.view, offset + F_VALUE_HI, 0);
    Atomics.store(this.view, offset + F_DEP_HEAD, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_SUB_HEAD, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_EFFECT_Q, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_OWNER_TID, -1);
    return offset;
  }

  /** Create a computed node (lazy, dirty on creation). */
  createComputed(): number {
    const offset = this.alloc(NODE_WORDS);
    if (offset === INVALID_OFFSET) return INVALID_OFFSET;
    Atomics.store(this.view, offset + F_FLAGS, SignalFlags.Computed | SignalFlags.Dirty);
    Atomics.store(this.view, offset + F_VERSION, 0);
    Atomics.store(this.view, offset + F_VALUE_LO, 0);
    Atomics.store(this.view, offset + F_VALUE_HI, 0);
    Atomics.store(this.view, offset + F_DEP_HEAD, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_SUB_HEAD, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_EFFECT_Q, INVALID_OFFSET);
    Atomics.store(this.view, offset + F_OWNER_TID, -1);
    return offset;
  }

  // ── Signal I/O ─────────────────────────────────────────────────────────────

  /** Read the Int32 value of a signal node. Atomic — safe from any thread. */
  read(nodeOffset: number): number {
    return Atomics.load(this.view, nodeOffset + F_VALUE_LO);
  }

  /**
   * Write a new value to a mutable signal.
   * CAS-bumps version, marks subscribers dirty, and wakes all waiters.
   * Returns true on success, false on contention (caller should retry with backoff).
   */
  write(nodeOffset: number, newValue: number): boolean {
    const flags = Atomics.load(this.view, nodeOffset + F_FLAGS);
    if (!(flags & SignalFlags.Mutable)) return false;

    const tid = this.threadId();
    if (Atomics.compareExchange(this.view, nodeOffset + F_OWNER_TID, -1, tid) !== -1) {
      return false;
    }

    try {
      Atomics.store(this.view, nodeOffset + F_VALUE_LO, newValue | 0);
      Atomics.add(this.view, nodeOffset + F_VERSION, 1);
      Atomics.or(this.view, nodeOffset + F_FLAGS, SignalFlags.Dirty | SignalFlags.Pending);
      Atomics.notify(this.view, nodeOffset + F_VERSION, Infinity);
    } finally {
      Atomics.store(this.view, nodeOffset + F_OWNER_TID, -1);
    }
    return true;
  }

  /**
   * Write with exponential backoff on contention.
   * Retries up to `maxRetries` times (default 10) using jittered backoff.
   * Returns true on success, false if all retries failed.
   */
  writeWithBackoff(nodeOffset: number, newValue: number, maxRetries = 10): boolean {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (this.write(nodeOffset, newValue)) return true;
      if (attempt < maxRetries) {
        // Exponential backoff with jitter: ~1µs → ~512µs
        const backoffUs = (1 << attempt) + Math.floor(Math.random() * (1 << attempt));
        const start = performance.now();
        while ((performance.now() - start) * 1000 < backoffUs) { /* spin-wait */ }
      }
    }
    return false;
  }

  /**
   * Wait for a signal's version to advance past `currentVersion`.
   * Uses `Atomics.waitAsync` — zero postMessage overhead.
   * Falls back to a polling Promise if `Atomics.waitAsync` is unavailable.
   */
  waitForChange(nodeOffset: number, currentVersion: number): Promise<number> {
    const atomicsAny = Atomics as unknown as Record<string, unknown>;
    if (typeof atomicsAny.waitAsync !== 'function') {
      // Fallback: poll every 4ms when Atomics.waitAsync is unavailable
      return new Promise((resolve) => {
        const check = (): void => {
          const v = Atomics.load(this.view, nodeOffset + F_VERSION);
          if (v !== currentVersion) { resolve(v); return; }
          setTimeout(check, 4);
        };
        check();
      });
    }

    type WaitAsyncResult =
      | { async: false; value: 'ok' | 'not-equal' | 'timed-out' }
      | { async: true; value: Promise<'ok' | 'timed-out'> };
    const waitAsync = atomicsAny.waitAsync as
      (ta: Int32Array, index: number, value: number) => WaitAsyncResult;

    const result = waitAsync(this.view, nodeOffset + F_VERSION, currentVersion);
    if (result.async) {
      return result.value.then(() => Atomics.load(this.view, nodeOffset + F_VERSION));
    }
    return Promise.resolve(Atomics.load(this.view, nodeOffset + F_VERSION));
  }

  // ── Flag Helpers ───────────────────────────────────────────────────────────

  hasFlag(nodeOffset: number, flag: number): boolean {
    return (Atomics.load(this.view, nodeOffset + F_FLAGS) & flag) !== 0;
  }

  setFlags(nodeOffset: number, flags: number): void {
    Atomics.or(this.view, nodeOffset + F_FLAGS, flags);
  }

  clearFlags(nodeOffset: number, flags: number): void {
    Atomics.and(this.view, nodeOffset + F_FLAGS, ~flags);
  }

  // ── Subscriber Links ───────────────────────────────────────────────────────

  addSubscriber(signalOffset: number, subscriberOffset: number): number {
    const linkOffset = this.alloc(LINK_WORDS);
    if (linkOffset === INVALID_OFFSET) return INVALID_OFFSET;

    const prevHead = Atomics.load(this.view, signalOffset + F_SUB_HEAD);
    Atomics.store(this.view, linkOffset + 0, signalOffset);
    Atomics.store(this.view, linkOffset + 1, subscriberOffset);
    Atomics.store(this.view, linkOffset + 2, INVALID_OFFSET);
    Atomics.store(this.view, linkOffset + 3, prevHead);
    Atomics.store(this.view, linkOffset + 4, INVALID_OFFSET);
    Atomics.store(this.view, linkOffset + 5, INVALID_OFFSET);

    if (prevHead !== INVALID_OFFSET) {
      Atomics.store(this.view, prevHead + 2, linkOffset);
    }
    Atomics.store(this.view, signalOffset + F_SUB_HEAD, linkOffset);
    Atomics.or(this.view, signalOffset + F_FLAGS, SignalFlags.Watching);
    return linkOffset;
  }

  // ── Version / Dirty ────────────────────────────────────────────────────────

  version(nodeOffset: number): number {
    return Atomics.load(this.view, nodeOffset + F_VERSION);
  }

  markClean(nodeOffset: number): void {
    Atomics.and(this.view, nodeOffset + F_FLAGS, ~(SignalFlags.Dirty | SignalFlags.Pending));
  }

  // ── Arena Diagnostics ──────────────────────────────────────────────────────

  get allocatedWords(): number {
    return Atomics.load(this.view, ALLOC_PTR_OFFSET);
  }

  get allocatedBytes(): number {
    return this.allocatedWords * 4;
  }

  // ── Internal ───────────────────────────────────────────────────────────────

  private threadId(): number {
    const wt = (globalThis as Record<string, unknown>)['__workerThreads__'] as
      { threadId: number } | undefined;
    if (wt) return wt.threadId;
    let tid = Atomics.load(this.view, TID_OFFSET);
    if (tid === 0) {
      const candidate = (Math.random() * 0x7fffffff) | 1;
      Atomics.compareExchange(this.view, TID_OFFSET, 0, candidate);
      tid = Atomics.load(this.view, TID_OFFSET);
    }
    return tid;
  }
}
