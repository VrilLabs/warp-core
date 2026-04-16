/**
 * @kronos/warp-core — Devtools Hooks
 *
 * Provides inspection and debugging utilities for the reactive runtime.
 * Exposes arena stats, signal inspection, and a global devtools hook
 * that UI panels or browser devtools extensions can read.
 */

import { SignalArena, SignalFlags, NODE_WORDS, F_FLAGS, F_VERSION, F_VALUE_LO, INVALID_OFFSET } from './signal-arena';
import { ReactiveStore } from './reactive-store';

/** Snapshot of a single signal node for devtools inspection. */
export interface SignalSnapshot {
  offset: number;
  value: number;
  version: number;
  flags: {
    mutable: boolean;
    watching: boolean;
    computed: boolean;
    effect: boolean;
    dirty: boolean;
    pending: boolean;
    scope: boolean;
    locked: boolean;
  };
}

/** Arena-level stats for devtools. */
export interface ArenaStats {
  allocatedBytes: number;
  allocatedWords: number;
  totalBytes: number;
  utilizationPercent: number;
}

/**
 * Inspect a signal node by offset.
 */
export function inspectSignal(arena: SignalArena, offset: number): SignalSnapshot | null {
  if (offset === INVALID_OFFSET) return null;
  const flags = Atomics.load(arena.view, offset + F_FLAGS);
  return {
    offset,
    value: Atomics.load(arena.view, offset + F_VALUE_LO),
    version: Atomics.load(arena.view, offset + F_VERSION),
    flags: {
      mutable:  !!(flags & SignalFlags.Mutable),
      watching: !!(flags & SignalFlags.Watching),
      computed: !!(flags & SignalFlags.Computed),
      effect:   !!(flags & SignalFlags.Effect),
      dirty:    !!(flags & SignalFlags.Dirty),
      pending:  !!(flags & SignalFlags.Pending),
      scope:    !!(flags & SignalFlags.Scope),
      locked:   !!(flags & SignalFlags.Locked),
    },
  };
}

/**
 * Get arena utilization stats.
 */
export function arenaStats(arena: SignalArena): ArenaStats {
  const totalBytes = arena.buffer.byteLength;
  const allocatedBytes = arena.allocatedBytes;
  return {
    allocatedBytes,
    allocatedWords: arena.allocatedWords,
    totalBytes,
    utilizationPercent: totalBytes > 0 ? (allocatedBytes / totalBytes) * 100 : 0,
  };
}

/**
 * Scan the arena and return snapshots of all allocated signal nodes.
 * Note: This is O(N) and should only be used in devtools, not in hot paths.
 */
export function scanSignals(arena: SignalArena, maxCount = 1000): SignalSnapshot[] {
  const results: SignalSnapshot[] = [];
  const allocated = arena.allocatedWords;
  let offset = 16; // skip arena header

  while (offset + NODE_WORDS <= allocated && results.length < maxCount) {
    const flags = Atomics.load(arena.view, offset + F_FLAGS);
    // A node with any flags set is likely a valid signal node
    if (flags !== 0) {
      const snap = inspectSignal(arena, offset);
      if (snap) results.push(snap);
    }
    offset += NODE_WORDS;
  }

  return results;
}

/**
 * Expose devtools hooks on globalThis.__WARP_CORE_DEVTOOLS__.
 * Call this once at startup to make the reactive graph inspectable.
 */
export function installDevtoolsHook(arena: SignalArena, stores?: Map<string, ReactiveStore<Record<string, unknown>>>): void {
  const hook = {
    arena,
    inspectSignal: (offset: number) => inspectSignal(arena, offset),
    arenaStats: () => arenaStats(arena),
    scanSignals: (maxCount?: number) => scanSignals(arena, maxCount),
    stores: stores ?? new Map(),
    version: '0.1.0',
  };
  (globalThis as Record<string, unknown>)['__WARP_CORE_DEVTOOLS__'] = hook;
}
