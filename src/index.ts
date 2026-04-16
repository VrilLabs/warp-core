/**
 * @kronos/warp-core — Public API
 *
 * Sub-microsecond reactive runtime with SharedArrayBuffer-backed signals,
 * cross-thread propagation, Proxy-based stores, and devtools integration.
 */

// ── Signal Arena (low-level SAB-backed signals) ────────────────────────────
export {
  SignalArena,
  SignalFlags,
  NODE_WORDS,
  LINK_WORDS,
  INVALID_OFFSET,
  DEFAULT_ARENA_WORDS,
  F_FLAGS,
  F_VERSION,
  F_VALUE_LO,
  F_VALUE_HI,
  F_DEP_HEAD,
  F_SUB_HEAD,
  F_EFFECT_Q,
  F_OWNER_TID,
} from './signal-arena';

// ── Effect Scope ───────────────────────────────────────────────────────────
export { EffectScope } from './effect-scope';

// ── Reactive Store (Proxy-based high-level state) ──────────────────────────
export { ReactiveStore, createStore } from './reactive-store';
export type { StateListener } from './reactive-store';

// ── Worker Sync (cross-context state bus) ──────────────────────────────────
export { WorkerSync } from './worker-sync';
export type {
  SyncMessage,
  SyncListener,
  BroadcastChannelLike,
  WorkerSyncConfig,
} from './worker-sync';

// ── Devtools ───────────────────────────────────────────────────────────────
export {
  inspectSignal,
  arenaStats,
  scanSignals,
  installDevtoolsHook,
} from './devtools';
export type { SignalSnapshot, ArenaStats } from './devtools';
