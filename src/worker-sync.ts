/**
 * @kronos/warp-core — Worker Sync
 *
 * Browser-worker synchronization API. Provides a typed channel for
 * sending reactive state updates between the main thread and Workers
 * using either BroadcastChannel (cross-tab) or MessagePort (same-origin).
 *
 * In Node.js / test environments, falls back to a tiny in-process emitter shim
 * (no Node `events` dependency — keeps the package browser-bundleable).
 */

/** Message types flowing through the sync channel. */
export interface SyncMessage {
  type: 'state-update' | 'signal-write' | 'effect-flush' | 'heartbeat' | 'custom';
  sourceId: string;
  payload: unknown;
  timestamp: number;
}

/** Listener for sync messages. */
export type SyncListener = (message: SyncMessage) => void;

/** Minimal BroadcastChannel interface for cross-environment compatibility. */
export interface BroadcastChannelLike {
  postMessage(data: unknown): void;
  close(): void;
  onmessage: ((event: { data: unknown }) => void) | null;
}

/** Configuration for WorkerSync. */
export interface WorkerSyncConfig {
  /** Unique identifier for this node. */
  nodeId: string;
  /** Channel name (default: 'warp_core_sync'). */
  channelName?: string;
  /** BroadcastChannel constructor override (for test injection). */
  BroadcastChannelCtor?: new (name: string) => BroadcastChannelLike;
}

// ── Tiny cross-environment emitter (replaces Node `events`) ─────────────────

type ShimHandler = (msg: SyncMessage) => void;
const shimChannels = new Map<string, Set<ShimHandler>>();

function shimOn(channel: string, handler: ShimHandler): void {
  if (!shimChannels.has(channel)) shimChannels.set(channel, new Set());
  shimChannels.get(channel)!.add(handler);
}

function shimOff(channel: string, handler: ShimHandler): void {
  shimChannels.get(channel)?.delete(handler);
}

function shimEmit(channel: string, msg: SyncMessage): void {
  const handlers = shimChannels.get(channel);
  if (handlers) for (const h of handlers) h(msg);
}

// ── WorkerSync ──────────────────────────────────────────────────────────────

/**
 * WorkerSync — Cross-context state synchronization bus.
 *
 * Usage:
 * ```ts
 * const sync = new WorkerSync({ nodeId: 'main' });
 * sync.on('state-update', (msg) => applyUpdate(msg.payload));
 * sync.send({ type: 'state-update', payload: { count: 42 } });
 * ```
 */
export class WorkerSync {
  private readonly nodeId: string;
  private readonly channelName: string;
  private readonly listeners = new Map<string, Set<SyncListener>>();
  private readonly wildcardListeners = new Set<SyncListener>();
  private channel: BroadcastChannelLike | null = null;
  private closed = false;
  private readonly shimHandler: ShimHandler | null = null;

  constructor(config: WorkerSyncConfig) {
    this.nodeId = config.nodeId;
    this.channelName = config.channelName ?? 'warp_core_sync';

    // Resolution order: explicit ctor > globalThis.BroadcastChannel > in-process shim
    const Ctor = config.BroadcastChannelCtor
      ?? (typeof BroadcastChannel !== 'undefined' ? BroadcastChannel as unknown as new (name: string) => BroadcastChannelLike : null);

    if (Ctor) {
      this.channel = new Ctor(this.channelName);
      this.channel.onmessage = (event: { data: unknown }) => {
        this.dispatch(event.data as SyncMessage);
      };
    } else {
      // In-process shim for Node.js / test — stores handler for targeted removal
      const handler: ShimHandler = (msg: SyncMessage) => {
        if (msg.sourceId !== this.nodeId) {
          this.dispatch(msg);
        }
      };
      this.shimHandler = handler;
      shimOn(this.channelName, handler);
    }
  }

  /** Subscribe to a specific message type. Returns unsubscribe function. */
  on(type: SyncMessage['type'] | '*', listener: SyncListener): () => void {
    if (type === '*') {
      this.wildcardListeners.add(listener);
      return () => { this.wildcardListeners.delete(listener); };
    }
    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set());
    }
    this.listeners.get(type)!.add(listener);
    return () => { this.listeners.get(type)?.delete(listener); };
  }

  /** Send a message to all other nodes on the channel. */
  send(partial: Omit<SyncMessage, 'sourceId' | 'timestamp'>): void {
    if (this.closed) return;
    const message: SyncMessage = {
      ...partial,
      sourceId: this.nodeId,
      timestamp: Date.now(),
    };
    if (this.channel) {
      this.channel.postMessage(message);
    } else {
      shimEmit(this.channelName, message);
    }
  }

  /** Close the channel. Only removes this instance's shim handler. */
  close(): void {
    this.closed = true;
    this.channel?.close();
    this.channel = null;
    if (this.shimHandler) {
      shimOff(this.channelName, this.shimHandler);
    }
  }

  private dispatch(message: SyncMessage): void {
    if (this.closed) return;
    if (message.sourceId === this.nodeId) return;

    const typed = this.listeners.get(message.type);
    if (typed) {
      for (const listener of typed) listener(message);
    }
    for (const listener of this.wildcardListeners) {
      listener(message);
    }
  }
}
