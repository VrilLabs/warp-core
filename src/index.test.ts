/**
 * @kronos/warp-core — Tests
 */

import { strict as assert } from 'node:assert';
import { describe, it, beforeEach, afterEach } from 'mocha';

import {
  SignalArena,
  SignalFlags,
  INVALID_OFFSET,
  NODE_WORDS,
  EffectScope,
  ReactiveStore,
  createStore,
  WorkerSync,
  inspectSignal,
  arenaStats,
  scanSignals,
  installDevtoolsHook,
} from './index';

import type { SyncMessage } from './index';

// ── SignalArena Tests ────────────────────────────────────────────────────────

describe('@kronos/warp-core', () => {
  describe('SignalArena', () => {
    let arena: SignalArena;

    beforeEach(() => {
      // Use small arena for tests (64 KB)
      arena = SignalArena.create(64 * 1024);
    });

    it('creates a signal with initial value', () => {
      const sig = arena.createSignal(42);
      assert.notStrictEqual(sig, INVALID_OFFSET);
      assert.strictEqual(arena.read(sig), 42);
    });

    it('writes and reads a signal', () => {
      const sig = arena.createSignal(0);
      assert.ok(arena.write(sig, 99));
      assert.strictEqual(arena.read(sig), 99);
    });

    it('increments version on write', () => {
      const sig = arena.createSignal(0);
      const v0 = arena.version(sig);
      arena.write(sig, 1);
      const v1 = arena.version(sig);
      assert.strictEqual(v1, v0 + 1);
    });

    it('creates computed nodes as dirty', () => {
      const comp = arena.createComputed();
      assert.notStrictEqual(comp, INVALID_OFFSET);
      assert.ok(arena.hasFlag(comp, SignalFlags.Computed));
      assert.ok(arena.hasFlag(comp, SignalFlags.Dirty));
    });

    it('adds a subscriber link', () => {
      const sig = arena.createSignal(10);
      const sub = arena.createComputed();
      const link = arena.addSubscriber(sig, sub);
      assert.notStrictEqual(link, INVALID_OFFSET);
      assert.ok(arena.hasFlag(sig, SignalFlags.Watching));
    });

    it('marks clean clears dirty and pending', () => {
      const comp = arena.createComputed();
      assert.ok(arena.hasFlag(comp, SignalFlags.Dirty));
      arena.markClean(comp);
      assert.ok(!arena.hasFlag(comp, SignalFlags.Dirty));
    });

    it('reports arena stats', () => {
      arena.createSignal(1);
      assert.ok(arena.allocatedWords > 0);
      assert.ok(arena.allocatedBytes > 0);
    });

    it('from() reconstructs from buffer', () => {
      const sig = arena.createSignal(777);
      const arena2 = SignalArena.from(arena.buffer);
      assert.strictEqual(arena2.read(sig), 777);
    });

    it('returns INVALID_OFFSET when arena is full', () => {
      // Tiny arena — fill it up
      const tiny = SignalArena.create(256); // 256 bytes = 64 words
      // Header takes 16 words, each signal takes 8 words, so ~6 signals fit
      const results: number[] = [];
      for (let i = 0; i < 20; i++) {
        results.push(tiny.createSignal(i));
      }
      assert.ok(results.some(r => r === INVALID_OFFSET));
    });
  });

  // ── EffectScope Tests ──────────────────────────────────────────────────────

  describe('EffectScope', () => {
    let arena: SignalArena;

    beforeEach(() => {
      arena = SignalArena.create(64 * 1024);
    });

    it('creates without error', () => {
      const scope = new EffectScope(arena);
      assert.ok(scope);
      assert.strictEqual(scope.isStopped, false);
    });

    it('stops cleanly', () => {
      const scope = new EffectScope(arena);
      const sig = arena.createSignal(0);
      scope.effect(sig, () => {});
      scope.stop();
      assert.strictEqual(scope.isStopped, true);
    });

    it('ignores effects after stop', () => {
      const scope = new EffectScope(arena);
      scope.stop();
      const sig = arena.createSignal(0);
      scope.effect(sig, () => {}); // should not throw
    });
  });

  // ── ReactiveStore Tests ────────────────────────────────────────────────────

  describe('ReactiveStore', () => {
    it('initializes with provided state', () => {
      const store = new ReactiveStore({ count: 0, name: 'test' });
      assert.strictEqual(store.state.count, 0);
      assert.strictEqual(store.state.name, 'test');
    });

    it('notifies field listeners on write', () => {
      const store = new ReactiveStore({ count: 0 });
      let received = -1;
      store.on('count', (newVal: unknown) => { received = newVal as number; });
      store.state.count = 42;
      assert.strictEqual(received, 42);
    });

    it('does not notify on same-value write', () => {
      const store = new ReactiveStore({ count: 0 });
      let called = 0;
      store.on('count', () => { called++; });
      store.state.count = 0; // same value
      assert.strictEqual(called, 0);
    });

    it('notifies global listeners', () => {
      const store = new ReactiveStore({ a: 1, b: 2 });
      const changes: unknown[] = [];
      store.onAny((newVal: unknown) => { changes.push(newVal); });
      store.state.a = 10;
      store.state.b = 20;
      assert.strictEqual(changes.length, 2);
    });

    it('unsubscribes correctly', () => {
      const store = new ReactiveStore({ count: 0 });
      let called = 0;
      const unsub = store.on('count', () => { called++; });
      store.state.count = 1;
      assert.strictEqual(called, 1);
      unsub();
      store.state.count = 2;
      assert.strictEqual(called, 1); // should not increase
    });

    it('returns snapshot', () => {
      const store = new ReactiveStore({ x: 1, y: 2 });
      const snap = store.snapshot();
      assert.deepStrictEqual(snap, { x: 1, y: 2 });
      // Snapshot is a copy
      snap.x = 100;
      assert.strictEqual(store.state.x, 1);
    });

    it('batch updates notify after all writes', () => {
      const store = new ReactiveStore({ a: 0, b: 0 });
      const notifications: string[] = [];
      store.on('a', () => notifications.push('a'));
      store.on('b', () => notifications.push('b'));
      store.batch((s: Record<string, unknown>) => {
        s.a = 1;
        s.b = 2;
      });
      assert.deepStrictEqual(notifications, ['a', 'b']);
    });
  });

  // ── createStore factory ────────────────────────────────────────────────────

  describe('createStore()', () => {
    it('creates a ReactiveStore instance', () => {
      const store = createStore({ value: 'hello' });
      assert.ok(store instanceof ReactiveStore);
      assert.strictEqual(store.state.value, 'hello');
    });
  });

  // ── WorkerSync Tests ───────────────────────────────────────────────────────

  describe('WorkerSync', () => {
    it('sends and receives messages via shim', (done) => {
      const a = new WorkerSync({ nodeId: 'node-a', channelName: 'test-ch-1' });
      const b = new WorkerSync({ nodeId: 'node-b', channelName: 'test-ch-1' });

      b.on('state-update', (msg: SyncMessage) => {
        assert.strictEqual(msg.sourceId, 'node-a');
        assert.deepStrictEqual(msg.payload, { count: 42 });
        a.close();
        b.close();
        done();
      });

      a.send({ type: 'state-update', payload: { count: 42 } });
    });

    it('does not receive own messages', (done) => {
      const a = new WorkerSync({ nodeId: 'node-self', channelName: 'test-ch-2' });
      let received = false;
      a.on('heartbeat', () => { received = true; });
      a.send({ type: 'heartbeat', payload: null });

      setTimeout(() => {
        assert.strictEqual(received, false);
        a.close();
        done();
      }, 50);
    });

    it('wildcard listener receives all types', (done) => {
      const a = new WorkerSync({ nodeId: 'sender', channelName: 'test-ch-3' });
      const b = new WorkerSync({ nodeId: 'receiver', channelName: 'test-ch-3' });

      b.on('*', (msg: SyncMessage) => {
        assert.strictEqual(msg.type, 'custom');
        a.close();
        b.close();
        done();
      });

      a.send({ type: 'custom', payload: 'data' });
    });

    it('does not send after close', () => {
      const a = new WorkerSync({ nodeId: 'closed', channelName: 'test-ch-4' });
      a.close();
      // Should not throw
      a.send({ type: 'heartbeat', payload: null });
    });
  });

  // ── Devtools Tests ─────────────────────────────────────────────────────────

  describe('Devtools', () => {
    let arena: SignalArena;

    beforeEach(() => {
      arena = SignalArena.create(64 * 1024);
    });

    it('inspects a signal', () => {
      const sig = arena.createSignal(42);
      const snap = inspectSignal(arena, sig);
      assert.ok(snap);
      assert.strictEqual(snap.value, 42);
      assert.strictEqual(snap.flags.mutable, true);
    });

    it('returns null for INVALID_OFFSET', () => {
      assert.strictEqual(inspectSignal(arena, INVALID_OFFSET), null);
    });

    it('returns arena stats', () => {
      arena.createSignal(1);
      const stats = arenaStats(arena);
      assert.ok(stats.allocatedBytes > 0);
      assert.ok(stats.totalBytes > 0);
      assert.ok(stats.utilizationPercent > 0);
    });

    it('scans signals', () => {
      arena.createSignal(10);
      arena.createSignal(20);
      arena.createSignal(30);
      const signals = scanSignals(arena, 100);
      assert.ok(signals.length >= 3);
    });

    it('installs devtools hook on globalThis', () => {
      installDevtoolsHook(arena);
      const hook = (globalThis as Record<string, unknown>)['__WARP_CORE_DEVTOOLS__'] as Record<string, unknown>;
      assert.ok(hook);
      assert.strictEqual(hook.version, '0.1.0');
      assert.ok(typeof hook.inspectSignal === 'function');
      // Clean up
      delete (globalThis as Record<string, unknown>)['__WARP_CORE_DEVTOOLS__'];
    });
  });
});
