# @kronos/warp-core

> Sub-microsecond reactive runtime — SharedArrayBuffer-backed signals with true cross-thread propagation, zero postMessage overhead, and fleet-aware effect scheduling.

## Why

Existing signal libraries (Alien Signals, Preact Signals, SolidJS) operate within a single thread. Cross-worker reactivity requires serializing dependency graphs through `postMessage`, adding milliseconds of overhead per notification.

**`@kronos/warp-core`** eliminates this entirely:

| Feature | Traditional Signals | Warp Core |
|---------|-------------------|-----------|
| Cross-thread propagation | postMessage (1-5ms) | Atomics.notify (~0.1μs) |
| Memory model | Per-thread copies | Single SharedArrayBuffer |
| Dependency tracking | Per-thread graph | One arena, all threads |
| Effect scheduling | Microtask queue | CAS + waitAsync wakeup |
| Store API | Framework-specific | Framework-agnostic Proxy |

## Install

```bash
npm install @kronos/warp-core
```

## Quick Start

### Low-level signals (SAB-backed)

```typescript
import { SignalArena, EffectScope } from '@kronos/warp-core';

// Create an arena (shared across all Workers)
const arena = SignalArena.create();
const counter = arena.createSignal(0);

// Set up reactive effects
const scope = new EffectScope(arena);
scope.effect(counter, (value) => {
  console.log('Counter changed:', value);
});

// Write from any thread — all effects fire
arena.write(counter, 42);

// Pass to Workers
worker.postMessage({ buffer: arena.buffer }, []);
// In Worker: const arena = SignalArena.from(buffer);
```

### High-level reactive store

```typescript
import { createStore } from '@kronos/warp-core';

const store = createStore({ count: 0, name: 'KRONOS' });

store.on('count', (newVal, oldVal) => {
  console.log(`count: ${oldVal} → ${newVal}`);
});

store.state.count = 1; // triggers listener

// Batch updates
store.batch((s) => {
  s.count = 10;
  s.name = 'Warp';
}); // listeners fire after all writes
```

### Cross-context sync

```typescript
import { WorkerSync } from '@kronos/warp-core';

const sync = new WorkerSync({ nodeId: 'main-thread' });

sync.on('state-update', (msg) => {
  console.log('Received update from:', msg.sourceId, msg.payload);
});

sync.send({ type: 'state-update', payload: { count: 42 } });
```

### Devtools

```typescript
import { SignalArena, installDevtoolsHook, arenaStats, scanSignals } from '@kronos/warp-core';

const arena = SignalArena.create();
installDevtoolsHook(arena);

// Inspect from console:
// globalThis.__WARP_CORE_DEVTOOLS__.arenaStats()
// globalThis.__WARP_CORE_DEVTOOLS__.scanSignals(10)
```

## API

### Signal Arena

- **`SignalArena.create(byteSize?)`** — Create a new arena (default 64 MB)
- **`SignalArena.from(buffer)`** — Reconstruct from SharedArrayBuffer
- **`arena.createSignal(initialValue)`** — Allocate a mutable signal
- **`arena.createComputed()`** — Allocate a computed node
- **`arena.read(offset)`** — Read signal value (atomic)
- **`arena.write(offset, value)`** — Write value (CAS-locked, version-bumped)
- **`arena.waitForChange(offset, version)`** — Async wait for next write
- **`arena.addSubscriber(signal, subscriber)`** — Link subscriber to signal

### Effect Scope

- **`new EffectScope(arena)`** — Create a scope
- **`scope.effect(signalOffset, callback)`** — Register reactive effect
- **`scope.stop()`** — Tear down all subscriptions

### Reactive Store

- **`createStore(initial)`** — Create a Proxy-based store
- **`store.on(field, listener)`** — Subscribe to field changes
- **`store.onAny(listener)`** — Subscribe to all changes
- **`store.batch(updater)`** — Batch multiple writes
- **`store.snapshot()`** — Get a shallow copy

### Worker Sync

- **`new WorkerSync(config)`** — Create a cross-context sync bus
- **`sync.on(type, listener)`** — Subscribe to message types
- **`sync.send(message)`** — Send to all other nodes
- **`sync.close()`** — Close the channel

### Devtools

- **`inspectSignal(arena, offset)`** — Get signal snapshot
- **`arenaStats(arena)`** — Get arena utilization
- **`scanSignals(arena, maxCount?)`** — Scan all allocated signals
- **`installDevtoolsHook(arena)`** — Expose on globalThis

## License

MIT
