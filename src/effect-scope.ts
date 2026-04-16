/**
 * @kronos/warp-core — Effect Scope
 *
 * Lightweight reactive scope that tracks signal subscriptions and tears
 * them all down on `stop()`. Each effect polls via `Atomics.waitAsync`
 * for zero postMessage overhead cross-thread reactivity.
 */

import {
  SignalArena,
  SignalFlags,
  INVALID_OFFSET,
} from './signal-arena';

export class EffectScope {
  private readonly links: number[] = [];
  private stopped = false;

  constructor(private readonly arena: SignalArena) {}

  /**
   * Register an effect callback that fires whenever the signal at
   * `signalOffset` changes value. The callback receives the new Int32 value.
   */
  effect(signalOffset: number, callback: (value: number) => void): void {
    if (this.stopped) return;

    const effectNode = this.arena.createComputed();
    if (effectNode === INVALID_OFFSET) return;

    this.arena.setFlags(effectNode, SignalFlags.Effect | SignalFlags.Mutable);
    const linkOffset = this.arena.addSubscriber(signalOffset, effectNode);
    if (linkOffset !== INVALID_OFFSET) this.links.push(linkOffset);

    const poll = (version: number): void => {
      if (this.stopped) return;
      this.arena.waitForChange(signalOffset, version)
        .then((newVersion) => {
          if (this.stopped) return;
          callback(this.arena.read(signalOffset));
          poll(newVersion);
        })
        .catch(() => { /* arena torn down or scope stopped */ });
    };

    poll(this.arena.version(signalOffset));
  }

  /** Tear down all subscriptions created by this scope. */
  stop(): void {
    this.stopped = true;
    for (const linkOffset of this.links) {
      const sigOffset = Atomics.load(this.arena.view, linkOffset + 0);
      // Walk the subscriber linked list to check if any other subscribers remain
      const subHead = Atomics.load(this.arena.view, sigOffset + 5); // F_SUB_HEAD
      let hasOtherSubscriber = false;
      let cursor = subHead;
      while (cursor !== -1) { // INVALID_OFFSET
        if (cursor !== linkOffset) {
          hasOtherSubscriber = true;
          break;
        }
        cursor = Atomics.load(this.arena.view, cursor + 3); // next link
      }
      if (!hasOtherSubscriber) {
        this.arena.clearFlags(sigOffset, SignalFlags.Watching);
      }
    }
    this.links.length = 0;
  }

  /** Whether this scope has been stopped. */
  get isStopped(): boolean {
    return this.stopped;
  }
}
