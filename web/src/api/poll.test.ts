import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Poller, type PollState } from "./poll";
import { ApiError } from "./client";

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

function deferred<T>() {
  let resolve!: (v: T) => void, reject!: (e: unknown) => void;
  const promise = new Promise<T>((a, b) => { resolve = a; reject = b; });
  return { promise, resolve, reject };
}

describe("Poller", () => {
  it("never overlaps requests and polls every interval after completion", async () => {
    let active = 0, maxActive = 0, calls = 0;
    const pending: ReturnType<typeof deferred<number>>[] = [];
    const p = new Poller<number>({
      intervalMs: 5000, onChange: () => {},
      load: () => { calls++; active++; maxActive = Math.max(maxActive, active); const d = deferred<number>(); pending.push(d); return d.promise.finally(() => active--); },
    });
    p.start();
    p.refresh(); p.refresh(); // ignored while in flight
    await vi.advanceTimersByTimeAsync(20000); // slow request: no new tick while pending
    expect(calls).toBe(1);
    pending[0]!.resolve(1);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(4999);
    expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toBe(2);
    expect(maxActive).toBe(1);
    p.stop();
  });

  it("backs off exponentially on errors and longer on 429, keeping stale data flagged", async () => {
    const states: PollState<number>[] = [];
    let n = 0;
    const p = new Poller<number>({
      intervalMs: 1000, maxBackoffMs: 60000, onChange: (s) => states.push(s),
      load: async () => { n++; if (n === 1) return 7; throw new ApiError(n === 2 ? "rate_limited" : "offline", 429, null, "x", true); },
    });
    expect(p.nextDelay(1, new ApiError("offline", null, null, "", true))).toBe(2000);
    expect(p.nextDelay(1, new ApiError("rate_limited", 429, null, "", true))).toBe(4000);
    expect(p.nextDelay(10, null)).toBe(60000);
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000); // 2nd call -> 429
    const last = states.at(-1)!;
    expect(last.data).toBe(7); // stale data retained...
    expect(last.error?.kind).toBe("rate_limited"); // ...but explicitly in error
    await vi.advanceTimersByTimeAsync(3999);
    expect(n).toBe(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(n).toBe(3);
    p.stop();
  });

  it("stops polling on 401", async () => {
    let n = 0;
    const p = new Poller({ intervalMs: 1000, onChange: () => {}, load: async () => { n++; throw new ApiError("unauthorized", 401, null, "", false); } });
    p.start();
    await vi.advanceTimersByTimeAsync(120000);
    expect(n).toBe(1);
  });

  it("pauses while hidden and resumes on visible", async () => {
    let hidden = false, n = 0;
    const states: PollState<number>[] = [];
    const p = new Poller<number>({ intervalMs: 1000, isHidden: () => hidden, onChange: (s) => states.push(s), load: async () => ++n });
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    hidden = true;
    p.visibilityChanged();
    await vi.advanceTimersByTimeAsync(60000);
    expect(n).toBe(1);
    expect(states.at(-1)!.paused).toBe(true);
    hidden = false;
    p.visibilityChanged();
    await vi.advanceTimersByTimeAsync(0);
    expect(n).toBe(2);
    p.stop();
  });

  it("stop() aborts the in-flight request and drops its result", async () => {
    let signal: AbortSignal | null = null;
    const d = deferred<number>();
    const states: PollState<number>[] = [];
    const p = new Poller<number>({ onChange: (s) => states.push(s), load: (s) => { signal = s; return d.promise; } });
    p.start();
    p.stop();
    expect(signal!.aborted).toBe(true);
    d.resolve(5);
    await vi.advanceTimersByTimeAsync(10000);
    expect(states.some((s) => s.data === 5)).toBe(false);
  });
});
