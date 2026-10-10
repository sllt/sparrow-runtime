// Bounded polling: one request in flight at most, pauses while the document
// is hidden, exponential backoff on 429/errors, aborts on stop (navigation
// or logout). Pure class so it is unit-testable with fake timers.
import { ApiError } from "./client";

export interface PollState<T> {
  data: T | null;
  error: ApiError | null;
  /** ms epoch of the last successful response */
  updatedAt: number | null;
  loading: boolean;
  /** consecutive failures since the last success */
  failures: number;
  paused: boolean;
}

export interface PollerOptions<T> {
  load: (signal: AbortSignal) => Promise<T>;
  intervalMs?: number;
  maxBackoffMs?: number;
  onChange: (s: PollState<T>) => void;
  isHidden?: () => boolean;
  now?: () => number;
}

export class Poller<T> {
  private state: PollState<T> = { data: null, error: null, updatedAt: null, loading: false, failures: 0, paused: false };
  private timer: ReturnType<typeof setTimeout> | null = null;
  private controller: AbortController | null = null;
  private stopped = false;
  private readonly interval: number;
  private readonly maxBackoff: number;

  constructor(private readonly opts: PollerOptions<T>) {
    this.interval = opts.intervalMs ?? 5000;
    this.maxBackoff = opts.maxBackoffMs ?? 60000;
  }

  private set(patch: Partial<PollState<T>>) {
    this.state = { ...this.state, ...patch };
    this.opts.onChange(this.state);
  }

  get inFlight(): boolean {
    return this.controller !== null;
  }

  start() {
    this.stopped = false;
    void this.tick();
  }

  /** Abort the in-flight request and cancel timers. */
  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.controller?.abort();
    this.controller = null;
  }

  /** Manual refresh; ignored while a request is in flight (no overlap). */
  refresh() {
    if (this.stopped || this.inFlight) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    void this.tick();
  }

  /** Call on visibilitychange. */
  visibilityChanged() {
    if (this.stopped) return;
    const hidden = this.opts.isHidden?.() ?? false;
    if (hidden) {
      if (this.timer) clearTimeout(this.timer);
      this.timer = null;
      this.set({ paused: true });
    } else if (this.state.paused) {
      this.set({ paused: false });
      this.refresh();
    }
  }

  nextDelay(failures: number, err: ApiError | null): number {
    if (failures === 0) return this.interval;
    const base = err?.kind === "rate_limited" ? this.interval * 2 : this.interval;
    return Math.min(this.maxBackoff, base * 2 ** Math.min(failures, 6));
  }

  private schedule(delay: number) {
    if (this.stopped) return;
    if (this.opts.isHidden?.()) {
      this.set({ paused: true });
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.tick();
    }, delay);
  }

  private async tick() {
    if (this.stopped || this.inFlight) return;
    if (this.opts.isHidden?.()) {
      this.set({ paused: true });
      return;
    }
    const controller = new AbortController();
    this.controller = controller;
    this.set({ loading: true, paused: false });
    try {
      const data = await this.opts.load(controller.signal);
      if (controller.signal.aborted || this.stopped) return;
      this.controller = null;
      this.set({ data, error: null, updatedAt: (this.opts.now ?? Date.now)(), loading: false, failures: 0 });
      this.schedule(this.interval);
    } catch (e) {
      if (controller.signal.aborted || this.stopped) return;
      this.controller = null;
      const err = e instanceof ApiError ? e : new ApiError("bad_response", null, null, String(e), false);
      const failures = this.state.failures + 1;
      // Keep last data but it becomes stale; the UI must show that distinctly.
      this.set({ error: err, loading: false, failures });
      // Never keep hammering after authentication is gone.
      if (err.kind === "unauthorized") return;
      this.schedule(this.nextDelay(failures, err));
    }
  }
}
