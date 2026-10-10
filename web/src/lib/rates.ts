// Counter-rate differencing. Counters are attempt-scoped (they restart at 0
// with each runtime attempt), so a change of attempt id — or any counter
// going backwards — resets the baseline instead of producing a negative or
// spurious rate. Counters are compared exactly as BigInt.
export interface Sample {
  attempt: string | null;
  at: number; // ms epoch
  value: bigint | null;
}

export type Rate =
  | { kind: "rate"; perSec: number }
  | { kind: "warming" } // first sample in this attempt
  | { kind: "reset" } // attempt changed or counter regressed
  | { kind: "unknown" }; // no value / no attempt

export function rate(prev: Sample | null, cur: Sample): Rate {
  if (cur.value === null || cur.attempt === null) return { kind: "unknown" };
  if (!prev || prev.value === null) return { kind: "warming" };
  if (prev.attempt !== cur.attempt || cur.value < prev.value) return { kind: "reset" };
  const dt = (cur.at - prev.at) / 1000;
  if (dt <= 0) return { kind: "warming" };
  return { kind: "rate", perSec: Number(cur.value - prev.value) / dt };
}

/** Keeps a bounded per-key series for sparklines; resets on attempt change. */
export class RateSeries {
  private last = new Map<string, Sample>();
  private series = new Map<string, number[]>();
  constructor(private readonly max = 30) {}

  push(key: string, cur: Sample): Rate {
    const r = rate(this.last.get(key) ?? null, cur);
    const prev = this.last.get(key);
    if (r.kind === "reset" || (prev && prev.attempt !== cur.attempt)) this.series.set(key, []);
    this.last.set(key, cur);
    if (r.kind === "rate") {
      const s = this.series.get(key) ?? [];
      s.push(r.perSec);
      if (s.length > this.max) s.shift();
      this.series.set(key, s);
    }
    return r;
  }

  get(key: string): number[] {
    return this.series.get(key) ?? [];
  }

  clear() {
    this.last.clear();
    this.series.clear();
  }
}
