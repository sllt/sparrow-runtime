import { approx } from "../api/json";

export type Quantile =
  | { kind: "bound"; upperUs: number | null; samples: number } // null upper = overflow (unbounded)
  | { kind: "insufficient"; samples: number; min: number }
  | { kind: "unavailable" };

/** p99 as reported by the server: an upper bucket bound, never an exact latency. */
export function p99(hist: unknown, minSamples = 100): Quantile {
  if (!hist || typeof hist !== "object") return { kind: "unavailable" };
  const h = hist as Record<string, unknown>;
  const samples = approx(h.samples);
  if (samples === null) return { kind: "unavailable" };
  if (samples < minSamples) return { kind: "insufficient", samples, min: minSamples };
  const up = h.p99_upper_us;
  return { kind: "bound", upperUs: up === null || up === undefined ? null : approx(up), samples };
}

export function fmtUs(us: number): string {
  if (us < 1000) return `${us} µs`;
  if (us < 1_000_000) return `${+(us / 1000).toFixed(1)} ms`;
  return `${+(us / 1_000_000).toFixed(2)} s`;
}

export function quantileText(q: Quantile): string {
  switch (q.kind) {
    case "bound":
      return q.upperUs === null ? "> 最大桶（无上界）" : `≤ ${fmtUs(q.upperUs)}`;
    case "insufficient":
      return `样本不足（${q.samples}/${q.min}）`;
    case "unavailable":
      return "无观测";
  }
}
