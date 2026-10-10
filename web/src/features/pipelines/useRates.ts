import { useMemo, useRef } from "react";
import { bigint } from "../../api/json";
import { RateSeries, type Rate } from "../../lib/rates";
import type { PipelineView } from "../../lib/health";

export interface RateInfo { inRate: Rate; outRate: Rate; inSeries: number[]; outSeries: number[] }

/** Attempt-aware rates; recomputed only when a new sample arrives. */
export function useRates(views: PipelineView[], at: number | null): Record<string, RateInfo> {
  const series = useRef(new RateSeries(30));
  const lastAt = useRef<number | null>(null);
  const cache = useRef<Record<string, RateInfo>>({});
  return useMemo(() => {
    if (at === null || at === lastAt.current) return cache.current;
    lastAt.current = at;
    const out: Record<string, RateInfo> = {};
    for (const v of views) {
      const fresh = v.health !== "stale";
      const attempt = fresh ? v.attempt : null;
      const inRate = series.current.push(`${v.name}/in`, { attempt, at, value: bigint(v.ingested) });
      const outRate = series.current.push(`${v.name}/out`, { attempt, at, value: bigint(v.emitted) });
      out[v.name] = { inRate, outRate, inSeries: series.current.get(`${v.name}/in`), outSeries: series.current.get(`${v.name}/out`) };
    }
    cache.current = out;
    return out;
  }, [views, at]);
}

export function rateText(r: Rate | undefined, fmt: (n: number) => string): string {
  if (!r) return "—";
  switch (r.kind) {
    case "rate": return fmt(r.perSec);
    case "warming": return "采样中";
    case "reset": return "新 attempt";
    case "unknown": return "—";
  }
}
