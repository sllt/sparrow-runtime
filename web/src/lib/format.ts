import { numText } from "../api/json";

const nf = new Intl.NumberFormat("zh-CN");

/** Exact integer text with grouping (works for values beyond 2^53). */
export function fmtInt(v: unknown): string {
  const t = numText(v);
  if (t === null) return "—";
  if (/^-?\d+$/.test(t)) return t.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return t;
}

export function fmtRate(perSec: number): string {
  if (perSec >= 1000) return `${nf.format(Math.round(perSec / 100) / 10)}k/s`;
  return `${perSec >= 10 ? Math.round(perSec) : +perSec.toFixed(1)}/s`;
}

export function fmtBytes(v: unknown): string {
  const t = numText(v);
  if (t === null) return "—";
  const n = Number(t);
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  return `${(n / 1024 ** 3).toFixed(2)} GiB`;
}

export function fmtAgo(ms: number | null, now = Date.now()): string {
  if (ms === null) return "从未";
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 5) return "刚刚";
  if (s < 60) return `${s} 秒前`;
  if (s < 3600) return `${Math.floor(s / 60)} 分钟前`;
  return `${Math.floor(s / 3600)} 小时前`;
}

export function fmtTime(ms: unknown): string {
  const t = numText(ms);
  if (t === null) return "—";
  const d = new Date(Number(t));
  return d.toLocaleString("zh-CN", { hour12: false });
}

export function fmtClock(ms: unknown): string {
  const t = numText(ms);
  if (t === null) return "—";
  return new Date(Number(t)).toLocaleTimeString("zh-CN", { hour12: false });
}
