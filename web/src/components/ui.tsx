import { useEffect, useState, type ReactNode } from "react";
import { Icon } from "./Icon";
import { ApiError } from "../api/client";
import { fmtAgo } from "../lib/format";
import { HEALTH_LABEL, HEALTH_TONE, type Health } from "../lib/health";
import type { PollState } from "../api/poll";

export type Tone = "ok" | "warn" | "bad" | "info" | "idle" | "muted" | "na";

export function Pill({ tone, children, title }: { tone: Tone; children: ReactNode; title?: string }) {
  return (
    <span className={`pill tone-${tone}`} title={title}>
      <span className="dot" aria-hidden="true" />
      {children}
    </span>
  );
}

export function HealthPill({ health }: { health: Health }) {
  return <Pill tone={HEALTH_TONE[health]}>{HEALTH_LABEL[health]}</Pill>;
}

export function Card({ title, sub, actions, children, pad = true }: { title?: ReactNode; sub?: ReactNode; actions?: ReactNode; children: ReactNode; pad?: boolean }) {
  return (
    <section className="card">
      {title !== undefined && (
        <header className="card-head">
          <div>
            <h2 className="card-title">{title}</h2>
            {sub && <div className="card-sub">{sub}</div>}
          </div>
          {actions && <div className="page-actions">{actions}</div>}
        </header>
      )}
      {pad ? <div className="card-body">{children}</div> : children}
    </section>
  );
}

export function StateView({ icon, title, message, tone, action }: { icon: string; title: string; message?: ReactNode; tone?: Tone; action?: ReactNode }) {
  return (
    <div className="state" role="status">
      <div className={`state-icon ${tone ? `tone-${tone}` : ""}`}><Icon name={icon} size={22} /></div>
      <div className="state-title">{title}</div>
      {message && <div className="state-msg">{message}</div>}
      {action}
    </div>
  );
}

export function errorCopy(e: ApiError): { icon: string; title: string; message: string; tone: Tone } {
  switch (e.kind) {
    case "unauthorized":
      return { icon: "lock", title: "登录已失效", message: "服务端拒绝了当前令牌（401）。请重新登录；页面不会继续显示旧数据为健康。", tone: "bad" };
    case "forbidden":
      return { icon: "shield", title: "当前角色无权查看", message: `服务端拒绝了此操作（403）：${e.message}`, tone: "muted" };
    case "offline":
      return { icon: "wifiOff", title: "无法连接服务", message: "浏览器没有收到 Sparrow 的响应。显示的数据可能已经过期，系统会自动退避重试。", tone: "bad" };
    case "rate_limited":
      return { icon: "clock", title: "服务繁忙（429）", message: "管理接口容量已满，已自动降低刷新频率。", tone: "warn" };
    case "unavailable":
      return { icon: "clock", title: "服务暂不可用", message: e.message, tone: "warn" };
    case "not_found":
      return { icon: "inbox", title: "不存在", message: e.message, tone: "muted" };
    default:
      return { icon: "alert", title: "请求失败", message: `${e.code ? `${e.code}: ` : ""}${e.message}`, tone: "bad" };
  }
}

export function ErrorView({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  const c = errorCopy(error);
  return <StateView {...c} action={onRetry && error.kind !== "unauthorized" ? <button className="btn" onClick={onRetry}><Icon name="refresh" size={15} />重试</button> : undefined} />;
}

export function Skeleton({ rows = 4 }: { rows?: number }) {
  return (
    <div className="stack" aria-busy="true" aria-label="加载中" style={{ padding: 20 }}>
      {Array.from({ length: rows }, (_, i) => <div key={i} className="skeleton" style={{ width: `${90 - i * 12}%` }} />)}
    </div>
  );
}

function useNow(ms = 1000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

/** Live/paused/stale indicator for a poller; stale is shown explicitly. */
export function LiveIndicator<T>({ state, onRefresh }: { state: PollState<T>; onRefresh: () => void }) {
  const now = useNow();
  const cls = state.error ? "err" : state.paused ? "paused" : "";
  const label = state.error
    ? `刷新失败 ×${state.failures}，上次成功 ${fmtAgo(state.updatedAt, now)}`
    : state.paused
      ? "页面隐藏，已暂停刷新"
      : state.updatedAt
        ? `每 5 秒刷新 · ${fmtAgo(state.updatedAt, now)}`
        : "加载中…";
  return (
    <div className="row">
      <span className={`live ${cls}`} aria-live="polite"><span className="dot" />{label}</span>
      <button className="btn ghost icon" onClick={onRefresh} title="立即刷新 (R)" aria-label="立即刷新" disabled={state.loading}>
        <Icon name="refresh" size={16} />
      </button>
    </div>
  );
}

/** Banner shown whenever previously loaded data is no longer fresh. */
export function StaleBanner<T>({ state }: { state: PollState<T> }) {
  if (!state.error || state.data === null) return null;
  const c = errorCopy(state.error);
  return (
    <div className={`banner tone-${state.error.kind === "rate_limited" ? "warn" : "bad"}`} role="alert">
      <Icon name={c.icon} />
      <div><strong>以下数据已过期，不代表当前状态。</strong> {c.title}：{c.message}</div>
    </div>
  );
}

export function Sparkline({ values, width = 96, height = 28, tone = "accent" }: { values: number[]; width?: number; height?: number; tone?: string }) {
  if (values.length < 2) {
    return <svg width={width} height={height} aria-hidden="true"><line x1="0" x2={width} y1={height - 2} y2={height - 2} stroke="var(--border-strong)" strokeDasharray="3 3" /></svg>;
  }
  const max = Math.max(...values, 1e-9);
  const step = width / (values.length - 1);
  const pts = values.map((v, i) => [i * step, height - 2 - (v / max) * (height - 6)] as const);
  const d = pts.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
  const color = tone === "accent" ? "var(--accent)" : `var(--${tone}-dot)`;
  return (
    <svg width={width} height={height} aria-hidden="true">
      <path d={`${d}L${width},${height}L0,${height}Z`} fill={color} opacity={0.12} />
      <path d={d} fill="none" stroke={color} strokeWidth={1.6} strokeLinejoin="round" />
    </svg>
  );
}

export function Tabs<K extends string>({ tabs, value, onChange }: { tabs: { key: K; label: string; badge?: ReactNode }[]; value: K; onChange: (k: K) => void }) {
  return (
    <div className="tabs" role="tablist" onKeyDown={(e) => {
      const i = tabs.findIndex((t) => t.key === value);
      if (e.key === "ArrowRight") onChange(tabs[(i + 1) % tabs.length]!.key);
      if (e.key === "ArrowLeft") onChange(tabs[(i - 1 + tabs.length) % tabs.length]!.key);
    }}>
      {tabs.map((t) => (
        <button key={t.key} role="tab" className="tab" aria-selected={t.key === value} tabIndex={t.key === value ? 0 : -1} onClick={() => onChange(t.key)}>
          {t.label} {t.badge}
        </button>
      ))}
    </div>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return <kbd className="kbd">{children}</kbd>;
}
