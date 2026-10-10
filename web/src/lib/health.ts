import { obj, str, numText, approx, type Json } from "../api/json";
import type { PollState } from "../api/poll";

/**
 * Display states. `stale`, `unavailable` and `unknown` are deliberately
 * distinct from each other and from `running`: the UI must never show a
 * healthy state for data it has not freshly observed.
 */
export type Health =
  | "running" // fresh status, actual=running and live observation available
  | "degraded" // running but observation reports problems
  | "starting"
  | "stopped"
  | "failed"
  | "blocked" // restart_blocked / safe-mode hold
  | "unavailable" // server answered but observation not available (no attempt, busy)
  | "unknown" // no actual state recorded
  | "stale" // last data too old / polling failing
  | "denied"; // not authorised to read

export const STALE_AFTER_MS = 25_000;

export interface PipelineView {
  name: string;
  health: Health;
  actualStatus: string | null;
  desiredStatus: string | null;
  revision: string | null;
  runningRevision: string | null;
  attempt: string | null;
  failures: number | null;
  restartBlocked: boolean;
  hasError: boolean;
  observationReason: string | null;
  ingested: Json;
  emitted: Json;
  checkpoint: Record<string, Json> | null;
  reasons: string[];
}

export function deriveView(name: string, body: Json, fresh: boolean): PipelineView {
  const b = obj(body) ?? {};
  const actual = obj(b.actual);
  const desired = obj(b.desired);
  const obs = obj(b.observation);
  const progress = obj(obs?.runtime_progress);
  const actualStatus = str(actual?.status);
  const reasons = Array.isArray(obs?.diagnosis_reasons)
    ? (obs!.diagnosis_reasons as Json[]).map((r) => (typeof r === "string" ? r : JSON.stringify(r)))
    : [];
  const restartBlocked = actual?.restart_blocked === true;
  const hasError = actual?.has_error === true || (actual?.last_error !== undefined && actual?.last_error !== null);
  let health: Health;
  if (!fresh) health = "stale";
  else if (!actual) health = "unknown";
  else if (restartBlocked) health = "blocked";
  else if (actualStatus === "failed") health = "failed";
  else if (actualStatus === "stopped") health = "stopped";
  else if (actualStatus === "starting") health = "starting";
  else if (actualStatus === "running") {
    if (obs?.available !== true) health = "unavailable";
    else if (reasons.length > 0) health = "degraded";
    else health = "running";
  } else health = "unknown";
  return {
    name,
    health,
    actualStatus,
    desiredStatus: str(desired?.status),
    revision: numText(b.revision),
    runningRevision: numText(obs?.running_revision) ?? numText(actual?.revision),
    attempt: numText(actual?.attempt_id) ?? str(actual?.attempt_id),
    failures: approx(actual?.consecutive_failures),
    restartBlocked,
    hasError,
    observationReason: obs?.available === true ? null : str(obs?.reason),
    ingested: progress?.ingested_rows ?? null,
    emitted: progress?.emitted_rows ?? null,
    checkpoint: obj(b.checkpoint),
    reasons,
  };
}

export function isFresh<T>(s: PollState<T>, now: number): boolean {
  return s.updatedAt !== null && s.error === null && now - s.updatedAt < STALE_AFTER_MS;
}

export const HEALTH_LABEL: Record<Health, string> = {
  running: "运行中",
  degraded: "运行异常",
  starting: "启动中",
  stopped: "已停止",
  failed: "失败",
  blocked: "重启受阻",
  unavailable: "观测不可用",
  unknown: "状态未知",
  stale: "数据过期",
  denied: "无权限",
};

export const HEALTH_TONE: Record<Health, "ok" | "warn" | "bad" | "idle" | "muted" | "info"> = {
  running: "ok",
  degraded: "warn",
  starting: "info",
  stopped: "idle",
  failed: "bad",
  blocked: "bad",
  unavailable: "muted",
  unknown: "muted",
  stale: "warn",
  denied: "muted",
};

export interface CheckpointRisk {
  level: "ok" | "warn" | "bad" | "na";
  label: string;
  detail: string;
}

export function checkpointRisk(cp: Record<string, Json> | null): CheckpointRisk {
  if (!cp) return { level: "na", label: "无数据", detail: "状态响应中没有 checkpoint 字段" };
  if (cp.available !== true) {
    const r = str(cp.reason);
    return {
      level: "na",
      label: "不适用",
      detail: r === "no_active_aligned_attempt" ? "当前没有对齐恢复（aligned）的运行 attempt；不代表可以恢复。" : `不可用：${r ?? "unknown"}`,
    };
  }
  const failed = approx(cp.failed_or_cancelled_total) ?? 0;
  const ok = approx(cp.succeeded_total) ?? 0;
  const lastErr = str(cp.last_error_code);
  const age = approx(cp.last_success_age_ms);
  if (lastErr) return { level: "bad", label: "最近失败", detail: `最近一次 checkpoint 错误码 ${lastErr}` };
  if (ok === 0) return { level: "warn", label: "尚无成功", detail: "本 attempt 还没有成功的 checkpoint，重启将无法从本 attempt 恢复。" };
  if (age !== null && age > 10 * 60_000) return { level: "warn", label: "较久未成功", detail: `距上次成功 ${Math.round(age / 60000)} 分钟` };
  return { level: failed > 0 ? "warn" : "ok", label: failed > 0 ? `有 ${failed} 次失败` : "正常", detail: `成功 ${ok} 次，失败/取消 ${failed} 次` };
}
