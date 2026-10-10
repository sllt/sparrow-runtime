import { Link } from "react-router-dom";
import { useAuth } from "../../auth/AuthContext";
import { usePoll } from "../../api/usePoll";
import { obj, str, type Json } from "../../api/json";
import { Card, ErrorView, LiveIndicator, Pill, Skeleton, Sparkline, StaleBanner, StateView } from "../../components/ui";
import { fmtTime } from "../../lib/format";
import { Icon } from "../../components/Icon";
import { fmtClock, fmtRate } from "../../lib/format";
import { MAX_OVERVIEW } from "../../api/overview";
import { PipelineTable } from "../pipelines/PipelineTable";
import { useOverview } from "../pipelines/useOverview";
import type { Health } from "../../lib/health";

const ATTENTION: Health[] = ["failed", "blocked", "degraded", "stale", "unavailable", "unknown", "denied"];

function Kpi({ label, value, foot, icon, spark, tone }: { label: string; value: string; foot: string; icon: string; spark?: number[]; tone?: string }) {
  return (
    <section className="card kpi">
      <div className="kpi-label"><Icon name={icon} size={16} />{label}</div>
      <div className="kpi-value" style={tone ? { color: `var(--${tone})` } : undefined}>{value}</div>
      <div className="kpi-foot">{foot}</div>
      {spark && <div className="kpi-spark"><Sparkline values={spark} width={110} height={36} /></div>}
    </section>
  );
}

export default function DashboardPage() {
  const { me } = useAuth();
  const { state, refresh, views, errors, rates } = useOverview();
  const [audit] = usePoll((c, s) => c.get("/v1/audit", s), "dash-audit", 15_000);

  const running = views.filter((v) => v.health === "running").length;
  const attention = views.filter((v) => ATTENTION.includes(v.health)).length;
  const known = Object.values(rates).filter((r) => r.outRate.kind === "rate");
  const total = known.reduce((a, r) => a + (r.outRate.kind === "rate" ? r.outRate.perSec : 0), 0);
  const len = Math.max(0, ...Object.values(rates).map((r) => r.outSeries.length));
  const totalSeries = Array.from({ length: len }, (_, i) =>
    Object.values(rates).reduce((a, r) => a + (r.outSeries[r.outSeries.length - len + i] ?? 0), 0));
  const stale = state.error !== null;

  const auditRows = (obj(audit.data)?.audit as Json[] | undefined) ?? [];

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">总览</h1>
          <div className="page-sub">你好，{me?.actor}。这里是本实例所有流水线的实时健康情况。</div>
        </div>
        <div className="page-actions"><LiveIndicator state={state} onRefresh={refresh} /></div>
      </div>
      <StaleBanner state={state} />
      {state.data === null && state.error ? (
        <Card><ErrorView error={state.error} onRetry={refresh} /></Card>
      ) : (
        <div className="stack">
          <div className="grid kpis">
            <Kpi label="流水线" icon="pipelines" value={state.data ? String(views.length) : "…"} foot={state.data?.truncated ? `仅展示前 ${MAX_OVERVIEW} 条` : "已存储的流水线配置"} />
            <Kpi label="运行中" icon="activity" value={state.data ? (stale ? "?" : String(running)) : "…"} tone={stale ? "warn" : "ok"}
              foot={stale ? "数据过期，无法确认" : "状态新鲜且有实时观测"} />
            <Kpi label="需要关注" icon="alert" value={state.data ? String(attention) : "…"} tone={attention ? "bad" : undefined}
              foot="失败 / 受阻 / 异常 / 无观测 / 未知 / 过期" />
            <Kpi label="总输出速率" icon="layers" value={known.length ? fmtRate(total) : "—"} spark={totalSeries}
              foot={known.length ? `基于 ${known.length} 条流水线的本 attempt 计数差分` : "等待第二次采样"} />
          </div>
          <div className="grid two">
            <Card title="流水线健康" sub="点击行查看详情；速率按 attempt 差分，attempt 变化时重置" pad={false}
              actions={<Link to="/pipelines" className="btn ghost">全部<Icon name="chevron" size={14} /></Link>}>
              {state.data === null ? <Skeleton rows={5} /> : views.length === 0 ? (
                <StateView icon="inbox" title="还没有流水线" message="通过 API 发布流水线后会显示在这里。K5.2 起可在页面中创建。" />
              ) : <PipelineTable views={views.slice(0, 8)} rates={rates} errors={errors} />}
            </Card>
            <div className="stack">
              <Card title="实例">
                <dl className="kv">
                  <dt>身份</dt><dd>{me?.actor} <Pill tone="info">{me?.role}</Pill></dd>
                  <dt>鉴权模式</dt><dd className="mono">{me?.authMode}</dd>
                  <dt>安全模式</dt><dd>{me?.safeMode ? <Pill tone="warn">开启</Pill> : "关闭"}</dd>
                  <dt>排空</dt><dd>{me?.draining ? <Pill tone="warn">排空中</Pill> : "否"}</dd>
                </dl>
              </Card>
              <Card title="最近审计" sub="有界摘要，不是完整历史" actions={<Link to="/audit" className="btn ghost">更多</Link>} pad={false}>
                {audit.error && audit.data === null ? <ErrorView error={audit.error} /> : audit.data === null ? <Skeleton /> : auditRows.length === 0 ? (
                  <StateView icon="audit" title="暂无审计记录" />
                ) : (
                  <ul className="list" style={{ padding: "4px 20px" }}>
                    {auditRows.slice(0, 6).map((r, i) => {
                      const o = obj(r) ?? {};
                      return (
                        <li key={i}>
                          <Pill tone={str(o.outcome) === "ok" ? "ok" : str(o.outcome) === "failed" ? "bad" : "idle"}>{str(o.outcome) ?? "?"}</Pill>
                          <span className="mono">{str(o.action)}</span>
                          <span className="muted small" style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{str(o.target) ?? ""}</span>
                          <span className="spacer" />
                          <span className="small muted" style={{ whiteSpace: "nowrap" }} title={fmtTime(o.at_ms)}>{str(o.actor)} · {fmtClock(o.at_ms)}</span>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </Card>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
