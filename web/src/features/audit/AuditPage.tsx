import { useMemo, useState } from "react";
import { usePoll } from "../../api/usePoll";
import { useAuth } from "../../auth/AuthContext";
import { obj, str, type Json } from "../../api/json";
import { Card, ErrorView, LiveIndicator, Pill, Skeleton, StaleBanner, StateView } from "../../components/ui";
import { fmtTime } from "../../lib/format";

export default function AuditPage() {
  const { me } = useAuth();
  const [state, refresh] = usePoll((c, s) => c.get("/v1/audit", s), "audit", 15_000);
  const [q, setQ] = useState("");
  const rows = useMemo(() => {
    const all = ((obj(state.data)?.audit as Json[] | undefined) ?? []).map((r) => obj(r) ?? {});
    const s = q.toLowerCase();
    return s ? all.filter((r) => [r.action, r.actor, r.target].some((x) => str(x)?.toLowerCase().includes(s))) : all;
  }, [state.data, q]);
  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">审计摘要</h1>
          <div className="page-sub">最近 50 条管理操作。这是有界摘要，不是完整发布史{me?.role === "viewer" ? "；只读角色不显示详情字段" : ""}。</div>
        </div>
        <div className="page-actions"><LiveIndicator state={state} onRefresh={refresh} /></div>
      </div>
      <StaleBanner state={state} />
      <Card pad={false} title={<input className="input search" placeholder="筛选动作 / 身份 / 目标" value={q} onChange={(e) => setQ(e.target.value)} aria-label="筛选" />}>
        {state.data === null && state.error ? <ErrorView error={state.error} onRetry={refresh} />
          : state.data === null ? <Skeleton rows={6} />
          : rows.length === 0 ? <StateView icon="audit" title="暂无审计记录" message="鉴权失败不会写入审计（防放大设计）。" />
          : (
            <table className="table">
              <thead><tr><th>时间</th><th>身份</th><th>动作</th><th>目标</th><th>结果</th>{me?.role !== "viewer" && <th>详情</th>}</tr></thead>
              <tbody>{rows.map((r, i) => (
                <tr key={i}>
                  <td className="small">{fmtTime(r.at_ms)}</td>
                  <td>{str(r.actor)}</td>
                  <td className="mono">{str(r.action)}</td>
                  <td className="mono small">{str(r.target) ?? "—"}</td>
                  <td><Pill tone={str(r.outcome) === "ok" ? "ok" : str(r.outcome) === "failed" ? "bad" : "idle"}>{str(r.outcome) ?? "?"}</Pill></td>
                  {me?.role !== "viewer" && <td className="small muted">{str(r.detail) ?? ""}</td>}
                </tr>
              ))}</tbody>
            </table>
          )}
      </Card>
    </>
  );
}
