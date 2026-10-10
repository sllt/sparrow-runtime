import { useMemo, useState } from "react";
import { Card, ErrorView, LiveIndicator, Skeleton, StaleBanner, StateView } from "../../components/ui";
import { HEALTH_LABEL, type Health } from "../../lib/health";
import { MAX_OVERVIEW } from "../../api/overview";
import { PipelineTable } from "./PipelineTable";
import { useOverview } from "./useOverview";

const FILTERS: { key: "all" | "attention" | Health; label: string }[] = [
  { key: "all", label: "全部" },
  { key: "running", label: HEALTH_LABEL.running },
  { key: "attention", label: "需要关注" },
  { key: "stopped", label: HEALTH_LABEL.stopped },
];

export default function PipelineListPage() {
  const { state, refresh, views, errors, rates } = useOverview();
  const [q, setQ] = useState("");
  const [f, setF] = useState<(typeof FILTERS)[number]["key"]>("all");
  const shown = useMemo(() => views.filter((v) => {
    if (q && !v.name.toLowerCase().includes(q.toLowerCase())) return false;
    if (f === "all") return true;
    if (f === "attention") return !["running", "stopped", "starting"].includes(v.health);
    return v.health === f;
  }), [views, q, f]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">流水线</h1>
          <div className="page-sub">{state.data ? `${views.length} 条${state.data.truncated ? `（仅展示前 ${MAX_OVERVIEW} 条）` : ""}` : "加载中…"}</div>
        </div>
        <div className="page-actions"><LiveIndicator state={state} onRefresh={refresh} /></div>
      </div>
      <StaleBanner state={state} />
      <Card pad={false} title={
        <div className="row" style={{ flexWrap: "wrap" }}>
          <input className="input search" placeholder="按名称筛选" value={q} onChange={(e) => setQ(e.target.value)} aria-label="按名称筛选" />
          <div className="row" role="radiogroup" aria-label="状态筛选" style={{ gap: 4 }}>
            {FILTERS.map((x) => (
              <button key={x.key} role="radio" aria-checked={f === x.key} className={`btn ${f === x.key ? "primary" : "ghost"}`} onClick={() => setF(x.key)}>{x.label}</button>
            ))}
          </div>
        </div>
      }>
        {state.data === null && state.error ? <ErrorView error={state.error} onRetry={refresh} />
          : state.data === null ? <Skeleton rows={6} />
          : shown.length === 0 ? <StateView icon="search" title={views.length ? "没有符合条件的流水线" : "还没有流水线"} message={views.length ? "调整筛选条件试试。" : "通过 API 发布流水线后会显示在这里。"} />
          : <PipelineTable views={shown} rates={rates} errors={errors} />}
      </Card>
    </>
  );
}
