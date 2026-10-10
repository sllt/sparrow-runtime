import { useNavigate } from "react-router-dom";
import { HealthPill, Pill, Sparkline } from "../../components/ui";
import { checkpointRisk, type PipelineView } from "../../lib/health";
import { fmtRate } from "../../lib/format";
import { rateText, type RateInfo } from "./useRates";

export function PipelineTable({ views, rates, errors }: { views: PipelineView[]; rates: Record<string, RateInfo>; errors: Record<string, string> }) {
  const nav = useNavigate();
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="table">
        <thead>
          <tr>
            <th>流水线</th><th>状态</th><th>版本</th><th>输入速率</th><th>输出速率</th><th>Checkpoint</th><th className="num">连续失败</th>
          </tr>
        </thead>
        <tbody>
          {views.map((v) => {
            const r = rates[v.name];
            const risk = checkpointRisk(v.checkpoint);
            const go = () => nav(`/pipelines/${encodeURIComponent(v.name)}`);
            const mismatch = v.runningRevision !== null && v.revision !== null && v.runningRevision !== v.revision;
            return (
              <tr key={v.name} className="clickable" tabIndex={0} onClick={go} onKeyDown={(e) => e.key === "Enter" && go()}>
                <td>
                  <div className="row-name">{v.name}</div>
                  <div className="row-meta">{errors[v.name] ?? (v.attempt ? `attempt ${v.attempt}` : v.observationReason ? `无观测：${v.observationReason}` : "无运行 attempt")}</div>
                </td>
                <td><HealthPill health={v.health} /></td>
                <td>
                  <span className="mono">r{v.revision ?? "?"}</span>
                  <span className="muted"> / </span>
                  <span className="mono">{v.runningRevision ? `r${v.runningRevision}` : "—"}</span>
                  {mismatch && <> <Pill tone="warn" title="运行中的版本不是最新存储版本">非最新</Pill></>}
                </td>
                <td><div className="row"><Sparkline values={r?.inSeries ?? []} width={72} height={22} /><span className="small">{rateText(r?.inRate, fmtRate)}</span></div></td>
                <td><div className="row"><Sparkline values={r?.outSeries ?? []} width={72} height={22} /><span className="small">{rateText(r?.outRate, fmtRate)}</span></div></td>
                <td><Pill tone={risk.level} title={risk.detail}>{risk.label}</Pill></td>
                <td className="num">{v.failures ?? "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
