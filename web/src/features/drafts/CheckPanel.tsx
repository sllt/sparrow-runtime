import { obj, str, type Json } from "../../api/json";
import { Card, Pill, StateView } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { fmtAgo } from "../../lib/format";

export interface CheckResult { raw: Record<string, Json>; etag: string; at: number }

function Err({ e }: { e: Json | undefined }) {
  const o = obj(e);
  return (
    <div className="err-box">
      <div className="row"><span className="chip">{str(o?.code) ?? "error"}</span><span className="muted small">HTTP {String(o?.status ?? "")}</span></div>
      <div className="mono small" style={{ whiteSpace: "pre-wrap", marginTop: 6 }}>{str(o?.message)}</div>
      {Array.isArray(o?.context) && (o!.context as Json[]).length > 0 && (
        <div className="small muted" style={{ marginTop: 4 }}>{(o!.context as Json[]).map((c) => `${str(obj(c)?.key)}=${str(obj(c)?.value)}`).join(" · ")}</div>
      )}
    </div>
  );
}

/** validate/explain are separate verdicts; they are never merged into one green tick. */
export function CheckPanel({ result, stale, onRun, running }: { result: CheckResult | null; stale: boolean; onRun: () => void; running: boolean }) {
  const r = result?.raw;
  const parse = obj(r?.parse), val = obj(r?.validate), exp = obj(r?.explain);
  const report = obj(exp?.report);
  const eff = obj(obj(val?.report)?.effective);
  return (
    <Card title="检查结果" sub={result ? <>基于 <span className="mono">{result.etag}</span> · {fmtAgo(result.at, Date.now())}</> : "完整 PipelineSpec 的 validate + explain"}
      actions={<button className="btn ghost" onClick={onRun} disabled={running}><Icon name="refresh" size={15} />{running ? "检查中…" : "重新检查"}</button>}>
      {!result ? (
        <StateView icon="check" title="尚未检查" message="检查会先保存草稿，再在服务端绑定计划、核对 I/O 策略、SecretRef 与恢复准入。不会连接外部端点。" />
      ) : (
        <div className="stack">
          {stale && <div className="banner tone-warn"><Icon name="alert" />草稿在检查后又被修改，以下结果已过期。</div>}
          <div className="check-row"><span>解析</span>{parse?.ok === true ? <Pill tone="ok">通过</Pill> : <Pill tone="bad">失败</Pill>}</div>
          {parse?.ok !== true && <Err e={parse?.error} />}
          {val && <>
            <div className="check-row"><span>校验（validate）</span>{val.ok === true ? <Pill tone="ok">通过</Pill> : <Pill tone="bad">拒绝</Pill>}</div>
            {val.ok !== true && <Err e={val.error} />}
          </>}
          {exp && <>
            <div className="check-row"><span>执行计划（explain）</span>{exp.ok === true ? <Pill tone="ok">可生成</Pill> : <Pill tone="bad">失败</Pill>}</div>
            {exp.ok !== true && <Err e={exp.error} />}
          </>}
          {report && (
            <dl className="kv">
              <dt>交付</dt><dd><span className="chip">{str(report.delivery)}</span></dd>
              <dt>恢复</dt><dd><span className="chip">{str(report.recovery)}</span>{report.replay ? <> · replay <span className="chip">{str(report.replay)}</span></> : null}</dd>
              <dt>时间语义</dt><dd className="small">{typeof report.time === "string" ? report.time : JSON.stringify(report.time)}</dd>
              <dt>状态</dt><dd className="small">{typeof report.state === "string" ? report.state : JSON.stringify(report.state)}</dd>
              <dt>保证说明</dt><dd className="small">{str(report.guarantee)}</dd>
              {eff && <><dt>对齐恢复</dt><dd>{eff.aligned_eligible === true ? <Pill tone="ok">可用</Pill> : eff.aligned_eligible === false ? <Pill tone="idle">不适用</Pill> : <Pill tone="muted">未知</Pill>} <span className="small muted">{str(eff.aligned_eligibility_reason)}</span></dd></>}
              {report.experimental === true && <><dt>注意</dt><dd><Pill tone="warn">实验性</Pill></dd></>}
            </dl>
          )}
          {report && Array.isArray(report.physical) && (
            <details className="raw"><summary>物理计划（只读）</summary><pre className="mono small pre">{(report.physical as Json[]).map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join("\n")}</pre></details>
          )}
          <div className="hint">校验在当前时刻检查完整 PipelineSpec 的计划绑定、I/O 策略、SecretRef 与恢复准入；不会连接外部端点。发布时会重新校验。</div>
        </div>
      )}
    </Card>
  );
}
