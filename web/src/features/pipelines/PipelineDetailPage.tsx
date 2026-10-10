import { useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { usePoll } from "../../api/usePoll";
import { useAuth } from "../../auth/AuthContext";
import { enc } from "../../api/client";
import { approx, bigint, numText, obj, str, type Json } from "../../api/json";
import { Card, ErrorView, HealthPill, LiveIndicator, Pill, Skeleton, Sparkline, StaleBanner, StateView, Tabs, type Tone } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { checkpointRisk, deriveView, isFresh, REASON_LABEL, reasonTone } from "../../lib/health";
import { fmtBytes, fmtInt, fmtRate } from "../../lib/format";
import { p99, quantileText, fmtUs } from "../../lib/histogram";
import { RateSeries, type Rate } from "../../lib/rates";
import { rateText } from "./useRates";
import { RevisionsTab } from "./RevisionsTab";
import { OpsTab, SafetyCard } from "./OpsTab";
import { OutcomeBanner, outcomeOf, type Outcome } from "./RevisionActions";

type TabKey = "overview" | "traffic" | "errors" | "checkpoint" | "diagnostics" | "revisions" | "ops";

function v(x: Json | undefined): string {
  if (x === null || x === undefined) return "—";
  if (typeof x === "boolean") return x ? "是" : "否";
  return numText(x) ?? (typeof x === "string" ? x : JSON.stringify(x));
}

function Unavailable({ what, reason }: { what: string; reason: string | null }) {
  const copy: Record<string, string> = {
    no_active_attempt: "当前没有运行中的 attempt，因此没有实时观测数据。这不代表流水线健康。",
    registry_busy: "服务端观测注册表繁忙，本次未取到数据（不是零值）。",
    initializing: "观测通道正在初始化。",
    unobserved_channel: "该通道没有观测器。",
    no_active_aligned_attempt: "没有对齐恢复（aligned）的运行 attempt；checkpoint 不适用。",
  };
  return <StateView icon="clock" tone="muted" title={`${what}不可用`} message={<>{copy[reason ?? ""] ?? "服务端未提供该数据。"}<div className="small mono" style={{ marginTop: 6 }}>reason: {reason ?? "unknown"}</div></>} />;
}

function QueueCard({ title, q }: { title: string; q: Record<string, Json> | null }) {
  if (!q || q.available !== true) return <Card title={title}><Unavailable what="队列观测" reason={str(q?.reason)} /></Card>;
  const cap = approx(q.capacity_items) ?? 0;
  const items = approx(q.queued_items) ?? 0;
  const pct = cap > 0 ? Math.min(100, (items / cap) * 100) : 0;
  const tone = pct > 80 ? "bad" : pct > 50 ? "warn" : "";
  const res = p99(q.residence);
  const wait = p99(q.capacity_wait);
  return (
    <Card title={title} sub="背压：队列占用与容量等待">
      <div className="row small" style={{ marginBottom: 6 }}><span>占用 {fmtInt(q.queued_items)} / {fmtInt(q.capacity_items)}</span><span className="spacer" /><span className="muted">{pct.toFixed(0)}%</span></div>
      <div className={`bar ${tone}`} role="meter" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100} aria-label={`${title}占用`}><span style={{ width: `${pct}%` }} /></div>
      <dl className="kv" style={{ marginTop: 16 }}>
        <dt>排队字节（逻辑）</dt><dd>{fmtBytes(q.queued_logical_bytes)}</dd>
        <dt>峰值</dt><dd>{fmtInt(q.peak_items)} 项 / {fmtBytes(q.peak_logical_bytes)}</dd>
        <dt>最老排队时长</dt><dd>{approx(q.oldest_queued_age_us) !== null ? fmtUs(approx(q.oldest_queued_age_us)!) : "—"}</dd>
        <dt>等待中的发送者</dt><dd>{v(q.waiting_senders)}</dd>
        <dt>容量等待次数</dt><dd>{fmtInt(q.capacity_waits_total)}</dd>
        <dt>驻留 p99</dt><dd title="服务端直方图 bucket 上界，不是精确延迟">{quantileText(res)}</dd>
        <dt>容量等待 p99</dt><dd title="服务端直方图 bucket 上界，不是精确延迟">{quantileText(wait)}</dd>
      </dl>
    </Card>
  );
}

function Endpoint({ name, e }: { name: string; e: Record<string, Json> | null }) {
  if (!e) return <li><span className="row-name" style={{ width: 64 }}>{name}</span><Pill tone="muted">无观测</Pill></li>;
  const st = str(e.state) ?? "unknown";
  const tone: Tone = st === "healthy" || st === "connected" || st === "ready" ? "ok" : st === "failed" || st === "error" ? "bad" : st === "unknown" ? "muted" : "warn";
  return (
    <li>
      <span className="row-name" style={{ width: 64 }}>{name}</span>
      <Pill tone={tone}>{st}</Pill>
      {str(e.reason) && <span className="chip">{str(e.reason)}</span>}
      {str(e.last_error_code) && <span className="chip" style={{ color: "var(--bad)" }}>{str(e.last_error_code)}</span>}
      <span className="spacer" />
      <span className="small muted">失败 {fmtInt(e.failures_total)} 次</span>
    </li>
  );
}

export default function PipelineDetailPage() {
  const { name = "" } = useParams();
  const { client, me } = useAuth();
  const [tab, setTab] = useState<TabKey>("overview");
  const [state, refresh] = usePoll((c, s) => c.get(`/v1/pipelines/${enc(name)}/status`, s), `status:${name}`);
  const fresh = isFresh(state, Date.now());
  const view = useMemo(() => deriveView(name, state.data, fresh), [name, state.data, fresh]);
  const body = obj(state.data) ?? {};
  const obs = obj(body.observation);
  const cp = obj(body.checkpoint);
  const risk = checkpointRisk(cp);

  const series = useMemo(() => new RateSeries(40), [name]);
  const rates = useMemo(() => {
    const at = state.updatedAt;
    if (at === null) return null;
    const attempt = fresh ? view.attempt : null;
    const prog = obj(obs?.runtime_progress);
    return {
      in: series.push("in", { attempt, at, value: bigint(prog?.ingested_rows) }),
      out: series.push("out", { attempt, at, value: bigint(prog?.emitted_rows) }),
      filt: series.push("filt", { attempt, at, value: bigint(prog?.transform_filtered_rows) }),
    } as Record<string, Rate>;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.updatedAt]);

  const [cpReq, setCpReq] = useState<{ busy: boolean; out: Outcome | null }>({ busy: false, out: null });
  async function requestCheckpoint() {
    if (!client) return;
    setCpReq({ busy: true, out: null });
    try {
      const r = obj(await client.send("POST", `/v1/pipelines/${enc(name)}/checkpoint`, {}));
      setCpReq({ busy: false, out: { kind: "ok", text: `checkpoint ${numText(r?.checkpoint_id)} 已提交。` } });
    } catch (e) {
      // A timeout is "pending confirmation": keep polling the real phase, never retry the mutation.
      setCpReq({ busy: false, out: outcomeOf(e) });
    } finally { refresh(); }
  }

  const [dl, setDl] = useState<{ busy: boolean; msg: string | null; tone: Tone }>({ busy: false, msg: null, tone: "info" });
  async function download() {
    if (!client) return;
    setDl({ busy: true, msg: null, tone: "info" });
    try {
      const text = await client.getText(`/v1/pipelines/${enc(name)}/diagnose`);
      const url = URL.createObjectURL(new Blob([text], { type: "application/json" }));
      const a = document.createElement("a");
      a.href = url;
      a.download = `sparrow-diagnostic-${name}-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 0);
      setDl({ busy: false, msg: `已下载 ${(text.length / 1024).toFixed(1)} KiB 脱敏诊断包`, tone: "ok" });
    } catch (e) {
      setDl({ busy: false, msg: e instanceof Error ? e.message : "下载失败", tone: "bad" });
    }
  }

  if (state.data === null && state.error) return <Card><ErrorView error={state.error} onRetry={refresh} /></Card>;
  if (state.data === null) return <Card><Skeleton rows={8} /></Card>;

  const mismatch = view.runningRevision !== null && view.revision !== null && view.runningRevision !== view.revision;
  const prog = obj(obs?.runtime_progress);
  const delivery = obj(obs?.delivery);
  const actual = obj(body.actual);

  return (
    <>
      <div className="page-head">
        <div>
          <div className="row"><h1 className="page-title">{name}</h1><HealthPill health={view.health} /></div>
          <div className="page-sub">
            最新版本 <span className="mono">r{view.revision ?? "?"}</span> · 运行版本 <span className="mono">{view.runningRevision ? `r${view.runningRevision}` : "—"}</span>
            {view.attempt && <> · attempt <span className="mono">{view.attempt}</span></>}
            {str(body.projection) === "viewer_safe" && <> · <span title="服务端返回的只读安全投影">安全投影</span></>}
          </div>
        </div>
        <div className="page-actions"><LiveIndicator state={state} onRefresh={refresh} /></div>
      </div>
      <StaleBanner state={state} />
      {mismatch && <div className="banner tone-warn"><Icon name="alert" /><div><strong>运行版本与最新存储版本不同。</strong> 存储了新版本不代表已在运行；以下观测属于运行中的 r{view.runningRevision}。</div></div>}
      {view.restartBlocked && <div className="banner tone-bad"><Icon name="alert" /><div><strong>重启受阻。</strong> 连续失败 {view.failures ?? "?"} 次，自动重启已停止。</div></div>}

      <Tabs<TabKey> value={tab} onChange={setTab} tabs={[
        { key: "overview", label: "概览" },
        { key: "traffic", label: "流量与背压" },
        { key: "errors", label: "错误", badge: view.hasError || view.health === "degraded" ? <Pill tone="bad">!</Pill> : undefined },
        { key: "checkpoint", label: "Checkpoint 风险" },
        { key: "diagnostics", label: "诊断" },
        ...(me?.role !== "viewer" ? [{ key: "revisions" as const, label: "版本历史" }, { key: "ops" as const, label: "运维处置" }] : []),
      ]} />
      {tab === "ops" && (
        <div className="stack">
          <SafetyCard safeMode={body.safe_mode === true} restartBlocked={view.restartBlocked} failures={view.failures === null ? null : String(view.failures)} lastError={str(actual?.last_error_code) ?? str(actual?.last_error)} />
          <OpsTab name={name} admin={me?.role === "admin"} stopped={str(obj(body.desired)?.status) === "stopped" && str(actual?.status) !== "running"} revision={view.runningRevision ?? view.revision} />
        </div>
      )}
      {tab === "revisions" && <RevisionsTab name={name} />}

      {tab === "overview" && (
        <div className="grid two">
          <Card title="期望与实际">
            <dl className="kv">
              <dt>期望状态（desired）</dt><dd>{v(obj(body.desired)?.status)} {obj(body.desired) && <span className="mono muted">r{v(obj(body.desired)?.revision)}</span>}</dd>
              <dt>实际状态（actual）</dt><dd>{actual ? <>{v(actual.status)} <span className="mono muted">r{v(actual.revision)}</span></> : <Pill tone="muted">未记录</Pill>}</dd>
              <dt>运行 attempt</dt><dd className="mono">{view.attempt ?? "—"}</dd>
              <dt>连续失败</dt><dd>{v(actual?.consecutive_failures)}</dd>
              <dt>重启受阻</dt><dd>{v(actual?.restart_blocked)}</dd>
              <dt>交付语义</dt><dd className="mono">{v(body.delivery)}</dd>
              <dt>恢复（effective）</dt><dd className="mono">{v(obj(body.effective)?.recovery)}</dd>
              <dt>重放</dt><dd className="mono">{v(body.replay)}</dd>
            </dl>
          </Card>
          <Card title="本 attempt 进度" sub="计数器只属于当前 attempt，不是外部确认">
            {obs?.available !== true ? <Unavailable what="实时观测" reason={str(obs?.reason)} /> : (
              <div className="grid cols-3">
                {([["输入", "in", prog?.ingested_rows], ["输出", "out", prog?.emitted_rows], ["过滤", "filt", prog?.transform_filtered_rows]] as const).map(([l, k, total]) => (
                  <div key={k}>
                    <div className="muted small">{l}</div>
                    <div style={{ fontSize: 22, fontWeight: 650 }}>{rateText(rates?.[k], fmtRate)}</div>
                    <Sparkline values={series.get(k)} width={120} height={30} />
                    <div className="small muted">累计 {fmtInt(total)}</div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>
      )}

      {tab === "traffic" && (
        obs?.available !== true ? <Card><Unavailable what="流量观测" reason={str(obs?.reason)} /></Card> : (
          <div className="stack">
            <div className="grid cols-2">
              <QueueCard title="Source 入站队列" q={obj(obs.source_inbox)} />
              <QueueCard title="Sink 出站队列" q={obj(obs.sink_outbox)} />
            </div>
            <Card title="交付（本 attempt）">
              <dl className="kv">
                <dt>在途输入批次</dt><dd>{fmtInt(delivery?.active_input_batches)}（{fmtInt(delivery?.active_rows)} 行）</dd>
                <dt>在途 HTTP 请求</dt><dd>{fmtInt(delivery?.active_http_requests)}</dd>
                <dt>已完成批次 / 行</dt><dd>{fmtInt(delivery?.completed_batches_total)} / {fmtInt(delivery?.completed_rows_total)}</dd>
                <dt>编码额度峰值</dt><dd>{fmtBytes(delivery?.peak_encoded_credit_bytes)}</dd>
              </dl>
            </Card>
          </div>
        )
      )}

      {tab === "errors" && (
        <div className="grid two">
          <Card title="端点状态" sub="静默不等于失败（silence_is_failure=false）">
            {obs?.available !== true ? <Unavailable what="端点观测" reason={str(obs?.reason)} /> : (
              <ul className="list"><Endpoint name="Source" e={obj(obs.source)} /><Endpoint name="Sink" e={obj(obs.sink)} /></ul>
            )}
          </Card>
          <Card title="错误摘要">
            <dl className="kv">
              <dt>最近错误</dt><dd>{view.hasError ? <Pill tone="bad">有错误记录</Pill> : actual ? <Pill tone="ok">无</Pill> : <Pill tone="muted">未知</Pill>}</dd>
              {me?.role !== "viewer" && str(actual?.last_error) && <><dt>错误信息</dt><dd className="mono small">{str(actual?.last_error)}</dd></>}
              <dt>诊断原因</dt><dd>{view.reasons.length ? <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>{view.reasons.map((r) => <Pill key={r} tone={reasonTone(r)} title={r}>{REASON_LABEL[r] ?? r}</Pill>)}</div> : obs?.available === true ? "无" : "—"}</dd>
            </dl>
            {me?.role === "viewer" && view.hasError && <p className="hint" style={{ marginTop: 12 }}>只读角色看不到原始错误文本（可能含敏感信息）；可下载脱敏诊断包或联系操作员。</p>}
          </Card>
        </div>
      )}

      {tab === "checkpoint" && (
        <div className="grid two">
          <Card title="恢复风险" actions={<>{me?.role !== "viewer" && <button className="btn sm" disabled={cpReq.busy} onClick={() => void requestCheckpoint()}><Icon name="save" size={13} />{cpReq.busy ? "等待提交…" : "请求 checkpoint"}</button>}<Pill tone={risk.level}>{risk.label}</Pill></>}>
            <p style={{ marginTop: 0 }}>{risk.detail}</p>
            <OutcomeBanner o={cpReq.out} />
            {cp?.available === true && (
              <dl className="kv">
                <dt>策略</dt><dd className="mono">{v(cp.policy)}</dd>
                <dt>阶段</dt><dd className="mono">{v(cp.phase)}</dd>
                <dt>成功 / 失败</dt><dd>{v(cp.succeeded_total)} / {v(cp.failed_or_cancelled_total)}</dd>
                <dt>上次成功 ID</dt><dd className="mono">{v(cp.last_success_id)}</dd>
                <dt>距上次成功</dt><dd>{approx(cp.last_success_age_ms) !== null ? `${Math.round(approx(cp.last_success_age_ms)! / 1000)} 秒` : "—"}</dd>
                <dt>从 checkpoint 恢复</dt><dd className="mono">{v(cp.restored_from_checkpoint)}</dd>
                <dt>最近错误码</dt><dd className="mono">{v(cp.last_error_code)}</dd>
              </dl>
            )}
          </Card>
          <Card title="存储代际" sub="列出的代际不等于可恢复授权；兼容性在恢复时检查" pad={false}>
            {(() => {
              const gens = obj(cp?.storage)?.generations;
              if (!Array.isArray(gens) || gens.length === 0) return <StateView icon="layers" title="无存储代际" message={cp?.available === true ? "尚未写出 checkpoint。" : "当前不适用。"} />;
              return (
                <table className="table"><thead><tr><th>ID</th><th>版本</th><th className="num">字节</th><th>当前</th></tr></thead>
                  <tbody>{gens.slice(0, 20).map((g, i) => { const o = obj(g) ?? {}; return (
                    <tr key={i}><td className="mono">{v(o.id)}</td><td className="mono">r{v(o.revision)}</td><td className="num">{fmtBytes(o.bytes)}</td><td>{o.current === true ? <Pill tone="ok">CURRENT</Pill> : ""}</td></tr>
                  ); })}</tbody></table>
              );
            })()}
          </Card>
        </div>
      )}

      {tab === "diagnostics" && (
        <Card title="脱敏诊断包" sub="服务端 allowlist 生成，≤128 KiB；不含原始配置、SQL、目标地址、秘密或自由文本错误">
          <div className="row">
            <button className="btn primary" onClick={download} disabled={dl.busy}><Icon name="download" size={16} />{dl.busy ? "生成中…" : "下载诊断包"}</button>
            {dl.msg && <Pill tone={dl.tone}>{dl.msg}</Pill>}
          </div>
          <p className="hint" style={{ marginTop: 12 }}>包含：构建信息、版本、实际状态、观测、队列、checkpoint、最多 32 条与该流水线相关的审计摘要。审计摘要只取最近 64 条全局记录后筛选，为空不代表没有历史。</p>
        </Card>
      )}
    </>
  );
}
