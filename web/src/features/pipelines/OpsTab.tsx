import { useMemo, useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { numText, obj, parseJson, str, stringifyJson, type Json } from "../../api/json";
import { Card, Pill, Skeleton, StateView } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { Modal } from "../../components/Modal";
import { fmtBytes, fmtTime } from "../../lib/format";
import { OutcomeBanner, outcomeOf, type Outcome } from "./RevisionActions";
import { pretty } from "../../lib/specText";

const NO_OUTBOX = /no durable outbox|not been initialized/;
const NO_DLQ = /no input DLQ|input_dlq|not configured|has no/i;
const decode = (b64: string | null) => { if (!b64) return ""; try { const bin = atob(b64); const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0)); return new TextDecoder("utf-8", { fatal: false }).decode(bytes); } catch { return "(无法解码)"; } };
const reasonOk = (r: string) => r.trim().length > 0 && r.length <= 256;

/** Safe-mode / restart-blocked: explain, list what the backend allows, no hidden "unblock". */
export function SafetyCard({ safeMode, restartBlocked, failures, lastError }: { safeMode: boolean; restartBlocked: boolean; failures: string | null; lastError: string | null }) {
  if (!safeMode && !restartBlocked) return null;
  return (
    <Card title="安全状态" sub="由服务端决定；页面不提供绕过或静默解除">
      {safeMode && <div className="banner tone-warn"><Icon name="shield" /><div><strong>服务器处于安全模式。</strong> 不会自动启动作业，也拒绝启用插件代码。解除需要运维人员在不带 <code>--safe-mode</code> 的情况下重启服务器；本页面没有对应操作。</div></div>}
      {restartBlocked && <div className="banner tone-bad"><Icon name="alert" /><div><strong>重启受阻：</strong>连续失败 {failures ?? "?"} 次，自动重启已停止{lastError ? <>（最近错误码 <code>{lastError}</code>）</> : null}。允许的操作：先定位并修复原因，然后在“版本历史”中显式启动某个版本（会清零失败计数）；或停止作业。</div></div>}
    </Card>
  );
}

function OutboxCard({ name, admin }: { name: string; admin: boolean }) {
  const { client } = useAuth();
  const [st, reload] = useLoad(async (c, s) => obj(await c.get(`/v1/pipelines/${enc(name)}/outbox`, s)) ?? {}, `outbox:${name}`);
  const [state, setState] = useState("pending");
  const [list, setList] = useState<Record<string, Json>[] | null>(null);
  const [cmd, setCmd] = useState<{ action: string; id?: string; gen?: string } | null>(null);
  const [raw, setRaw] = useState<{ id: string; text: string } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  if (st.loading && !st.data) return <Card title="输出 outbox"><Skeleton rows={3} /></Card>;
  if (st.error) {
    const none = NO_OUTBOX.test(st.error.message);
    return <Card title="输出 outbox">{none ? <StateView icon="layers" title="未配置 durable outbox" message="这条流水线的 Sink 没有启用 durable_outbox；HTTP 输出按 Sink 自身语义交付。" /> : <div className="err-box small">{st.error.message}</div>}</Card>;
  }
  const o = obj(st.data!.outbox) ?? {};
  const uuid = str(o.uuid);
  async function load(s: string) {
    if (!client) return;
    setState(s); setErr(null);
    try { const v = obj(await client.get(`/v1/pipelines/${enc(name)}/outbox/entries?state=${s}&limit=50`)); setList(((v?.entries as Json[]) ?? []).map((e) => obj(e) ?? {})); }
    catch (e) { setErr(asApiError(e).message); }
  }
  async function view(id: string) {
    if (!client) return;
    try { const v = obj(await client.get(`/v1/pipelines/${enc(name)}/outbox/entries/${enc(id)}`)); setRaw({ id, text: decode(str(v?.body_base64)) }); }
    catch (e) { setErr(asApiError(e).message); }
  }
  return (
    <Card title="输出 outbox" sub="本地 SQLite 提交后异步投递；远端为至少一次" actions={<Pill tone={o.paused === true ? "warn" : "ok"}>{o.paused === true ? "已暂停投递" : "投递中"}</Pill>}>
      <dl className="kv">
        <dt>待投递</dt><dd>{numText(o.pending_entries)} 条 · {fmtBytes(o.pending_bytes)}</dd>
        <dt>DLQ</dt><dd>{numText(o.dlq_entries) ?? "—"} 条</dd>
        <dt>阻塞</dt><dd>{numText(o.blocked_entries) ?? "—"}</dd>
        <dt>已接受 / 已送达</dt><dd>{numText(o.accepted_total) ?? numText(o.accepted)} / {numText(o.delivered_total) ?? numText(o.delivered)}</dd>
        <dt>UUID</dt><dd className="mono small">{uuid}</dd>
      </dl>
      <div className="banner tone-info"><Icon name="info" />停止流水线<strong>不会暂停</strong>已接受的 outbox 投递；如需停止对外发送，请显式暂停。</div>
      {admin ? (
        <div className="stack" style={{ marginTop: 12 }}>
          <div className="row" style={{ gap: 6, flexWrap: "wrap" }}>
            {o.paused === true ? <button className="btn sm" onClick={() => setCmd({ action: "resume" })}><Icon name="play" size={13} />恢复投递</button>
              : <button className="btn sm" onClick={() => setCmd({ action: "pause" })}><Icon name="stop" size={13} />暂停投递</button>}
            <span className="spacer" />
            {["pending", "dlq", "blocked"].map((s) => <button key={s} className={`btn ghost sm${list && state === s ? " on" : ""}`} onClick={() => void load(s)}>{s === "pending" ? "待投递" : s === "dlq" ? "DLQ" : "阻塞"}列表</button>)}
          </div>
          {err && <div className="err-box small" role="alert">{err}</div>}
          {list && (list.length === 0 ? <div className="hint">此状态下没有条目。</div> : (
            <div className="table-wrap"><table className="table compact">
              <thead><tr><th>序号</th><th>大小</th><th>尝试</th><th>原因</th><th>代次</th><th /></tr></thead>
              <tbody>{list.map((e) => { const id = str(e.id)!; return (
                <tr key={id}><td className="mono">{numText(e.sequence)}</td><td>{fmtBytes(e.body_bytes)}</td><td>{numText(e.attempts)}</td><td className="small">{str(e.reason) || "—"}</td><td className="mono">{numText(e.replay_generation)}</td>
                  <td className="row" style={{ gap: 4, justifyContent: "flex-end" }}>
                    <button className="btn ghost sm" onClick={() => void view(id)}>查看原文</button>
                    {state === "dlq" && <><button className="btn ghost sm" onClick={() => setCmd({ action: "replay", id, gen: numText(e.replay_generation) ?? "0" })}>重送</button><button className="btn ghost sm" onClick={() => setCmd({ action: "purge", id, gen: numText(e.replay_generation) ?? "0" })}>删除</button></>}
                  </td></tr>
              ); })}</tbody></table></div>
          ))}
        </div>
      ) : <div className="hint">条目、原文和投递命令仅管理员可用。</div>}
      {raw && <Modal title={`outbox 原文 ${raw.id}`} onClose={() => setRaw(null)} wide footer={<button className="btn" onClick={() => setRaw(null)}>关闭</button>}><div className="banner tone-warn"><Icon name="alert" />原文可能含业务数据；只在此对话框内显示，不会缓存。</div><pre className="raw-view mono">{raw.text}</pre></Modal>}
      {cmd && uuid && <OutboxCommand name={name} uuid={uuid} cmd={cmd} onClose={() => setCmd(null)} onDone={() => { reload(); if (list) void load(state); }} />}
    </Card>
  );
}

function OutboxCommand({ name, uuid, cmd, onClose, onDone }: { name: string; uuid: string; cmd: { action: string; id?: string; gen?: string }; onClose: () => void; onDone: () => void }) {
  const { client } = useAuth();
  const [reason, setReason] = useState("");
  const [out, setOut] = useState<Outcome | null>(null);
  const [busy, setBusy] = useState(false);
  const label = { pause: "暂停投递", resume: "恢复投递", replay: "重送 DLQ 条目", purge: "永久删除 DLQ 条目" }[cmd.action] ?? cmd.action;
  async function go() {
    if (!client) return;
    setBusy(true);
    try {
      const body: Record<string, Json> = { action: cmd.action, approve_uuid: uuid, reason: reason.trim() };
      if (cmd.id) { body.id = cmd.id; body.replay_generation = parseJson(cmd.gen ?? "0"); }
      await client.send("POST", `/v1/pipelines/${enc(name)}/outbox/command`, body);
      setOut({ kind: "ok", text: "已执行，命令与审计已原子提交。" }); onDone();
    } catch (e) { setOut(outcomeOf(e)); onDone(); } finally { setBusy(false); }
  }
  return (
    <Modal title={label} sub={cmd.id ? `条目 ${cmd.id} · 代次 ${cmd.gen}` : name} onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>{out ? "关闭" : "取消"}</button>{!out && <button className={`btn ${cmd.action === "purge" ? "danger" : "primary"}`} disabled={busy || !reasonOk(reason)} onClick={() => void go()}>确认</button>}</>}>
      {cmd.action === "purge" && <p className="small"><strong>删除是显式永久丢弃，不是投递成功。</strong></p>}
      {cmd.action === "replay" && <p className="small">条目回到待投递；若当前已暂停，不会自动恢复投递。</p>}
      <p className="small muted">绑定 outbox UUID <code>{uuid}</code>{cmd.gen !== undefined && <> 和代次 {cmd.gen}</>}；状态已变化时服务端会拒绝。</p>
      <label><span className="field-label">审计原因（必填）</span><input className="input" value={reason} maxLength={256} onChange={(e) => setReason(e.target.value)} aria-label="审计原因" /></label>
      <OutcomeBanner o={out} />
    </Modal>
  );
}

function InputDlqCard({ name, admin, stopped }: { name: string; admin: boolean; stopped: boolean }) {
  const { client } = useAuth();
  const [st, reload] = useLoad(async (c, s) => obj(await c.get(`/v1/pipelines/${enc(name)}/input-dlq`, s)) ?? {}, `idlq:${name}`);
  const [list, setList] = useState<Record<string, Json>[] | null>(null);
  const [raw, setRaw] = useState<{ pos: string; text: string } | null>(null);
  const [purge, setPurge] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  if (st.loading && !st.data) return <Card title="输入 DLQ"><Skeleton rows={3} /></Card>;
  if (st.error) return <Card title="输入 DLQ">{NO_DLQ.test(st.error.message) ? <StateView icon="layers" title="未启用输入隔离" message="source 未配置 input_dlq；解析错误按流水线的错误策略处理。" /> : <div className="err-box small">{st.error.message}</div>}</Card>;
  const d = obj(st.data!.input_dlq) ?? {};
  async function load() {
    if (!client) return;
    try { const v = obj(await client.get(`/v1/pipelines/${enc(name)}/input-dlq/entries?after=0&limit=50`)); setList(((v?.entries as Json[]) ?? []).map((e) => obj(e) ?? {})); }
    catch (e) { setErr(asApiError(e).message); }
  }
  async function view(pos: string) {
    if (!client) return;
    try { const v = obj(await client.get(`/v1/pipelines/${enc(name)}/input-dlq/entries/${pos}`)); setRaw({ pos, text: decode(str(v?.body_base64)) }); }
    catch (e) { setErr(asApiError(e).message); }
  }
  return (
    <Card title="输入 DLQ" sub="完整原始记录的隔离区；隔离不是 ACK">
      <dl className="kv">
        <dt>条目</dt><dd>{numText(d.entries)} · {fmtBytes(d.payload_bytes)}</dd>
        <dt>累计隔离 / 已删除</dt><dd>{numText(d.captured_total)} / {numText(d.purged_total)}</dd>
        <dt>重放下限</dt><dd className="mono">{numText(d.replay_floor)}</dd>
      </dl>
      {admin ? (
        <div className="stack" style={{ marginTop: 12 }}>
          <div className="row"><button className="btn ghost sm" onClick={() => void load()}>条目列表</button></div>
          {err && <div className="err-box small" role="alert">{err}</div>}
          {list && (list.length === 0 ? <div className="hint">没有隔离条目。</div> : (
            <div className="table-wrap"><table className="table compact">
              <thead><tr><th>位置</th><th>记录</th><th>错误码</th><th>大小</th><th>时间</th><th /></tr></thead>
              <tbody>{list.map((e) => { const pos = numText(e.position)!; return (
                <tr key={pos}><td className="mono">{pos}</td><td className="mono">{numText(e.record_index)}</td><td className="mono">{str(e.code)}</td><td>{fmtBytes(e.body_bytes)}</td><td>{fmtTime(Number(numText(e.at_ms)))}</td>
                  <td className="row" style={{ gap: 4, justifyContent: "flex-end" }}><button className="btn ghost sm" onClick={() => void view(pos)}>查看原文</button><button className="btn ghost sm" onClick={() => setPurge(pos)}>删除至此</button></td></tr>
              ); })}</tbody></table></div>
          ))}
        </div>
      ) : <div className="hint">条目与原文仅管理员可见；原文绝不进入诊断包或普通列表。</div>}
      {raw && <Modal title={`输入原文 @ ${raw.pos}`} onClose={() => setRaw(null)} wide footer={<button className="btn" onClick={() => setRaw(null)}>关闭</button>}><div className="banner tone-warn"><Icon name="alert" />原文可能含业务数据；只在此对话框内显示。</div><pre className="raw-view mono">{raw.text}</pre></Modal>}
      {purge && <DlqPurge name={name} uuid={str(d.uuid) ?? ""} position={purge} stopped={stopped} onClose={() => setPurge(null)} onDone={() => { reload(); void load(); }} />}
    </Card>
  );
}

function DlqPurge({ name, uuid, position, stopped, onClose, onDone }: { name: string; uuid: string; position: string; stopped: boolean; onClose: () => void; onDone: () => void }) {
  const { client } = useAuth();
  const [floor, setFloor] = useState(position);
  const [cp, setCp] = useState("");
  const [reason, setReason] = useState("");
  const [out, setOut] = useState<Outcome | null>(null);
  const ok = /^\d+$/.test(floor) && /^\d+$/.test(cp) && reasonOk(reason);
  async function go() {
    if (!client) return;
    try {
      await client.send("POST", `/v1/pipelines/${enc(name)}/input-dlq/purge`, { approve_uuid: uuid, position: parseJson(position), approve_replay_floor: parseJson(floor), approve_checkpoint: parseJson(cp), reason: reason.trim() });
      setOut({ kind: "ok", text: "已删除，replay_floor 已推进并写入审计。" }); onDone();
    } catch (e) { setOut(outcomeOf(e)); }
  }
  return (
    <Modal title="删除输入 DLQ 原文" sub={`位置 ≤ ${position}`} onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>{out ? "关闭" : "取消"}</button>{!out && <button className="btn danger" disabled={!ok} onClick={() => void go()}>确认删除</button>}</>}>
      {!stopped && <div className="banner tone-warn"><Icon name="alert" />服务端要求期望与实际状态均为 stopped，当前不满足时会拒绝。</div>}
      <p className="small">删除与 replay_floor 推进在同一事务中完成：<strong>此后拒绝从该下限之前恢复或历史重放原 lineage。</strong>这是明确的数据删除，不是业务成功。</p>
      <div className="grid cols-2">
        <label><span className="field-label">批准的 replay_floor</span><input className="input mono" value={floor} onChange={(e) => setFloor(e.target.value)} aria-label="replay floor" /></label>
        <label><span className="field-label">批准的 CURRENT checkpoint</span><input className="input mono" value={cp} onChange={(e) => setCp(e.target.value)} aria-label="checkpoint" /></label>
      </div>
      <label><span className="field-label">审计原因（必填）</span><input className="input" value={reason} maxLength={256} onChange={(e) => setReason(e.target.value)} aria-label="删除原因" /></label>
      <OutcomeBanner o={out} />
    </Modal>
  );
}

const MODES: { key: string; label: string; help: string }[] = [
  { key: "resume", label: "resume 兼容续跑", help: "同名发布兼容修订，从审批的 CURRENT 继续，保留状态与身份。" },
  { key: "fork", label: "fork 新 lineage", help: "从 checkpoint 切点之后以空状态启动新流水线，继续未来输入。" },
  { key: "replay", label: "replay 范围重算", help: "新 lineage，空状态重算 (from, checkpoint]；末端停止读新输入，等待显式 finish。" },
  { key: "dlq_replay", label: "dlq_replay 修正重放", help: "选择 1～64 条已覆盖的输入 DLQ 记录，写入独立 artifact 在新流水线中处理。" },
];

function RecoveryCard({ name, revision }: { name: string; revision: string | null }) {
  const { client } = useAuth();
  const [st, reload] = useLoad(async (c, s) => obj(await c.get(`/v1/pipelines/${enc(name)}/recovery/operations`, s)) ?? {}, `recops:${name}`);
  const [wizard, setWizard] = useState(false);
  const [abort, setAbort] = useState<string | null>(null);
  const [out, setOut] = useState<Outcome | null>(null);
  const ops = ((st.data?.operations as Json[]) ?? (Array.isArray(st.data) ? (st.data as Json[]) : [])).map((o) => obj(o) ?? {});
  async function finish(id: string) {
    if (!client) return;
    try { await client.send("POST", `/v1/pipelines/${enc(name)}/recovery/operations/${enc(id)}/finish`, {}); setOut({ kind: "ok", text: `操作 ${id} 已完成。` }); }
    catch (e) { setOut(outcomeOf(e)); } finally { reload(); }
  }
  return (
    <Card title="恢复操作" sub="预览 → 审批摘要 → 执行 → 显式启动 → 完成 / 中止" actions={<button className="btn sm primary" onClick={() => setWizard(true)}><Icon name="plus" size={13} />新建</button>}>
      <OutcomeBanner o={out} />
      {st.loading && !st.data ? <Skeleton rows={2} /> : st.error ? <div className="err-box small">{st.error.message}</div> : ops.length === 0 ? <div className="hint">没有恢复操作记录。</div> : (
        <div className="table-wrap"><table className="table compact">
          <thead><tr><th>操作</th><th>模式</th><th>目标</th><th>状态</th><th /></tr></thead>
          <tbody>{ops.map((o) => { const id = str(o.operation) ?? str(o.id) ?? "?"; const status = str(o.status) ?? str(o.state) ?? "unknown"; return (
            <tr key={id}><td className="mono">{id}</td><td className="mono">{str(o.mode)}</td><td className="mono">{str(o.target)}</td><td><Pill tone={status === "finished" ? "ok" : status === "aborted" ? "idle" : "warn"}>{status}</Pill></td>
              <td className="row" style={{ gap: 4, justifyContent: "flex-end" }}>{!["finished", "aborted"].includes(status) && <><button className="btn ghost sm" onClick={() => void finish(id)}>完成</button><button className="btn ghost sm" onClick={() => setAbort(id)}>中止</button></>}</td></tr>
          ); })}</tbody></table></div>
      )}
      <div className="hint">恢复成功不代表远端业务幂等；回退/重放不会撤销已产生的 HTTP 输出。仅后端声明支持的范围可恢复，CURRENT-only 图不提供历史点选项；后端拒绝即为最终结果。</div>
      {wizard && <RecoveryWizard name={name} revision={revision} onClose={() => { setWizard(false); reload(); }} />}
      {abort && <AbortDialog name={name} id={abort} onClose={() => { setAbort(null); reload(); }} />}
    </Card>
  );
}

function AbortDialog({ name, id, onClose }: { name: string; id: string; onClose: () => void }) {
  const { client } = useAuth();
  const [reason, setReason] = useState("");
  const [out, setOut] = useState<Outcome | null>(null);
  async function go() {
    if (!client) return;
    try { await client.send("POST", `/v1/pipelines/${enc(name)}/recovery/operations/${enc(id)}/abort`, { reason: reason.trim() }); setOut({ kind: "ok", text: "已中止。" }); }
    catch (e) { setOut(outcomeOf(e)); }
  }
  return (
    <Modal title={`中止恢复操作 ${id}`} onClose={onClose} footer={<><button className="btn ghost" onClick={onClose}>{out ? "关闭" : "取消"}</button>{!out && <button className="btn danger" disabled={!reasonOk(reason)} onClick={() => void go()}>确认中止</button>}</>}>
      <label><span className="field-label">原因（必填）</span><input className="input" value={reason} maxLength={256} onChange={(e) => setReason(e.target.value)} aria-label="中止原因" /></label>
      <OutcomeBanner o={out} />
    </Modal>
  );
}

function RecoveryWizard({ name, revision, onClose }: { name: string; revision: string | null; onClose: () => void }) {
  const { client } = useAuth();
  const [f, setF] = useState({ operation: `rec-${Date.now().toString(36)}`, mode: "replay", target: `${name}-replay`, parent: revision ?? "", checkpoint: "", from: "", reason: "", reset: false, dup: false, positions: "", spec: "" });
  const [specSt] = useLoad(async (c, s) => {
    const v = obj(await c.get(`/v1/pipelines/${enc(name)}`, s));
    const text = pretty(v?.spec ?? {});
    setF((x) => (x.spec ? x : { ...x, spec: text }));
    return text;
  }, `recspec:${name}`);
  const [preview, setPreview] = useState<{ sent: string; digest: string; body: Record<string, Json> } | null>(null);
  const [executed, setExecuted] = useState(false);
  const [out, setOut] = useState<Outcome | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (k: keyof typeof f, v: string | boolean) => setF((x) => ({ ...x, [k]: v }));
  const built = useMemo((): { body: Record<string, Json> } | { error: string } => {
    try {
      if (!/^\d+$/.test(f.parent) || !/^\d+$/.test(f.checkpoint)) return { error: "父版本与 checkpoint 必须是整数" };
      if (f.from && !/^\d+$/.test(f.from)) return { error: "from_checkpoint 必须是整数" };
      const body: Record<string, Json> = { operation: f.operation, mode: f.mode, approve_parent_revision: parseJson(f.parent), checkpoint_id: parseJson(f.checkpoint), target: f.target, spec: parseJson(f.spec), reason: f.reason.trim(), accept_state_reset: f.reset, accept_duplicate_outputs: f.dup };
      if (f.from) body.from_checkpoint = parseJson(f.from);
      if (f.mode === "dlq_replay") body.dlq_positions = f.positions.split(/[\s,]+/).filter(Boolean).map((p) => parseJson(p));
      return { body };
    } catch (e) { return { error: `配置 JSON 无效：${(e as Error).message}` }; }
  }, [f]);
  const sent = "body" in built ? stringifyJson(built.body) : "";
  // Any edit after preview invalidates the approval: the digest binds the exact request.
  const approved = preview !== null && preview.sent === sent;
  const stage = executed ? 3 : approved ? 2 : 1;
  async function doPreview() {
    if (!client || !("body" in built)) return;
    setBusy(true); setOut(null);
    try {
      const r = obj(await client.send("POST", `/v1/pipelines/${enc(name)}/recovery/preview`, built.body)) ?? {};
      const digest = str(r.approve_digest);
      if (!digest) throw new Error("预览未返回 approve_digest");
      setPreview({ sent, digest, body: r });
    } catch (e) { setPreview(null); setOut(e instanceof Error && !(e as { kind?: string }).kind ? { kind: "error", text: e.message } : outcomeOf(e)); } finally { setBusy(false); }
  }
  async function doExecute() {
    if (!client || !preview || !approved || !("body" in built)) return;
    setBusy(true); setOut(null);
    try {
      await client.send("POST", `/v1/pipelines/${enc(name)}/recovery/execute`, { ...built.body, approve_digest: preview.digest });
      setExecuted(true);
      setOut({ kind: "ok", text: `已执行并发布 ${f.target}。下一步需要显式启动目标流水线，完成后在列表中“完成”。` });
    } catch (e) { setOut(outcomeOf(e)); } finally { setBusy(false); }
  }
  async function doStart() {
    if (!client) return;
    try { await client.send("POST", `/v1/pipelines/${enc(f.target)}/start`, {}); setOut({ kind: "ok", text: `已提交启动 ${f.target}；请在其详情页查看实际状态。` }); }
    catch (e) { setOut(outcomeOf(e)); }
  }
  const mode = MODES.find((m) => m.key === f.mode)!;
  return (
    <Modal title="恢复操作向导" sub={name} wide onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>关闭</button>
        {stage === 1 && <button className="btn primary" disabled={busy || "error" in built || !reasonOk(f.reason)} onClick={() => void doPreview()}>预览</button>}
        {stage === 2 && <button className="btn primary" disabled={busy} onClick={() => void doExecute()}>按审批摘要执行</button>}
        {stage === 3 && <button className="btn primary" onClick={() => void doStart()}><Icon name="play" size={14} />启动 {f.target}</button>}</>}>
      <div className="steps-bar">{["1 预览", "2 审批并执行", "3 显式启动", "4 完成/中止"].map((t, i) => <span key={t} className={`st${i + 1 === stage ? " on" : i + 1 < stage ? " done" : ""}`}>{t}</span>)}</div>
      <div className="grid cols-2">
        <label><span className="field-label">模式</span><select className="input" value={f.mode} onChange={(e) => set("mode", e.target.value)} disabled={stage > 1 && executed} aria-label="恢复模式">{MODES.map((m) => <option key={m.key} value={m.key}>{m.label}</option>)}</select></label>
        <label><span className="field-label">操作 ID</span><input className="input mono" value={f.operation} onChange={(e) => set("operation", e.target.value)} aria-label="操作 ID" /></label>
        <label><span className="field-label">目标流水线</span><input className="input mono" value={f.target} onChange={(e) => set("target", e.target.value)} aria-label="目标流水线" /></label>
        <label><span className="field-label">批准的父版本</span><input className="input mono" value={f.parent} onChange={(e) => set("parent", e.target.value)} aria-label="父版本" /></label>
        <label><span className="field-label">checkpoint_id</span><input className="input mono" value={f.checkpoint} onChange={(e) => set("checkpoint", e.target.value)} aria-label="checkpoint_id" /></label>
        {f.mode === "replay" && <label><span className="field-label">from_checkpoint（可选）</span><input className="input mono" value={f.from} onChange={(e) => set("from", e.target.value)} aria-label="from_checkpoint" /></label>}
        {f.mode === "dlq_replay" && <label><span className="field-label">DLQ 位置（逗号分隔）</span><input className="input mono" value={f.positions} onChange={(e) => set("positions", e.target.value)} aria-label="DLQ 位置" /></label>}
      </div>
      <div className="hint">{mode.help}</div>
      <label className="check"><input type="checkbox" checked={f.reset} onChange={(e) => set("reset", e.target.checked)} /> 我确认清空状态（fork/replay 必需）</label>
      <label className="check"><input type="checkbox" checked={f.dup} onChange={(e) => set("dup", e.target.checked)} /> 我确认可能产生重复输出</label>
      {specSt.error && <div className="err-box small">无法读取当前配置：{specSt.error.message}</div>}
      <label><span className="field-label">目标配置（PipelineSpec JSON，默认为当前最新版本）</span><textarea className="input mono raw-json" style={{ minHeight: 120 }} value={f.spec} onChange={(e) => set("spec", e.target.value)} spellCheck={false} aria-label="目标配置" /></label>
      <label><span className="field-label">审计原因（必填）</span><input className="input" value={f.reason} maxLength={256} onChange={(e) => set("reason", e.target.value)} aria-label="恢复原因" /></label>
      {"error" in built && <div className="hint tone-warn">⚠ {built.error}</div>}
      {preview && !approved && !executed && <div className="banner tone-warn"><Icon name="alert" />预览后修改了参数：审批已失效，需要重新预览。</div>}
      {preview && approved && (
        <div className="stack">
          <div className="row small"><Pill tone="ok">已预览</Pill><span className="muted">审批摘要</span><code className="mono">{preview.digest}</code></div>
          <pre className="raw-view mono">{pretty(Object.fromEntries(Object.entries(preview.body).filter(([k]) => k !== "approve_digest")))}</pre>
        </div>
      )}
      <OutcomeBanner o={out} />
    </Modal>
  );
}

export function OpsTab({ name, admin, stopped, revision }: { name: string; admin: boolean; stopped: boolean; revision: string | null }) {
  return (
    <div className="ops-grid">
      <OutboxCard name={name} admin={admin} />
      <InputDlqCard name={name} admin={admin} stopped={stopped} />
      {admin ? <RecoveryCard name={name} revision={revision} /> : <Card title="恢复操作"><div className="hint">恢复预览/执行需要管理员角色。</div></Card>}
    </div>
  );
}
