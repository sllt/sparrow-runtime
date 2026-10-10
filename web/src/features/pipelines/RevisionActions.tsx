import { useMemo, useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError } from "../../api/useLoad";
import { enc } from "../../api/client";
import { numText, obj, parseJson, str, type Json } from "../../api/json";
import { Modal } from "../../components/Modal";
import { Pill } from "../../components/ui";
import { Icon } from "../../components/Icon";

export interface RevState {
  latest: string | null;
  latestEtag: string | null;
  desired: string | null;
  desiredStatus: string | null;
  actual: string | null;
}

const opId = () => `ui-${(crypto.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`).replace(/[^A-Za-z0-9_-]/g, "").slice(0, 40)}`;

/** Outcome of a mutation whose network leg failed: never assume either way. */
export type Outcome = { kind: "ok"; text: string } | { kind: "conflict"; text: string } | { kind: "unknown"; text: string } | { kind: "error"; text: string };
export function outcomeOf(e: unknown): Outcome {
  const a = asApiError(e);
  if (a.kind === "conflict") return { kind: "conflict", text: `${a.message}。状态已变化：请关闭并重新查看后再确认。` };
  if (a.kind === "offline" || a.kind === "unavailable" || a.kind === "aborted" || a.kind === "bad_response") return { kind: "unknown", text: "结果待确认：请求未得到明确响应，后端可能已经提交。请刷新查看实际状态；不会自动重试。" };
  return { kind: "error", text: a.message };
}
export function OutcomeBanner({ o }: { o: Outcome | null }) {
  if (!o) return null;
  const tone = o.kind === "ok" ? "ok" : o.kind === "error" ? "bad" : "warn";
  return <div className={`banner tone-${tone}`} role={o.kind === "ok" ? "status" : "alert"}><Icon name={o.kind === "ok" ? "check" : "alert"} />{o.text}</div>;
}

function Facts({ s, target }: { s: RevState; target: string }) {
  return (
    <dl className="kv">
      <dt>目标版本</dt><dd className="mono">rev {target}</dd>
      <dt>最新 / ETag</dt><dd className="mono">rev {s.latest ?? "—"} · {s.latestEtag ?? "—"}</dd>
      <dt>期望状态</dt><dd className="mono">{s.desiredStatus ?? "—"}{s.desired ? ` @ rev ${s.desired}` : ""}</dd>
      <dt>实际运行</dt><dd className="mono">{s.actual ? `rev ${s.actual}` : "未运行"}</dd>
    </dl>
  );
}

export function StartDialog({ name, target, s, onClose, onDone }: { name: string; target: string; s: RevState; onClose: () => void; onDone: () => void }) {
  const { client } = useAuth();
  const [busy, setBusy] = useState(false);
  const [out, setOut] = useState<Outcome | null>(null);
  const replacing = s.actual !== null && s.actual !== target;
  async function go() {
    if (!client) return;
    setBusy(true); setOut(null);
    try {
      // Bound to what the user reviewed; the server checks it atomically.
      const body = { revision: parseJson(target), expected_etag: s.latestEtag, expected_desired: { status: s.desiredStatus ?? "stopped", revision: s.desired === null ? null : parseJson(s.desired) } };
      await client.request("POST", `/v1/pipelines/${enc(name)}/start`, { body });
      setOut({ kind: "ok", text: `已提交：期望状态为 running @ rev ${target}。Supervisor 异步收敛，请在概览中查看实际状态。` });
      onDone();
    } catch (e) { setOut(outcomeOf(e)); onDone(); } finally { setBusy(false); }
  }
  const done = out?.kind === "ok" || out?.kind === "unknown";
  return (
    <Modal title={`启动 rev ${target}`} sub={name} onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>{done ? "关闭" : "取消"}</button>{!done && <button className="btn primary" onClick={() => void go()} disabled={busy || out?.kind === "conflict"}><Icon name="play" size={14} />{busy ? "提交中…" : "确认启动"}</button>}</>}>
      <Facts s={s} target={target} />
      <ul className="risk-list">
        {replacing && <li><Pill tone="warn">替换</Pill> 当前运行的 rev {s.actual} 会被停止，再以 rev {target} 启动。</li>}
        {target !== s.latest && <li><Pill tone="info">非最新</Pill> rev {target} 不是最新版本 rev {s.latest}。</li>}
        <li><Pill tone="idle">outbox</Pill> 停止或替换作业<strong>不会暂停</strong>已接受的 outbox 投递；需要时请在“运维”中显式暂停。</li>
        <li><Pill tone="idle">HTTP</Pill> 回退或重启不会撤销已发送的 HTTP 请求；远端是否幂等由业务保证。</li>
      </ul>
      <div className="hint">确认会绑定上面的 ETag 和期望状态；若此后有人发布或启动了其他版本，服务端会拒绝，不会先停止旧作业再发现冲突。</div>
      <OutcomeBanner o={out} />
    </Modal>
  );
}

export function RollbackDialog({ name, target, s, onClose, onDone }: { name: string; target: string; s: RevState; onClose: () => void; onDone: (rev: string | null) => void }) {
  const { client } = useAuth();
  const op = useMemo(opId, []);
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [out, setOut] = useState<Outcome | null>(null);
  const [created, setCreated] = useState<string | null>(null);
  async function go() {
    if (!client) return;
    setBusy(true); setOut(null);
    try {
      // Same operation_id on every retry: a repeated click returns the original receipt.
      const r = await client.request("POST", `/v1/pipelines/${enc(name)}/rollback`, { body: { operation_id: op, from_revision: parseJson(target), expected_etag: s.latestEtag, reason: reason.trim() } });
      const rec = obj(obj(r.data)?.receipt as Json);
      const rev = numText(rec?.revision);
      setCreated(rev);
      setOut({ kind: "ok", text: `已发布 rev ${rev}（内容来自 rev ${target}，来源记录为 ${str(rec?.draft_id)}）。未启动任何作业。` });
      onDone(rev);
    } catch (e) { setOut(outcomeOf(e)); onDone(null); } finally { setBusy(false); }
  }
  return (
    <Modal title={`回退到 rev ${target} 的配置`} sub={name} onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>{created ? "关闭" : "取消"}</button>{!created && <button className="btn primary" onClick={() => void go()} disabled={busy || !reason.trim() || out?.kind === "conflict"}><Icon name="history" size={14} />{busy ? "发布中…" : out?.kind === "unknown" ? "用同一操作 ID 查询/重试" : "发布为新版本"}</button>}</>}>
      <Facts s={s} target={target} />
      <p className="small">会以 rev {target} 的完整配置发布一个<strong>新的</strong> revision（rev {s.latest ? String(BigInt(s.latest) + 1n) : "?"}），旧历史全部保留。发布前会按当前目录和策略重新校验。发布后需要再显式启动。</p>
      <label><span className="field-label">审计原因（必填）</span>
        <input className="input" value={reason} onChange={(e) => setReason(e.target.value)} maxLength={256} placeholder="例如：rev 5 阈值导致误报，回退" aria-label="回退原因" /></label>
      <div className="hint">操作 ID <code>{op}</code>：重复提交返回同一回执，不会重复发布。</div>
      <OutcomeBanner o={out} />
    </Modal>
  );
}
