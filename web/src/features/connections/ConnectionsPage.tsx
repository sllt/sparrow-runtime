import { useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { obj, str, type Json } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton, StateView } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { fmtAgo } from "../../lib/format";
import { IoEditor } from "../drafts/IoForm";
import { connectorKinds } from "../drafts/caps";

type Conn = { name: string; role: "source" | "sink"; kind: string; version: string; etag: string; spec: Json; updatedBy: string; updatedAt: number | null };

export default function ConnectionsPage() {
  const { me } = useAuth();
  const [st, reload] = useLoad(async (c, s) => {
    const [l, caps, sec] = await Promise.all([c.get("/v1/connections", s), c.get("/v1/capabilities", s), c.get("/v1/secrets", s).catch(() => null)]);
    const conns: Conn[] = ((obj(l)?.connections as Json[]) ?? []).map((x) => { const o = obj(x)!; return { name: str(o.name)!, role: str(o.role) === "sink" ? "sink" : "source", kind: str(o.kind) ?? "", version: String(o.version ?? ""), etag: str(o.etag) ?? "", spec: o.spec ?? {}, updatedBy: str(o.updated_by) ?? "", updatedAt: Number(o.updated_at_ms) || null }; });
    const secrets = ((obj(sec)?.secrets as Json[]) ?? []).map((x) => str(obj(x)?.name)!).filter(Boolean);
    return { conns, kinds: connectorKinds(caps), secrets };
  }, "connections");
  const [sel, setSel] = useState<string | null>(null);
  if (st.loading && !st.data) return <Skeleton rows={6} />;
  if (st.error && !st.data) return <ErrorView error={st.error} onRetry={reload} />;
  const d = st.data!;
  const cur = sel === "__new" ? null : d.conns.find((c) => c.name === sel) ?? d.conns[0] ?? null;
  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">连接模板</h1>
          <div className="page-sub">模板只在编写时使用：套用到草稿时会复制参数，发布保存完整展开的 Source/Sink。修改模板<strong>不会</strong>改变已发布的流水线。</div>
        </div>
        <div className="page-actions"><button className="btn primary" onClick={() => setSel("__new")}><Icon name="plus" size={16} />新建模板</button></div>
      </div>
      <div className="grid split">
        <div className="stack">
          <Card pad={false} title="模板" sub={`${d.conns.length} / 64`}>
            {d.conns.length === 0 && sel !== "__new" ? <StateView icon="plug" title="还没有连接模板" message="把常用的 Broker/HTTP 目标保存为模板，编写草稿时一键套用。" /> : (
              <ul className="list nav-list">
                {d.conns.map((c) => <li key={c.name}><button className={`list-btn ${cur?.name === c.name && sel !== "__new" ? "active" : ""}`} onClick={() => setSel(c.name)}><Icon name="plug" size={15} />{c.name}<span className="spacer" /><span className="chip">{c.role} · {c.kind}</span></button></li>)}
                {sel === "__new" && <li><button className="list-btn active"><Icon name="plus" size={15} />新模板</button></li>}
              </ul>
            )}
          </Card>
          <SecretsCard names={d.secrets} isAdmin={me?.role === "admin"} onChanged={reload} />
        </div>
        {(sel === "__new" || cur) && <ConnEditor key={sel === "__new" ? "__new" : cur!.name + cur!.etag} conn={sel === "__new" ? null : cur} kinds={d.kinds} onSaved={(n) => { setSel(n); reload(); }} onDeleted={() => { setSel(null); reload(); }} />}
      </div>
    </>
  );
}

function ConnEditor({ conn, kinds, onSaved, onDeleted }: { conn: Conn | null; kinds: { source: string[]; sink: string[] }; onSaved: (n: string) => void; onDeleted: () => void }) {
  const { client, me } = useAuth();
  const [name, setName] = useState(conn?.name ?? "");
  const [role, setRole] = useState<"source" | "sink">(conn?.role ?? "source");
  const [spec, setSpec] = useState<Json>(conn?.spec ?? { kind: role === "source" ? "mqtt" : "http" });
  const [msg, setMsg] = useState<{ tone: string; text: string } | null>(null);
  const [probe, setProbe] = useState<Record<string, Json> | null>(null);
  const [busy, setBusy] = useState(false);
  const isAdmin = me?.role === "admin";

  async function save() {
    if (!client) return;
    setBusy(true); setMsg(null);
    try {
      await client.request("PUT", `/v1/connections/${enc(name)}`, { ifMatch: conn?.etag, body: { role, spec } });
      onSaved(name);
    } catch (e) { const a = asApiError(e); setMsg({ tone: a.kind === "conflict" ? "warn" : "bad", text: a.kind === "conflict" ? "模板已被他人修改或同名已存在，你的编辑保留在表单中；请刷新后再合并。" : a.message }); }
    finally { setBusy(false); }
  }
  async function del() {
    if (!client || !conn || !confirm(`删除模板 “${conn.name}”？已发布流水线不受影响。`)) return;
    try { await client.request("DELETE", `/v1/connections/${enc(conn.name)}`, { ifMatch: conn.etag }); onDeleted(); }
    catch (e) { setMsg({ tone: "bad", text: asApiError(e).message }); }
  }
  async function test(stage: "config" | "handshake") {
    if (!client || !conn) return;
    setProbe(null);
    try { setProbe(obj(await client.send("POST", `/v1/connections/${enc(conn.name)}/test`, { stage }))); }
    catch (e) { setProbe({ stage, ok: false, error: { message: asApiError(e).message } }); }
  }
  return (
    <Card title={conn ? conn.name : "新建连接模板"} sub={conn ? <>v{conn.version} · {conn.updatedBy} · {fmtAgo(conn.updatedAt, Date.now())}</> : "名称、角色与参数"}
      actions={<>
        {conn && <button className="btn ghost icon" aria-label="删除模板" onClick={del}><Icon name="trash" size={15} /></button>}
        <button className="btn primary" disabled={busy || !/^[A-Za-z0-9_.-]{1,64}$/.test(name)} onClick={save}><Icon name="save" size={15} />保存</button>
      </>}>
      <div className="stack">
        {msg && <div className={`banner tone-${msg.tone}`} role="status">{msg.text}</div>}
        <div className="grid cols-2">
          <label><span className="field-label">名称</span><input className="input" value={name} disabled={!!conn} onChange={(e) => setName(e.target.value)} placeholder="plant-mqtt" /></label>
          <label><span className="field-label">角色</span><select className="input" value={role} disabled={!!conn} onChange={(e) => { const r = e.target.value as "source" | "sink"; setRole(r); setSpec({ kind: r === "source" ? "mqtt" : "http" }); }}><option value="source">Source（输入）</option><option value="sink">Sink（输出）</option></select></label>
        </div>
        <IoEditor role={role} value={spec} onChange={setSpec} kinds={kinds[role]} />
        <div className="hint">秘密请使用 SecretRef 引用（例如 <code>{"{\"secret\": \"name\"}"}</code> 形式，按各 Connector 文档），不要把明文写进模板。</div>
        {conn && (
          <div className="probe">
            <div className="row">
              <strong>连接探测</strong><span className="spacer" />
              <button className="btn" onClick={() => void test("config")}>1 · 配置校验</button>
              <button className="btn" disabled={!isAdmin} title={isAdmin ? "" : "网络握手探测仅管理员可用"} onClick={() => void test("handshake")}>2 · 网络握手{!isAdmin && "（仅管理员）"}</button>
            </div>
            {probe && (
              <div className="row small" style={{ marginTop: 10, flexWrap: "wrap" }}>
                <Pill tone={probe.ok === true ? "ok" : str(probe.result) === "probe_unavailable" ? "muted" : "bad"}>{str(probe.stage) === "handshake" ? "网络握手" : "配置校验"}：{probe.ok === true ? "通过" : str(probe.result) === "probe_unavailable" ? "不可探测" : "失败"}</Pill>
                <span className="muted">{str(probe.result) === "probe_unavailable" ? "该类型尚无“只握手”的安全探测，未进行任何拨号；仍可正常配置。" : str(obj(probe.error)?.message) ?? (probe.ok === true ? "仅检查模板结构；目标策略与 SecretRef 在完整校验时检查。" : "")}</span>
              </div>
            )}
            <div className="hint" style={{ marginTop: 8 }}>探测成功只代表该阶段通过，不代表输出权限、业务成功或长期健康；不会发送测试消息或创建订阅。</div>
          </div>
        )}
      </div>
    </Card>
  );
}

function SecretsCard({ names, isAdmin, onChanged }: { names: string[]; isAdmin: boolean; onChanged: () => void }) {
  const { client } = useAuth();
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  async function put() {
    if (!client) return;
    try { await client.send("PUT", `/v1/secrets/${enc(name)}`, { value }); setValue(""); setName(""); setMsg(`已写入 “${name}”（值不会回显）`); onChanged(); }
    catch (e) { setMsg(asApiError(e).message); setValue(""); }
  }
  return (
    <Card title="SecretRef" sub="只显示名称与存在性；值只写不读">
      {names.length === 0 ? <div className="hint">尚无秘密。</div> : <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>{names.map((n) => <span key={n} className="chip"><Icon name="key" size={11} /> {n}</span>)}</div>}
      {isAdmin && (
        <div className="stack" style={{ marginTop: 14, gap: 8 }}>
          <div className="grid cols-2">
            <input className="input sm" placeholder="名称" value={name} onChange={(e) => setName(e.target.value)} aria-label="秘密名称" />
            <input className="input sm" type="password" autoComplete="off" placeholder="值（不会回显）" value={value} onChange={(e) => setValue(e.target.value)} aria-label="秘密值" />
          </div>
          <button className="btn" disabled={!/^[A-Za-z0-9_.-]{1,64}$/.test(name) || !value} onClick={put}><Icon name="lock" size={14} />写入秘密</button>
          {msg && <div className="hint">{msg}</div>}
        </div>
      )}
    </Card>
  );
}
