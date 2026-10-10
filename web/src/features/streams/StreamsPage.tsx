import { useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { obj, str, type Json } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton, StateView } from "../../components/ui";
import { Icon } from "../../components/Icon";

const TYPES = ["utf8", "int64", "uint64", "float64", "bool", "timestamp_micros_utc", "dynamic"];
const TYPE_LABEL: Record<string, string> = { utf8: "文本", int64: "整数", uint64: "无符号整数", float64: "浮点", bool: "布尔", timestamp_micros_utc: "时间（µs, UTC）", dynamic: "动态 JSON" };
type Field = { name: string; type: string; nullable: boolean };

export default function StreamsPage() {
  const [st, reload] = useLoad(async (c, s) => ((obj(await c.get("/v1/streams", s))?.streams as Json[]) ?? []).filter((x): x is string => typeof x === "string"), "streams");
  const [sel, setSel] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const active = creating ? "" : sel ?? st.data?.[0] ?? null;
  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Stream 与 Schema</h1>
          <div className="page-sub">字段定义决定 SQL 可用的列与类型。修改使用条件写入（If-Match），不会静默覆盖他人的改动。</div>
        </div>
        <div className="page-actions"><button className="btn primary" onClick={() => { setCreating(true); setSel(null); }}><Icon name="plus" size={16} />新建 Stream</button></div>
      </div>
      {st.loading && !st.data ? <Skeleton rows={5} /> : st.error && !st.data ? <ErrorView error={st.error} onRetry={reload} /> : (
        <div className="grid split">
          <Card pad={false} title="Streams" sub={`${st.data!.length} 个`}>
            {st.data!.length === 0 && !creating ? <StateView icon="stream" title="还没有 Stream" message="先定义输入数据的字段，再编写流水线。" /> : (
              <ul className="list nav-list">
                {st.data!.map((n) => <li key={n}><button className={`list-btn ${active === n ? "active" : ""}`} onClick={() => { setSel(n); setCreating(false); }}><Icon name="stream" size={15} />{n}</button></li>)}
                {creating && <li><button className="list-btn active"><Icon name="plus" size={15} />新 Stream</button></li>}
              </ul>
            )}
          </Card>
          {active !== null && <StreamEditor key={active || "__new"} name={active} onSaved={(n) => { setCreating(false); setSel(n); reload(); }} />}
        </div>
      )}
    </>
  );
}

function StreamEditor({ name, onSaved }: { name: string; onSaved: (n: string) => void }) {
  const { client, me } = useAuth();
  const isNew = name === "";
  const [st] = useLoad(async (c, s) => {
    if (isNew) return { etag: "absent", fields: [{ name: "id", type: "utf8", nullable: false }, { name: "ts", type: "timestamp_micros_utc", nullable: false }] as Field[] };
    const v = obj(await c.get(`/v1/streams/${enc(name)}`, s));
    const fields = ((obj(v?.schema)?.fields as Json[]) ?? []).map((f) => { const o = obj(f)!; return { name: str(o.name) ?? "", type: str(o.type) ?? "utf8", nullable: o.nullable !== false }; });
    return { etag: str(v?.etag) ?? "absent", fields };
  }, `stream:${name}`);
  const [fields, setFields] = useState<Field[] | null>(null);
  const [etag, setEtag] = useState<string | null>(null);
  const [newName, setNewName] = useState("");
  const [msg, setMsg] = useState<{ tone: string; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  if (st.loading && !st.data) return <Card><Skeleton /></Card>;
  if (st.error && !st.data) return <Card><ErrorView error={st.error} /></Card>;
  const f = fields ?? st.data!.fields;
  const tag = etag ?? st.data!.etag;
  const dirty = fields !== null;
  const dup = new Set(f.map((x) => x.name)).size !== f.length;
  const invalid = f.some((x) => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(x.name)) || dup || f.length === 0 || (isNew && !/^[A-Za-z0-9_.-]{1,64}$/.test(newName));
  const canWrite = me?.role !== "viewer";
  const update = (i: number, p: Partial<Field>) => setFields(f.map((x, j) => (j === i ? { ...x, ...p } : x)));

  async function save() {
    if (!client) return;
    setBusy(true); setMsg(null);
    const target = isNew ? newName : name;
    try {
      const v = obj(await client.request("PUT", `/v1/streams/${enc(target)}`, { ifMatch: tag, body: { fields: f } }).then((r) => r.data));
      setEtag(str(v?.etag)); setFields(null);
      setMsg({ tone: "info", text: `已保存（${str(v?.etag)}）。已发布的流水线在下次启动/重新校验时使用新 schema。` });
      if (isNew) onSaved(target);
    } catch (e) {
      const a = asApiError(e);
      setMsg({ tone: a.kind === "conflict" ? "warn" : "bad", text: a.kind === "conflict" ? (isNew ? "同名 Stream 已存在。" : "Stream 已被他人修改。你的编辑仍在表格中；请复制后刷新页面查看最新版本。") : a.message });
    } finally { setBusy(false); }
  }

  return (
    <Card title={isNew ? "新建 Stream" : name} sub={isNew ? "定义字段后保存" : <>ETag <span className="mono">{tag}</span></>}
      actions={<button className="btn primary" disabled={!canWrite || busy || invalid || (!dirty && !isNew)} onClick={save}><Icon name="save" size={15} />{busy ? "保存中…" : "保存"}</button>}>
      <div className="stack">
        {msg && <div className={`banner tone-${msg.tone}`} role="status">{msg.text}</div>}
        {isNew && <label><span className="field-label">名称</span><input className="input" value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="sensors" /></label>}
        <table className="table compact">
          <thead><tr><th>字段名</th><th>类型</th><th>可空</th><th /></tr></thead>
          <tbody>
            {f.map((x, i) => (
              <tr key={i}>
                <td><input className="input sm mono" aria-label={`字段 ${i + 1} 名称`} value={x.name} onChange={(e) => update(i, { name: e.target.value })} /></td>
                <td><select className="input sm" aria-label={`字段 ${i + 1} 类型`} value={x.type} onChange={(e) => update(i, { type: e.target.value })}>{[...new Set([...TYPES, x.type])].map((t) => <option key={t} value={t}>{TYPE_LABEL[t] ?? t} · {t}</option>)}</select></td>
                <td><input type="checkbox" aria-label={`字段 ${i + 1} 可空`} checked={x.nullable} onChange={(e) => update(i, { nullable: e.target.checked })} /></td>
                <td>
                  {x.type === "timestamp_micros_utc" && <Pill tone="info">可作事件时间</Pill>}
                  <button className="btn ghost icon" aria-label={`删除字段 ${x.name}`} onClick={() => setFields(f.filter((_, j) => j !== i))}><Icon name="trash" size={14} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="row">
          <button className="btn" onClick={() => setFields([...f, { name: `field_${f.length + 1}`, type: "utf8", nullable: true }])}><Icon name="plus" size={14} />添加字段</button>
          {dup && <span className="hint" style={{ color: "var(--bad)" }}>字段名重复</span>}
          <span className="spacer" /><span className="hint">事件时间在流水线中以 <code>timestamp_micros_utc</code> 字段声明。</span>
        </div>
      </div>
    </Card>
  );
}
