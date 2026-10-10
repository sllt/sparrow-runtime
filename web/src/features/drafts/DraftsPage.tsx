import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { obj, str } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton, StateView } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { Modal } from "../../components/Modal";
import { fmtAgo, fmtBytes } from "../../lib/format";
import { emptyGraphSpec, emptySpec, pretty } from "../../lib/specText";
import { MODE_LABEL, parseSummary } from "./model";

const NAME_RE = /^[A-Za-z0-9_.-]{1,64}$/;

export default function DraftsPage() {
  const { client } = useAuth();
  const nav = useNavigate();
  const [st, reload] = useLoad(async (c, s) => {
    const [d, p, str_] = await Promise.all([c.get("/v1/drafts", s), c.get("/v1/pipelines", s), c.get("/v1/streams", s)]);
    const drafts = (obj(d)?.drafts as unknown[] | undefined ?? []).map((x) => parseSummary(x as never));
    const pipes = ((obj(p)?.pipelines as unknown[]) ?? []).filter((x): x is string => typeof x === "string");
    const streams = ((obj(str_)?.streams as unknown[]) ?? []).filter((x): x is string => typeof x === "string");
    return { drafts, pipes, streams };
  }, "drafts");
  const [creating, setCreating] = useState(false);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">草稿与发布</h1>
          <div className="page-sub">草稿保存在服务端，可以暂时无效；保存不会创建作业、连接或 checkpoint。发布需要重新校验，启动是另一个明确操作。</div>
        </div>
        <div className="page-actions">
          <button className="btn primary" onClick={() => setCreating(true)}><Icon name="plus" size={16} />新建草稿</button>
        </div>
      </div>
      <Card pad={false}>
        {st.loading && !st.data ? <Skeleton rows={5} /> : st.error && !st.data ? <ErrorView error={st.error} onRetry={reload} /> : st.data && st.data.drafts.length === 0 ? (
          <StateView icon="file" title="还没有草稿" message="从空白模板或现有流水线创建草稿，编辑 SQL 与 Source/Sink 后检查并发布。" action={<button className="btn primary" onClick={() => setCreating(true)}><Icon name="plus" size={16} />新建草稿</button>} />
        ) : (
          <table className="table">
            <thead><tr><th>草稿</th><th>目标流水线</th><th>模式</th><th>基线版本</th><th>最后编辑</th><th className="num">大小</th></tr></thead>
            <tbody>
              {st.data!.drafts.map((d) => (
                <tr key={d.id} className="clickable" tabIndex={0} onClick={() => nav(`/drafts/${enc(d.id)}`)} onKeyDown={(e) => e.key === "Enter" && nav(`/drafts/${enc(d.id)}`)}>
                  <td><div className="row-name">{d.id}</div><div className="row-meta mono">{d.etag}</div></td>
                  <td>{d.pipeline} {st.data!.pipes.includes(d.pipeline) ? <Pill tone="info">更新现有</Pill> : <Pill tone="idle">新建</Pill>}</td>
                  <td><span className="chip">{MODE_LABEL[d.mode] ?? d.mode}</span></td>
                  <td className="mono">{d.baseEtag ?? <span className="muted">—（新流水线）</span>}</td>
                  <td>{d.updatedBy} <span className="muted">· {fmtAgo(d.updatedAt, Date.now())}</span></td>
                  <td className="num">{fmtBytes(d.textBytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
      {creating && st.data && client && (
        <NewDraft pipes={st.data.pipes} streams={st.data.streams} onClose={() => setCreating(false)} onCreated={(id) => nav(`/drafts/${enc(id)}`)} />
      )}
    </>
  );
}

function NewDraft({ pipes, streams, onClose, onCreated }: { pipes: string[]; streams: string[]; onClose: () => void; onCreated: (id: string) => void }) {
  const { client } = useAuth();
  const [from, setFrom] = useState<string>("");
  const [pipeline, setPipeline] = useState("");
  const [id, setId] = useState("");
  const [mode, setMode] = useState<"sql" | "graph">("sql");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const target = from || pipeline;
  const valid = NAME_RE.test(id) && NAME_RE.test(target);

  async function create() {
    if (!client || !valid) return;
    setBusy(true); setErr(null);
    try {
      const stream = streams[0] ?? "events";
      let text = mode === "graph" ? emptyGraphSpec(stream) : emptySpec(stream);
      let base: string | null = null;
      let m: string = mode;
      if (from) {
        const cur = obj(await client.get(`/v1/pipelines/${enc(from)}`));
        text = pretty(cur?.spec ?? {});
        base = str(cur?.etag);
        m = obj(obj(cur?.spec)?.graph) ? "graph" : "sql";
      }
      await client.request("PUT", `/v1/drafts/${enc(id)}`, { body: { pipeline: target, mode: m, text, metadata: {}, base_etag: base } });
      onCreated(id);
    } catch (e) {
      const a = asApiError(e);
      setErr(a.kind === "conflict" ? `草稿 “${id}” 已存在，请换一个名称。` : a.message);
    } finally { setBusy(false); }
  }

  return (
    <Modal title="新建草稿" sub="名称只允许字母、数字、_ . -（1–64 个字符）" onClose={onClose}
      footer={<><button className="btn ghost" onClick={onClose}>取消</button><button className="btn primary" disabled={!valid || busy} onClick={create}>{busy ? "创建中…" : "创建并编辑"}</button></>}>
      <div className="stack">
        <label><span className="field-label">起点</span>
          <select className="input" value={from} onChange={(e) => { setFrom(e.target.value); if (e.target.value && !id) setId(`${e.target.value}-edit`); }}>
            <option value="">空白模板（新流水线）</option>
            {pipes.map((p) => <option key={p} value={p}>基于现有流水线：{p}</option>)}
          </select>
        </label>
        {!from && (
          <div role="radiogroup" aria-label="编辑方式" className="seg">
            {(["sql", "graph"] as const).map((k) => (
              <button key={k} role="radio" aria-checked={mode === k} className={`seg-btn${mode === k ? " on" : ""}`} onClick={() => setMode(k)}>
                <Icon name={k === "sql" ? "code" : "branch"} size={15} />{k === "sql" ? "SQL" : "Graph 设计器"}
              </button>
            ))}
          </div>
        )}
        {!from && <label><span className="field-label">目标流水线名称</span><input className="input" value={pipeline} onChange={(e) => setPipeline(e.target.value)} placeholder="例如 temperature-alerts" /></label>}
        <label><span className="field-label">草稿名称</span><input className="input" value={id} onChange={(e) => setId(e.target.value)} placeholder="例如 alerts-v2" /></label>
        {from && <div className="hint">将复制该流水线当前最新版本的完整配置，并记录其 ETag 作为发布基线；期间若有人发布新版本，发布会以冲突拒绝。</div>}
        {err && <div className="banner tone-bad" role="alert"><Icon name="alert" />{err}</div>}
      </div>
    </Modal>
  );
}
