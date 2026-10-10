import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { numText, obj, str, type Json } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { DiffView } from "../../components/DiffView";
import { fmtTime } from "../../lib/format";
import { pretty } from "../../lib/specText";

export function RevisionsTab({ name }: { name: string }) {
  const { client } = useAuth();
  const nav = useNavigate();
  const [before, setBefore] = useState<string | null>(null);
  const [st, reload] = useLoad(async (c, s) => obj(await c.get(`/v1/pipelines/${enc(name)}/revisions?limit=20${before ? `&before=${before}` : ""}`, s)) ?? {}, `revs:${name}:${before}`);
  const [view, setView] = useState<{ rev: string; spec: Json; latest: Json } | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  if (st.loading && !st.data) return <Skeleton rows={5} />;
  if (st.error && !st.data) return <ErrorView error={st.error} onRetry={reload} />;
  const d = st.data!;
  const latest = numText(d.latest_revision), desired = numText(d.desired_revision), actual = numText(d.actual_revision);
  const revs = ((d.revisions as Json[]) ?? []).map((r) => obj(r)!);
  async function open(rev: string) {
    if (!client) return;
    const [a, b] = await Promise.all([client.get(`/v1/pipelines/${enc(name)}/revisions/${rev}`), client.get(`/v1/pipelines/${enc(name)}/revisions/${latest}`)]);
    setView({ rev, spec: obj(a)?.spec ?? null, latest: obj(b)?.spec ?? null });
  }
  async function draftFrom(rev: string) {
    if (!client || !view) return;
    const id = `${name}-from-r${rev}`.slice(0, 64);
    try {
      await client.request("PUT", `/v1/drafts/${enc(id)}`, { body: { pipeline: name, mode: "sql", text: pretty(view.spec), metadata: { source_revision: Number(rev) }, base_etag: str(d.latest_etag) } });
      nav(`/drafts/${enc(id)}`);
    } catch (e) { const a = asApiError(e); setMsg(a.kind === "conflict" ? `草稿 “${id}” 已存在。` : a.message); }
  }
  return (
    <div className="stack">
      <div className="grid cols-3">
        <div className="mini"><div className="mini-label">最新（latest）</div><div className="mini-value">rev {latest ?? "—"}</div><div className="hint">目录中最新发布的配置</div></div>
        <div className="mini"><div className="mini-label">期望（desired）</div><div className="mini-value">{desired ? `rev ${desired}` : "—"}</div><div className="hint">最近一次启动请求指定的版本</div></div>
        <div className="mini"><div className="mini-label">实际（actual）</div><div className="mini-value">{actual ? `rev ${actual}` : "—"}</div><div className="hint">运行 attempt 实际使用的版本</div></div>
      </div>
      {latest && actual && latest !== actual && <div className="banner tone-warn"><Icon name="alert" />正在运行的是 rev {actual}，不是最新的 rev {latest}。发布新版本不会自动替换运行中的作业。</div>}
      <Card pad={false} title="发布历史" sub="目录中保留的全部版本；审计日志有上限，不能当作完整发布史">
        <table className="table">
          <thead><tr><th>版本</th><th>ETag</th><th>发布时间</th><th className="num">大小</th><th>标记</th><th /></tr></thead>
          <tbody>
            {revs.map((r) => { const rev = numText(r.revision)!; return (
              <tr key={rev}>
                <td className="row-name">rev {rev}</td>
                <td className="mono">{str(r.etag)}</td>
                <td>{fmtTime(Number(numText(r.created_at)))}</td>
                <td className="num">{numText(r.spec_bytes)} B</td>
                <td className="row" style={{ gap: 6 }}>{rev === latest && <Pill tone="info">latest</Pill>}{rev === desired && <Pill tone="idle">desired</Pill>}{rev === actual && <Pill tone="ok">actual</Pill>}</td>
                <td><button className="btn ghost" onClick={() => void open(rev)}><Icon name="diff" size={14} />查看 / 对比</button></td>
              </tr>
            ); })}
          </tbody>
        </table>
        <div className="row" style={{ padding: "var(--sp-3) var(--sp-5)" }}>
          {before && <button className="btn ghost" onClick={() => setBefore(null)}>回到最新</button>}
          <span className="spacer" />
          {revs.length === 20 && <button className="btn" onClick={() => setBefore(numText(revs[revs.length - 1]!.revision))}>更早的版本</button>}
        </div>
      </Card>
      {view && (
        <Card title={`rev ${view.rev} 与最新 rev ${latest} 的差异`} actions={<><button className="btn" onClick={() => void draftFrom(view.rev)}><Icon name="file" size={14} />基于此版本新建草稿</button><button className="btn ghost icon" aria-label="关闭" onClick={() => setView(null)}><Icon name="x" size={14} /></button></>}>
          {msg && <div className="banner tone-warn">{msg}</div>}
          <DiffView before={pretty(view.spec)} after={pretty(view.latest)} beforeLabel={`rev ${view.rev}`} afterLabel={`rev ${latest}（latest）`} />
          <div className="hint" style={{ marginTop: 8 }}>基于旧版本新建草稿后，发布会生成新的 revision（保留原历史），不会覆盖旧版本。</div>
        </Card>
      )}
    </div>
  );
}
