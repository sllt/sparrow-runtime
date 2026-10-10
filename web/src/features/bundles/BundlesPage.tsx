import { useState } from "react";
import { Link } from "react-router-dom";
import { useClient } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { obj, parseJson, str, stringifyJson, type Json } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton } from "../../components/ui";
import { Icon } from "../../components/Icon";

const arr = (v: Json | undefined): Json[] => (Array.isArray(v) ? v : []);
const ACTION: Record<string, [string, "ok" | "info" | "bad"]> = {
  create: ["将创建", "info"], exists_identical: ["已存在且一致", "ok"], conflict: ["Schema 冲突", "bad"],
};

/** K5.6 author-side config bundles: allowlist export, digest-bound import into drafts. */
export default function BundlesPage() {
  const client = useClient();
  const [st, reload] = useLoad(async (c, s) => ((obj(await c.get("/v1/pipelines", s))?.pipelines as Json[]) ?? []).filter((x): x is string => typeof x === "string"), "bundle-pipes");
  const [pick, setPick] = useState<Set<string>>(new Set());
  const [logic, setLogic] = useState(false);
  const [expMsg, setExpMsg] = useState<{ tone: string; text: string } | null>(null);
  const [bundle, setBundle] = useState<Json | null>(null);
  const [fileName, setFileName] = useState("");
  const [prefix, setPrefix] = useState("import-");
  const [preview, setPreview] = useState<Record<string, Json> | null>(null);
  const [result, setResult] = useState<Record<string, Json> | null>(null);
  const [impMsg, setImpMsg] = useState<{ tone: string; text: string } | null>(null);
  const [busy, setBusy] = useState(false);

  if (st.loading && !st.data) return <Skeleton rows={5} />;
  if (st.error && !st.data) return <ErrorView error={st.error} onRetry={reload} />;
  const pipes = st.data!;

  const doExport = async () => {
    setBusy(true); setExpMsg(null);
    try {
      const q = `/v1/bundles/export?pipelines=${[...pick].map(encodeURIComponent).join(",")}${logic ? "&include_logic=true" : ""}`;
      const b = obj(await client.get(q));
      const fill = arr(b?.pipelines).reduce<number>((n, p) => n + arr(obj(p)?.fill_in).length, 0);
      const url = URL.createObjectURL(new Blob([stringifyJson(b)], { type: "application/json" }));
      const a = document.createElement("a"); a.href = url; a.download = `sparrow-bundle-${pick.size}.json`; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 0);
      setExpMsg({ tone: "ok", text: `已导出 ${pick.size} 条流水线；${fill} 处字段被替换为待填写标记。` });
    } catch (e) { setExpMsg({ tone: "bad", text: asApiError(e).message }); }
    setBusy(false);
  };

  const onFile = async (f: File | undefined) => {
    setPreview(null); setResult(null); setImpMsg(null); setBundle(null);
    if (!f) return;
    setFileName(f.name);
    if (f.size > 1024 * 1024) { setImpMsg({ tone: "bad", text: "文件超过 1 MiB 上限。" }); return; }
    try { setBundle(parseJson(await f.text())); } catch { setImpMsg({ tone: "bad", text: "不是合法的 JSON 文件。" }); }
  };
  const doPreview = async () => {
    setBusy(true); setImpMsg(null); setResult(null);
    try { setPreview(obj(await client.send("POST", "/v1/bundles/import/preview", { bundle, draft_prefix: prefix }))); }
    catch (e) { setPreview(null); setImpMsg({ tone: "bad", text: asApiError(e).message }); }
    setBusy(false);
  };
  const doImport = async () => {
    setBusy(true); setImpMsg(null);
    try {
      setResult(obj(await client.send("POST", "/v1/bundles/import/execute", { bundle, draft_prefix: prefix, approve_digest: preview?.approve_digest })));
      setPreview(null);
    } catch (e) {
      const a = asApiError(e);
      setImpMsg({ tone: "bad", text: a.kind === "conflict" ? `${a.message}（请重新预览）` : a.message });
      setPreview(null);
    }
    setBusy(false);
  };
  const blocked = arr(preview?.blocked).map(String);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">配置导入导出</h1>
          <div className="page-sub">版本化的<strong>编写侧配置包</strong>：包含 Stream Schema 与流水线配置。不包含 Secret 值、插件二进制、DLQ/Outbox 内容和 checkpoint 状态，<strong>不是</strong>灾备备份。</div>
        </div>
      </div>
      <div className="grid split">
        <Card title="导出" sub="白名单投影：地址、Header、路径、凭据等字段替换为待填写标记">
          <div className="stack">
            {pipes.length === 0 ? <div className="muted">还没有已发布的流水线。</div> : (
              <div className="bundle-pick" role="group" aria-label="选择流水线">
                {pipes.map((p) => (
                  <label key={p} className="check"><input type="checkbox" checked={pick.has(p)} onChange={(e) => { const n = new Set(pick); if (e.target.checked) n.add(p); else n.delete(p); setPick(n); }} /> {p}</label>
                ))}
              </div>
            )}
            <label className="check"><input type="checkbox" checked={logic} onChange={(e) => setLogic(e.target.checked)} /> 包含 SQL / Graph 逻辑</label>
            {logic && <div className="banner tone-warn"><Icon name="alert" /><div>SQL 字面量和图参数可能包含敏感值（阈值、设备号、内部名称），服务端无法逐项识别。请确认接收方可以看到这些内容。</div></div>}
            {expMsg && <div className={`banner tone-${expMsg.tone}`} role="status">{expMsg.text}</div>}
            <div className="row"><button className="btn primary" disabled={busy || pick.size === 0 || pick.size > 16} onClick={doExport}><Icon name="download" size={15} />导出 {pick.size || ""} 条</button><span className="muted">最多 16 条</span></div>
          </div>
        </Card>
        <Card title="导入" sub="只创建草稿与缺失的 Stream；不发布、不启动、不覆盖">
          <div className="stack">
            <div className="grid cols-2">
              <label><span className="field-label">配置包文件</span><input className="input" type="file" accept="application/json,.json" aria-label="配置包文件" onChange={(e) => onFile(e.target.files?.[0])} /></label>
              <label><span className="field-label">草稿 ID 前缀</span><input className="input" value={prefix} onChange={(e) => { setPrefix(e.target.value); setPreview(null); }} /></label>
            </div>
            {fileName && bundle && !preview && !result && <div className="muted">{fileName} · 格式 {str(obj(bundle)?.format) ?? "未知"} · {arr(obj(bundle)?.pipelines).length} 条流水线</div>}
            {impMsg && <div className={`banner tone-${impMsg.tone}`} role="alert">{impMsg.text}</div>}
            {preview && (
              <div className="stack" aria-label="导入预览">
                <div className="bundle-table"><table className="table">
                  <thead><tr><th>流水线</th><th>草稿</th><th>目标</th><th>待填写</th></tr></thead>
                  <tbody>{arr(preview.drafts).map((d, i) => { const o = obj(d)!; const f = arr(o.fill_in).map(String); return (
                    <tr key={i}><td className="mono">{str(o.pipeline)}</td><td className="mono">{str(o.draft)}</td>
                      <td>{o.target === "existing_pipeline" ? <Pill tone="warn" title={`基线 ${str(o.base_etag)}`}>更新已有流水线</Pill> : <Pill tone="info">新流水线</Pill>}</td>
                      <td>{f.length ? <span title={f.join("\n")}><Pill tone="warn">{f.length} 项</Pill> <span className="muted mono small fill-list">{f.slice(0, 3).join(", ")}{f.length > 3 ? "…" : ""}</span></span> : <Pill tone="ok">无</Pill>}</td></tr>); })}</tbody>
                </table></div>
                {arr(preview.streams).length > 0 && <div className="row wrap">{arr(preview.streams).map((s, i) => { const o = obj(s)!; const [l, t] = ACTION[str(o.action) ?? ""] ?? [str(o.action), "info"]; return <Pill key={i} tone={t}>Stream {str(o.name)}：{l}</Pill>; })}</div>}
                {blocked.length > 0 ? <div className="banner tone-bad" role="alert"><Icon name="alert" /><div><strong>无法导入：</strong>{blocked.join("；")}</div></div>
                  : <div className="banner tone-info"><Icon name="info" /><div>含待填写标记的草稿在每个字段补齐前<strong>无法发布</strong>。审批摘要 <span className="mono small">{str(preview.approve_digest)?.slice(0, 16)}…</span>；预览后若配置包或目录发生变化，导入会被拒绝。</div></div>}
              </div>
            )}
            {result && (
              <div className="banner tone-ok" role="status"><Icon name="check" /><div>
                已创建 {arr(result.drafts).length} 个草稿{arr(result.streams_created).length ? `、${arr(result.streams_created).length} 个 Stream` : ""}；未发布，未启动。
                <div className="row wrap" style={{ marginTop: 6 }}>{arr(result.drafts).map((d, i) => <Link key={i} className="btn" to={`/drafts/${encodeURIComponent(str(obj(d)?.id) ?? "")}`}>打开 {str(obj(d)?.id)}</Link>)}</div>
              </div></div>
            )}
            <div className="row">
              <button className="btn" disabled={busy || !bundle || !/^[A-Za-z0-9_.-]{0,32}$/.test(prefix)} onClick={doPreview}><Icon name="diff" size={15} />预览</button>
              <button className="btn primary" disabled={busy || !preview || blocked.length > 0} onClick={doImport}><Icon name="upload" size={15} />确认导入为草稿</button>
            </div>
          </div>
        </Card>
      </div>
    </>
  );
}
