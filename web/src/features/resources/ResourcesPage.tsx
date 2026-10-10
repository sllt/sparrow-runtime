import { useState, type ReactNode } from "react";
import { useLoad } from "../../api/useLoad";
import { useAuth } from "../../auth/AuthContext";
import { enc } from "../../api/client";
import { numText, obj, parseJson, str, stringifyJson, type Json } from "../../api/json";
import { Icon } from "../../components/Icon";
import { Modal } from "../../components/Modal";
import { pretty } from "../../lib/specText";
import { OutcomeBanner, outcomeOf, type Outcome } from "../pipelines/RevisionActions";
import { Card, ErrorView, Pill, Skeleton, StateView, Tabs } from "../../components/ui";

type Tab = "tables" | "plugins";

function cell(v: Json | undefined): string {
  if (v === undefined || v === null) return "—";
  if (typeof v === "boolean") return v ? "是" : "否";
  const n = numText(v);
  if (n !== null) return n;
  if (typeof v === "string") return v.length > 24 && /^[0-9a-f]+$/.test(v) ? `${v.slice(0, 12)}…` : v;
  const s = stringifyJson(v);
  return s.length > 60 ? `${s.slice(0, 60)}…` : s;
}

type Act =
  | { kind: "table-rollback"; name: string; latest: string }
  | { kind: "table-gc"; name: string }
  | { kind: "plugin"; action: "enable" | "disable" | "uninstall"; digest: string; label: string }
  | { kind: "install" };

function TableDetail({ name, onClose }: { name: string; onClose: () => void }) {
  const [st] = useLoad(async (c, s) => {
    const [r, d] = await Promise.all([c.get(`/v1/tables/${enc(name)}/revisions`, s), c.get(`/v1/tables/${enc(name)}/dependencies`, s)]);
    return { revs: r, deps: d };
  }, `tbl:${name}`);
  return (
    <Card title={`参考表 ${name}`} sub="依赖分为 pinned（固定 revision，发布新版本不影响）与 follow_latest（热跟随）。运行中绑定的 revision 与 latest 分开显示。"
      actions={<button className="btn ghost icon" aria-label="关闭" onClick={onClose}><Icon name="x" size={14} /></button>}>
      {st.loading && !st.data ? <Skeleton rows={3} /> : st.error ? <div className="err-box small">{st.error.message}</div> : (
        <div className="grid cols-2">
          <div><div className="field-label">依赖与绑定</div><pre className="raw-view mono">{pretty(st.data!.deps)}</pre></div>
          <div><div className="field-label">版本</div><pre className="raw-view mono">{pretty(st.data!.revs)}</pre></div>
        </div>
      )}
    </Card>
  );
}

function ActionDialog({ act, onClose, onDone }: { act: Act; onClose: () => void; onDone: () => void }) {
  const { client } = useAuth();
  const [target, setTarget] = useState("");
  const [hash, setHash] = useState("");
  const [file, setFile] = useState<{ name: string; bytes: Uint8Array; sha: string } | null>(null);
  const [out, setOut] = useState<Outcome | null>(null);
  const [busy, setBusy] = useState(false);
  let title = "", body: ReactNode = null, ok = true, danger = false;
  if (act.kind === "table-rollback") {
    title = `回退参考表 ${act.name}`; ok = /^\d+$/.test(target) && target !== act.latest;
    body = <><p className="small">以目标 revision 的内容发布一个<strong>新</strong> revision（基于当前 rev {act.latest}）。pinned 的流水线不受影响；follow_latest 的运行作业会看到新内容。</p>
      <label><span className="field-label">目标 revision</span><input className="input mono" value={target} onChange={(e) => setTarget(e.target.value)} aria-label="目标 revision" /></label></>;
  } else if (act.kind === "table-gc") {
    title = `GC 参考表 ${act.name}`; danger = true;
    body = <p className="small">只删除未被任何流水线 revision 引用、且不是 latest 的旧版本；被 pin 的版本由服务端在同一事务中保护。删除后不可恢复。</p>;
  } else if (act.kind === "plugin") {
    title = `${act.action === "enable" ? "启用" : act.action === "disable" ? "停用" : "卸载"}插件 ${act.label}`;
    danger = act.action !== "enable"; ok = act.action !== "enable" || hash.trim() === act.digest;
    body = act.action === "enable" ? <><p className="small">启用会加载受信任代码。请核对并粘贴<strong>完整</strong>的 manifest SHA-256 作为审批（安全模式下服务端会拒绝）。</p>
      <div className="mono small" style={{ wordBreak: "break-all" }}>{act.digest}</div>
      <label><span className="field-label">粘贴哈希确认</span><input className="input mono" value={hash} onChange={(e) => setHash(e.target.value)} aria-label="审批哈希" /></label></>
      : act.action === "disable" ? <p className="small">停用后引用此插件的新启动会被拒绝；运行中的作业行为以服务端为准。</p>
      : <p className="small">仍被流水线 revision 引用时服务端会拒绝卸载（引用保护）。</p>;
  } else {
    title = "安装插件包"; ok = file !== null;
    body = <><p className="small">包内容只上传给服务端校验（大小、manifest、签名、权限）；页面不会执行或解析其中的代码。安装后仍需按哈希审批启用。</p>
      <input type="file" aria-label="插件包文件" onChange={async (e) => { const f = e.target.files?.[0]; if (!f) return; const bytes = new Uint8Array(await f.arrayBuffer()); const d = await crypto.subtle.digest("SHA-256", bytes); setFile({ name: f.name, bytes, sha: Array.from(new Uint8Array(d)).map((b) => b.toString(16).padStart(2, "0")).join("") }); }} />
      {file && <dl className="kv"><dt>文件</dt><dd>{file.name} · {file.bytes.length} B</dd><dt>文件 SHA-256</dt><dd className="mono small" style={{ wordBreak: "break-all" }}>{file.sha}</dd></dl>}</>;
  }
  async function go() {
    if (!client) return;
    setBusy(true);
    try {
      let r: Json = null;
      if (act.kind === "table-rollback") r = await client.send("POST", `/v1/tables/${enc(act.name)}/rollback`, { expected_revision: parseJson(act.latest), target_revision: parseJson(target) });
      else if (act.kind === "table-gc") r = await client.send("POST", `/v1/tables/${enc(act.name)}/gc`, {});
      else if (act.kind === "plugin") r = await client.send("POST", `/v1/plugins/${enc(act.digest)}/${act.action}`, act.action === "enable" ? { approve_manifest_sha256: hash.trim() } : {});
      else if (file) r = (await client.request("POST", "/v1/plugins/install", { body: file.bytes })).data;
      setOut({ kind: "ok", text: `完成。${act.kind === "install" ? `manifest ${str(obj(r)?.manifest_sha256) ?? ""}，尚未启用。` : ""}` });
      onDone();
    } catch (e) { setOut(outcomeOf(e)); } finally { setBusy(false); }
  }
  return (
    <Modal title={title} onClose={onClose} footer={<><button className="btn ghost" onClick={onClose}>{out ? "关闭" : "取消"}</button>{!out && <button className={`btn ${danger ? "danger" : "primary"}`} disabled={busy || !ok} onClick={() => void go()}>确认</button>}</>}>
      {body}
      <OutcomeBanner o={out} />
    </Modal>
  );
}

export default function ResourcesPage() {
  const { me } = useAuth();
  const admin = me?.role === "admin";
  const [tab, setTab] = useState<Tab>("tables");
  const [detail, setDetail] = useState<string | null>(null);
  const [act, setAct] = useState<Act | null>(null);
  const [st, reload] = useLoad(async (c, s) => {
    const [t, p] = await Promise.all([c.get("/v1/tables", s), c.get("/v1/plugins", s).catch((e) => ({ __error: String(e.message ?? e) }) as Json)]);
    return { tables: ((obj(t)?.tables as Json[]) ?? []).map((x) => obj(x) ?? {}), plugins: obj(p) };
  }, "resources");
  const plugins = st.data?.plugins;
  const pkgs = Array.isArray(plugins?.packages) ? (plugins!.packages as Json[]).map((x) => obj(x) ?? {}) : [];
  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">参考表与插件</h1>
          <div className="page-sub">版本、哈希、依赖与启用状态。{admin ? "敏感操作需要明确审批，结果以服务端为准。" : "回退、GC、安装与启停仅管理员可用。"}</div>
        </div>
        {admin && tab === "plugins" && <div className="page-actions"><button className="btn primary" onClick={() => setAct({ kind: "install" })}><Icon name="upload" size={14} />安装插件包</button></div>}
      </div>
      <Tabs<Tab> value={tab} onChange={setTab} tabs={[{ key: "tables", label: "参考表", badge: st.data ? <span className="chip">{st.data.tables.length}</span> : undefined }, { key: "plugins", label: "插件", badge: st.data ? <span className="chip">{pkgs.length}</span> : undefined }]} />
      {st.loading && !st.data ? <Skeleton rows={5} /> : st.error && !st.data ? <ErrorView error={st.error} onRetry={reload} /> : tab === "tables" ? (
        <div className="stack">
          <Card pad={false}>
            {st.data!.tables.length === 0 ? <StateView icon="inbox" title="没有参考表" /> : (
              <div className="table-wrap"><table className="table">
                <thead><tr><th>名称</th><th>latest</th><th>sha256</th><th>行数</th><th /></tr></thead>
                <tbody>{st.data!.tables.map((t) => { const name = str(t.name) ?? "?"; const latest = numText(t.revision) ?? numText(t.latest_revision) ?? "?"; return (
                  <tr key={name}><td className="row-name">{name}</td><td className="mono">rev {latest}</td><td className="mono" title={str(t.sha256) ?? undefined}>{cell(t.sha256)}</td><td>{cell(t.row_count ?? t.rows_count)}</td>
                    <td className="row" style={{ gap: 4, justifyContent: "flex-end" }}>
                      <button className="btn ghost sm" onClick={() => setDetail(name)}>依赖与版本</button>
                      {admin && <><button className="btn ghost sm" onClick={() => setAct({ kind: "table-rollback", name, latest })}>回退</button><button className="btn ghost sm" onClick={() => setAct({ kind: "table-gc", name })}>GC</button></>}
                    </td></tr>
                ); })}</tbody></table></div>
            )}
          </Card>
          {detail && <TableDetail name={detail} onClose={() => setDetail(null)} />}
        </div>
      ) : (
        <div className="stack">
          {plugins?.__error ? <div className="banner tone-warn">{str(plugins.__error)}</div> : (
            <div className="row" style={{ flexWrap: "wrap", gap: 8 }}>
              {(["native_allowed", "script_allowed", "wasm_allowed", "external_allowed", "signature_required"] as const).map((k) => (
                <Pill key={k} tone={plugins?.[k] === true ? (k === "signature_required" ? "ok" : "info") : "idle"}>{k}: {plugins?.[k] === true ? "是" : "否"}</Pill>
              ))}
            </div>
          )}
          <Card pad={false}>
            {pkgs.length === 0 ? <StateView icon="puzzle" title="没有已安装的插件包" message={admin ? "使用右上角“安装插件包”上传。" : undefined} /> : (
              <div className="table-wrap"><table className="table">
                <thead><tr><th>名称</th><th>类型</th><th>manifest</th><th>签名</th><th>引用</th><th>状态</th><th /></tr></thead>
                <tbody>{pkgs.map((p) => { const m = obj(p.manifest) ?? {}; const digest = str(p.manifest_sha256) ?? ""; const label = `${str(m.name) ?? "?"}@${str(m.version) ?? "?"}`; return (
                  <tr key={digest}><td className="row-name">{label}</td><td className="mono">{str(m.kind)}</td><td className="mono" title={digest}>{cell(digest)}</td>
                    <td>{p.signature_verified === true ? <Pill tone="ok">已验证{str(p.publisher) ? ` · ${str(p.publisher)}` : ""}</Pill> : <Pill tone="idle">未签名</Pill>}</td>
                    <td>{numText(p.pins) ?? "0"}</td>
                    <td>{p.enabled === true ? <Pill tone="ok">启用</Pill> : <Pill tone="idle">停用</Pill>}</td>
                    <td className="row" style={{ gap: 4, justifyContent: "flex-end" }}>{admin && (p.enabled === true
                      ? <button className="btn ghost sm" onClick={() => setAct({ kind: "plugin", action: "disable", digest, label })}>停用</button>
                      : <><button className="btn ghost sm" onClick={() => setAct({ kind: "plugin", action: "enable", digest, label })}>启用</button><button className="btn ghost sm" onClick={() => setAct({ kind: "plugin", action: "uninstall", digest, label })}>卸载</button></>)}</td></tr>
                ); })}</tbody></table></div>
            )}
          </Card>
        </div>
      )}
      {act && <ActionDialog act={act} onClose={() => setAct(null)} onDone={reload} />}
    </>
  );
}
