import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, useLoad } from "../../api/useLoad";
import { enc } from "../../api/client";
import { obj, str, type Json } from "../../api/json";
import { Card, ErrorView, Kbd, Pill, Skeleton, Tabs } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { CodeEditor } from "../../components/CodeEditor";
import { parseSpec, setField, summarizeIo } from "../../lib/specText";
import { fmtAgo } from "../../lib/format";
import { MODE_LABEL, parseDraft, type Draft } from "./model";
import { IoEditor } from "./IoForm";
import { connectorKinds } from "./caps";
import { CheckPanel, type CheckResult } from "./CheckPanel";
import { SamplePanel } from "./SamplePanel";
import { PublishDialog } from "./PublishDialog";

type Tab = "sql" | "json" | "io";

export default function DraftEditorPage() {
  const { id = "" } = useParams();
  const [st, reload] = useLoad(async (c, s) => {
    const [d, caps, conns] = await Promise.all([
      c.request("GET", `/v1/drafts/${enc(id)}`, { signal: s }),
      c.get("/v1/capabilities", s),
      c.get("/v1/connections", s).catch(() => null),
    ]);
    return { draft: parseDraft(d.data), caps, conns };
  }, `draft:${id}`);
  if (st.loading && !st.data) return <Skeleton rows={8} />;
  if (st.error && !st.data) return <ErrorView error={st.error} onRetry={reload} />;
  return <Editor key={st.data!.draft.etag === "" ? id : id} initial={st.data!.draft} caps={st.data!.caps} conns={st.data!.conns} />;
}

function Editor({ initial, caps, conns }: { initial: Draft; caps: Json; conns: Json }) {
  const { client, me } = useAuth();
  const nav = useNavigate();
  const [server, setServer] = useState(initial); // last saved/loaded server copy
  const [text, setText] = useState(initial.text);
  const [meta, setMeta] = useState<Record<string, Json>>(obj(initial.metadata) ?? {});
  const [tab, setTab] = useState<Tab>("sql");
  const [saving, setSaving] = useState(false);
  const [conflict, setConflict] = useState<{ current: string | null } | null>(null);
  const [notice, setNotice] = useState<{ tone: "ok" | "bad" | "warn"; text: string } | null>(null);
  const [check, setCheck] = useState<CheckResult | null>(null);
  const [checking, setChecking] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const dirty = text !== server.text || JSON.stringify(meta) !== JSON.stringify(obj(server.metadata) ?? {});
  const parsed = useMemo(() => parseSpec(text), [text]);
  const sqlText = parsed.ok ? str(parsed.value.sql) : null;
  const kinds = useMemo(() => connectorKinds(caps), [caps]);
  const templates = useMemo(() => (Array.isArray(obj(conns)?.connections) ? (obj(conns)!.connections as Json[]) : []).map((c) => obj(c)!).filter(Boolean), [conns]);
  const canWrite = me?.role !== "viewer";

  // Warn before leaving with unsaved edits.
  useEffect(() => {
    const h = (e: BeforeUnloadEvent) => { if (dirty) { e.preventDefault(); } };
    window.addEventListener("beforeunload", h);
    return () => window.removeEventListener("beforeunload", h);
  }, [dirty]);

  const save = useCallback(async (force?: string) => {
    if (!client || saving) return null;
    setSaving(true); setNotice(null);
    try {
      const r = await client.request("PUT", `/v1/drafts/${enc(server.id)}`, {
        ifMatch: force ?? server.etag,
        body: { pipeline: server.pipeline, mode: server.mode, text, metadata: meta, base_etag: server.baseEtag },
      });
      const next = { ...server, etag: r.etag ?? server.etag, text, metadata: meta, updatedAt: Date.now(), updatedBy: me?.actor ?? "" };
      setServer(next); setConflict(null);
      setNotice({ tone: "ok", text: `已保存为 ${next.etag}` });
      return next;
    } catch (e) {
      const a = asApiError(e);
      if (a.kind === "conflict") setConflict({ current: a.context.current_etag ?? null });
      else setNotice({ tone: "bad", text: `保存失败：${a.message}` });
      return null;
    } finally { setSaving(false); }
  }, [client, saving, server, text, meta, me]);

  const runCheck = useCallback(async () => {
    if (!client) return;
    let d = server;
    if (dirty) { const s = await save(); if (!s) return; d = s; }
    setChecking(true);
    try {
      const v = obj(await client.send("POST", `/v1/drafts/${enc(d.id)}/check`, undefined));
      setCheck({ raw: v ?? {}, etag: str(v?.draft_etag) ?? d.etag, at: Date.now() });
    } catch (e) { setNotice({ tone: "bad", text: `检查失败：${asApiError(e).message}` }); }
    finally { setChecking(false); }
  }, [client, server, dirty, save]);

  // Keyboard: Ctrl/Cmd+S save, Ctrl/Cmd+Enter check.
  const keys = useRef({ save, runCheck });
  keys.current = { save, runCheck };
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key.toLowerCase() === "s") { e.preventDefault(); void keys.current.save(); }
      if (e.key === "Enter") { e.preventDefault(); void keys.current.runCheck(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  async function loadLatest(discard: boolean) {
    if (!client) return;
    const r = await client.request("GET", `/v1/drafts/${enc(server.id)}`);
    const d = parseDraft(r.data);
    setServer(d);
    if (discard) { setText(d.text); setMeta(obj(d.metadata) ?? {}); setNotice({ tone: "warn", text: `已加载 ${d.etag}（${d.updatedBy}），本地修改已丢弃` }); }
    setConflict(null);
    return d;
  }

  async function del() {
    if (!client || !confirm(`删除草稿 “${server.id}”？已发布的流水线不受影响。`)) return;
    try { await client.request("DELETE", `/v1/drafts/${enc(server.id)}`, { ifMatch: server.etag }); nav("/drafts"); }
    catch (e) { const a = asApiError(e); if (a.kind === "conflict") setConflict({ current: a.context.current_etag ?? null }); else setNotice({ tone: "bad", text: a.message }); }
  }

  const io = summarizeIo(parsed.ok ? parsed.value : null);
  const applyTemplate = (role: "source" | "sink", name: string) => {
    const t = templates.find((x) => str(x.name) === name);
    if (!t || !parsed.ok) return;
    const next = setField(text, role, t.spec ?? {});
    if (next) {
      setText(next);
      const refs = obj(meta.connection_refs) ?? {};
      setMeta({ ...meta, connection_refs: { ...refs, [role]: { name, version: t.version ?? null } } });
      setNotice({ tone: "ok", text: `已将连接模板 “${name}” (v${String(t.version)}) 的参数复制进草稿；以后修改模板不会改变已发布的流水线。` });
    }
  };

  return (
    <>
      <div className="page-head">
        <div style={{ minWidth: 0 }}>
          <div className="row" style={{ gap: 10 }}>
            <h1 className="page-title">{server.id}</h1>
            <Pill tone="info">{MODE_LABEL[server.mode] ?? server.mode}</Pill>
            {dirty ? <Pill tone="warn">未保存</Pill> : <Pill tone="ok">已保存</Pill>}
          </div>
          <div className="page-sub">
            目标 <strong>{server.pipeline}</strong> · <span className="mono">{server.etag}</span> · 基线 <span className="mono">{server.baseEtag ?? "新流水线"}</span> · {server.updatedBy} 编辑于 {fmtAgo(server.updatedAt, Date.now())}
          </div>
        </div>
        <div className="page-actions">
          <button className="btn ghost icon" onClick={del} title="删除草稿" aria-label="删除草稿" disabled={!canWrite}><Icon name="trash" size={16} /></button>
          <button className="btn" onClick={() => void save()} disabled={!dirty || saving || !canWrite}><Icon name="save" size={16} />{saving ? "保存中…" : "保存"}<Kbd>Ctrl S</Kbd></button>
          <button className="btn" onClick={() => void runCheck()} disabled={checking}><Icon name="check" size={16} />{checking ? "检查中…" : "检查"}<Kbd>Ctrl ↵</Kbd></button>
          <button className="btn primary" onClick={() => setPublishing(true)} disabled={dirty || !parsed.ok || !canWrite} title={dirty ? "先保存草稿" : undefined}><Icon name="upload" size={16} />发布…</button>
        </div>
      </div>

      {conflict && (
        <div className="banner tone-warn" role="alert">
          <Icon name="alert" />
          <div className="stack" style={{ gap: 8 }}>
            <div><strong>草稿已被其他人修改</strong>（服务端当前为 <span className="mono">{conflict.current ?? "未知"}</span>，你的编辑基于 <span className="mono">{server.etag}</span>）。你的修改仍保留在编辑器中，没有被覆盖。</div>
            <div className="row">
              <button className="btn" onClick={() => void loadLatest(true)}>放弃我的修改，加载最新</button>
              <button className="btn" onClick={async () => { const d = await loadLatest(false); if (d) void save(d.etag); }}>以我的版本覆盖最新</button>
            </div>
          </div>
        </div>
      )}
      {notice && <div className={`banner tone-${notice.tone === "ok" ? "info" : notice.tone}`} role="status"><Icon name={notice.tone === "ok" ? "check" : "alert"} />{notice.text}</div>}

      <div className="grid editor-grid">
        <Card pad={false}>
          <div className="editor-tabs" style={{ padding: "0 var(--sp-5)" }}>
            <Tabs<Tab> value={tab} onChange={setTab} tabs={[
              { key: "sql", label: "SQL" },
              { key: "io", label: `Source / Sink`, badge: <span className="chip">{io.source} → {io.sink}</span> },
              { key: "json", label: "完整配置 JSON" },
            ]} />
          </div>
          <div className="editor-pane">
            {tab === "sql" && (parsed.ok && sqlText !== null ? (
              <CodeEditor language="sql" label="SQL 编辑器" value={sqlText} readOnly={!canWrite} onSave={() => void save()}
                onChange={(v) => { const n = setField(text, "sql", v); if (n !== null) setText(n); }} minHeight={360} />
            ) : (
              <div className="pane-msg">
                {parsed.ok ? <>该配置没有顶层 <code>sql</code> 字段（可能是 Graph 模式）。请在“完整配置 JSON”中编辑。</> : <>完整配置 JSON 当前无法解析：<code>{parsed.error}</code>。可以先保存无效文本，修复后再使用 SQL 视图。</>}
              </div>
            ))}
            {tab === "json" && <CodeEditor language="json" label="完整配置 JSON 编辑器" value={text} onChange={setText} readOnly={!canWrite} onSave={() => void save()} minHeight={420} />}
            {tab === "io" && (parsed.ok ? (
              <div className="grid cols-2" style={{ padding: "var(--sp-5)", alignItems: "start" }}>
                {(["source", "sink"] as const).map((role) => (
                  <div key={role} className="io-col">
                    <div className="row" style={{ marginBottom: 12 }}>
                      <h3 className="card-title">{role === "source" ? "Source" : "Sink"}</h3>
                      <span className="spacer" />
                      {templates.filter((t) => str(t.role) === role).length > 0 && (
                        <select className="input" style={{ height: 32, width: "auto" }} value="" aria-label={`从连接模板套用 ${role}`} onChange={(e) => applyTemplate(role, e.target.value)}>
                          <option value="">从连接模板套用…</option>
                          {templates.filter((t) => str(t.role) === role).map((t) => <option key={str(t.name)!} value={str(t.name)!}>{str(t.name)} · {str(t.kind)} · v{String(t.version)}</option>)}
                        </select>
                      )}
                    </div>
                    {(() => { const ref = obj(obj(meta.connection_refs)?.[role]); return ref ? <div className="hint" style={{ marginBottom: 8 }}>来源：连接模板 <code>{str(ref.name)}</code> v{String(ref.version)}（已展开复制，发布后独立于模板）</div> : null; })()}
                    <IoEditor role={role} value={parsed.value[role]} kinds={kinds[role]} readOnly={!canWrite}
                      onChange={(v) => { const n = setField(text, role, v); if (n !== null) setText(n); }} />
                  </div>
                ))}
              </div>
            ) : <div className="pane-msg">完整配置 JSON 无法解析，表单不可用：<code>{parsed.error}</code></div>)}
          </div>
        </Card>
        <div className="stack">
          <CheckPanel result={check} stale={check !== null && (check.etag !== server.etag || dirty)} onRun={() => void runCheck()} running={checking} />
          <SamplePanel spec={parsed.ok ? parsed.value : null} />
        </div>
      </div>

      {publishing && client && (
        <PublishDialog draft={server} onClose={() => setPublishing(false)}
          onPublished={() => void client.request("GET", `/v1/drafts/${enc(server.id)}`).then((r) => setServer(parseDraft(r.data))).catch(() => {})} />
      )}
    </>
  );
}


