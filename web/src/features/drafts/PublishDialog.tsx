import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../../auth/AuthContext";
import { asApiError, opId } from "../../api/useLoad";
import { enc } from "../../api/client";
import { numText, obj, str, stringifyJson, type Json } from "../../api/json";
import { Modal } from "../../components/Modal";
import { Pill, Skeleton } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { DiffView } from "../../components/DiffView";
import { parseSpec, pretty } from "../../lib/specText";
import type { Draft } from "./model";

type Phase =
  | { k: "review" }
  | { k: "publishing" }
  | { k: "done"; revision: string; etag: string; replayed: boolean }
  | { k: "unknown"; message: string } // timeout/network: result must be confirmed by receipt
  | { k: "failed"; message: string; conflict: boolean };

const same = (a: Json | undefined, b: Json | undefined) => stringifyJson(a ?? null) === stringifyJson(b ?? null);

export function PublishDialog({ draft, onClose, onPublished }: { draft: Draft; onClose: () => void; onPublished: () => void }) {
  const { client } = useAuth();
  const nav = useNavigate();
  const operation = useMemo(() => opId(), []); // one id per review; retries reuse it
  const [check, setCheck] = useState<Record<string, Json> | null>(null);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [ack, setAck] = useState(false);
  const [phase, setPhase] = useState<Phase>({ k: "review" });
  const [startMsg, setStartMsg] = useState<string | null>(null);

  useEffect(() => {
    const ac = new AbortController();
    client?.send("POST", `/v1/drafts/${enc(draft.id)}/check`, undefined, ac.signal).then((v) => setCheck(obj(v)), (e) => setLoadErr(asApiError(e).message));
    return () => ac.abort();
  }, [client, draft.id]);

  const target = parseSpec(draft.text);
  const cur = obj(check?.current);
  const curSpec = obj(cur?.spec);
  const valOk = obj(check?.validate)?.ok === true;
  const baseStale = cur ? str(cur.etag) !== draft.baseEtag : draft.baseEtag !== null;
  const tv = target.ok ? target.value : null;
  const changes = tv ? [
    { k: "SQL / Graph", changed: !same(curSpec?.sql, tv.sql) || !same(curSpec?.graph, tv.graph) },
    { k: "Source", changed: !same(curSpec?.source, tv.source), warn: true },
    { k: "Sink", changed: !same(curSpec?.sink, tv.sink), warn: true },
    { k: "Checkpoint / 恢复", changed: !same(curSpec?.checkpoint, tv.checkpoint) || !same(curSpec?.recovery, tv.recovery) || !same(curSpec?.delivery, tv.delivery), warn: true },
  ] : [];
  const eff = obj(obj(obj(check?.validate)?.report)?.effective);

  async function publish() {
    if (!client) return;
    setPhase({ k: "publishing" });
    try {
      const v = obj(await client.send("POST", `/v1/drafts/${enc(draft.id)}/publish`, { operation_id: operation, draft_etag: draft.etag, base_etag: draft.baseEtag }));
      const r = obj(v?.receipt);
      setPhase({ k: "done", revision: numText(r?.revision) ?? "?", etag: str(r?.etag) ?? "", replayed: r?.replayed === true });
      onPublished();
    } catch (e) {
      const a = asApiError(e);
      if (a.kind === "offline" || a.kind === "aborted" || a.kind === "unavailable" || (a.status ?? 0) >= 500) {
        setPhase({ k: "unknown", message: a.message });
        void confirmReceipt();
      } else setPhase({ k: "failed", message: a.message, conflict: a.kind === "conflict" });
    }
  }

  async function confirmReceipt() {
    if (!client) return;
    try {
      const r = obj(obj(await client.get(`/v1/publications/${enc(operation)}`))?.receipt);
      setPhase({ k: "done", revision: numText(r?.revision) ?? "?", etag: str(r?.etag) ?? "", replayed: true });
      onPublished();
    } catch (e) {
      const a = asApiError(e);
      setPhase({ k: "unknown", message: a.kind === "not_found" ? "服务端没有该操作的回执：本次发布很可能没有提交。可以用同一操作 ID 安全重试（不会重复发布）。" : `仍无法确认：${a.message}` });
    }
  }

  async function start(revision: string) {
    if (!client) return;
    setStartMsg("正在提交启动请求…");
    try {
      await client.send("POST", `/v1/pipelines/${enc(draft.pipeline)}/start`, { revision: Number(revision) });
      setStartMsg(null);
      onClose();
      nav(`/pipelines/${enc(draft.pipeline)}`);
    } catch (e) { setStartMsg(`启动请求失败：${asApiError(e).message}（未自动重试）`); }
  }

  const footer = phase.k === "done" ? (
    <>
      <button className="btn ghost" onClick={onClose}>稍后启动</button>
      <button className="btn primary" onClick={() => void start(phase.revision)}><Icon name="play" size={14} />启动 rev {phase.revision}</button>
    </>
  ) : phase.k === "unknown" ? (
    <>
      <button className="btn" onClick={() => void confirmReceipt()}><Icon name="refresh" size={14} />查询回执</button>
      <button className="btn primary" onClick={() => void publish()}>用同一操作 ID 重试</button>
    </>
  ) : (
    <>
      <button className="btn ghost" onClick={onClose}>取消</button>
      <button className="btn primary" disabled={!check || !valOk || !ack || phase.k === "publishing" || (phase.k === "failed" && phase.conflict)} onClick={() => void publish()}>
        <Icon name="upload" size={14} />{phase.k === "publishing" ? "发布中…" : "确认发布（不启动）"}
      </button>
    </>
  );

  return (
    <Modal wide title={`发布 ${draft.pipeline}`} sub={<>草稿 <span className="mono">{draft.id}@{draft.etag}</span> → 基线 <span className="mono">{draft.baseEtag ?? "新流水线"}</span> · 操作 ID <span className="mono">{operation}</span></>} onClose={onClose} footer={footer}>
      {loadErr ? <div className="banner tone-bad"><Icon name="alert" />无法完成发布前检查：{loadErr}</div> : !check ? <Skeleton rows={6} /> : (
        <div className="stack">
          {phase.k === "done" && (
            <div className="banner tone-info" role="status"><Icon name="check" /><div><strong>已发布 rev {phase.revision}</strong>（<span className="mono">{phase.etag}</span>{phase.replayed ? "，来自已有回执，未重复发布" : ""}）。流水线<strong>尚未启动</strong>；启动是单独的操作，以随后的状态页为准。</div></div>
          )}
          {phase.k === "unknown" && <div className="banner tone-warn" role="alert"><Icon name="clock" /><div><strong>结果待确认。</strong>{phase.message}</div></div>}
          {phase.k === "failed" && <div className="banner tone-bad" role="alert"><Icon name="alert" /><div><strong>{phase.conflict ? "发布冲突：" : "发布被拒绝："}</strong>{phase.message}{phase.conflict && "。草稿或流水线在审阅后被修改；关闭对话框，重新加载后再审阅。你的草稿内容保持不变。"}</div></div>}
          {startMsg && <div className="banner tone-warn">{startMsg}</div>}
          <div className="grid cols-3">
            <div className="mini"><div className="mini-label">完整校验</div>{valOk ? <Pill tone="ok">通过</Pill> : <Pill tone="bad">拒绝</Pill>}</div>
            <div className="mini"><div className="mini-label">基线</div>{baseStale ? <Pill tone="bad">已过期（当前 {str(cur?.etag) ?? "不存在"}）</Pill> : <Pill tone="ok">{draft.baseEtag ?? "新建"} 仍是最新</Pill>}</div>
            <div className="mini"><div className="mini-label">恢复结论</div><span className="chip">{str(eff?.recovery) ?? str(obj(obj(check.explain)?.report)?.recovery) ?? "未知"}</span></div>
          </div>
          {!valOk && <div className="banner tone-bad"><Icon name="alert" />{str(obj(obj(check.validate)?.error)?.message) ?? str(obj(obj(check.parse)?.error)?.message) ?? "校验未通过"}</div>}
          <div className="row" style={{ flexWrap: "wrap", gap: 8 }}>
            {changes.map((c) => <Pill key={c.k} tone={c.changed ? (c.warn ? "warn" : "info") : "idle"}>{c.k}：{c.changed ? "有变更" : "无变更"}</Pill>)}
          </div>
          <DiffView before={curSpec ? pretty(curSpec) : ""} after={tv ? pretty(tv) : draft.text} beforeLabel={cur ? `当前 rev ${numText(cur.revision)}` : "（不存在）"} afterLabel="发布后" />
          <div className="hint">发布只写入新的不可变 revision，不会停止或启动任何作业；HTTP 2xx 或本地 outbox 接受不等于业务 exactly-once。</div>
          {phase.k === "review" || phase.k === "failed" ? (
            <label className="row check"><input type="checkbox" checked={ack} onChange={(e) => setAck(e.target.checked)} />我已核对上述差异、Source/Sink 变更与恢复结论</label>
          ) : null}
        </div>
      )}
    </Modal>
  );
}
