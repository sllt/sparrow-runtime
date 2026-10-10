import { useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError } from "../../api/useLoad";
import { numText, obj, parseJson, str, stringifyJson, type Json } from "../../api/json";
import { Card, Pill } from "../../components/ui";
import { Icon } from "../../components/Icon";
import { Modal } from "../../components/Modal";

type Ev =
  | { type: "data"; row: string }
  | { type: "advance_clock"; to: string }
  | { type: "watermark"; micros: string }
  | { type: "eof" };

const TYPE_LABEL: Record<Ev["type"], string> = { data: "数据", advance_clock: "推进时钟", watermark: "水位线", eof: "EOF" };
const TONE: Record<Ev["type"], "info" | "warn" | "ok" | "idle"> = { data: "info", advance_clock: "warn", watermark: "ok", eof: "idle" };
const IDLE_MS = 5 * 60_000;
const NOT_VERIFIED_ZH: Record<string, string> = {
  "source/sink connections and credentials": "Source/Sink 连接与凭据",
  "delivery guarantees and outbox": "交付保证与 outbox",
  "checkpoint, recovery and restore": "checkpoint、恢复与 restore",
  "wall-clock scheduling and backpressure": "真实墙钟调度与背压",
};

function defaults(): Ev[] {
  return [
    { type: "data", row: '{"device_id": "d1", "temperature": 31.5, "ts": 1700000000000000}' },
    { type: "data", row: '{"device_id": "d2", "temperature": 18.0, "ts": 1700000001000000}' },
  ];
}
const isInt = (s: string) => /^-?\d+$/.test(s.trim());
export function fmtMicros(v: string | null): string {
  if (v === null) return "—";
  if (v === "-1") return "未设置";
  if (v === "9223372036854775807") return "+∞";
  const n = Number(v);
  if (!Number.isFinite(n)) return v;
  return Math.abs(n) >= 1000 ? `${(n / 1e6).toLocaleString("zh-CN", { maximumFractionDigits: 6 })} s` : `${v} μs`;
}

/** Build the wire events; numbers stay literal text (lossless). */
export function toWire(evs: Ev[]): { events: Json[] } | { error: string; index: number } {
  const out: Json[] = [];
  for (const [i, e] of evs.entries()) {
    try {
      if (e.type === "data") {
        const row = obj(parseJson(e.row));
        if (!row) throw new Error("数据必须是 JSON 对象");
        out.push({ type: "data", row });
      } else if (e.type === "advance_clock") {
        if (!isInt(e.to)) throw new Error("时钟必须是整数微秒");
        out.push(parseJson(`{"type":"advance_clock","to_micros":${e.to.trim()}}`));
      } else if (e.type === "watermark") {
        if (!isInt(e.micros)) throw new Error("水位线必须是整数微秒");
        out.push(parseJson(`{"type":"watermark","micros":${e.micros.trim()}}`));
      } else out.push({ type: "eof" });
    } catch (x) { return { error: x instanceof Error ? x.message : String(x), index: i }; }
  }
  return { events: out };
}

/** Local ordering hints; the server re-checks and is authoritative. */
export function timelineIssue(start: string, evs: Ev[]): string | null {
  let clock = isInt(start) ? BigInt(start.trim()) : 0n, wm = -1n, eof = false;
  for (const [i, e] of evs.entries()) {
    if (eof) return `第 ${i + 1} 步：EOF 之后不能再有事件`;
    if (e.type === "advance_clock" && isInt(e.to)) { const t = BigInt(e.to.trim()); if (t < clock) return `第 ${i + 1} 步：时钟不能倒退`; clock = t; }
    if (e.type === "watermark" && isInt(e.micros)) { const t = BigInt(e.micros.trim()); if (t < wm) return `第 ${i + 1} 步：水位线不能倒退`; wm = t; }
    if (e.type === "eof") eof = true;
  }
  return null;
}
function lastClock(start: string, evs: Ev[]): bigint {
  let c = isInt(start) ? BigInt(start.trim()) : 0n;
  for (const e of evs) if (e.type === "advance_clock" && isInt(e.to)) c = BigInt(e.to.trim());
  return c;
}

/**
 * `/v1/preview`: the server binds the draft's plan, swaps its I/O for an
 * in-memory source/capture and steps a virtual clock. Samples and results
 * live only in this component; they are dropped on navigation/logout and
 * after 5 idle minutes, and are never saved into the draft.
 */
export function PreviewPanel({ spec }: { spec: Record<string, Json> | null }) {
  const { client } = useAuth();
  const [start, setStart] = useState("0");
  const [evs, setEvs] = useState<Ev[]>(defaults);
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<Record<string, Json> | null>(null);
  const [err, setErr] = useState<{ text: string; index?: number } | null>(null);
  const [touched, setTouched] = useState(Date.now());
  const [focus, setFocus] = useState<number | null>(null);
  const [exporting, setExporting] = useState(false);
  const abort = useRef<AbortController | null>(null);
  useEffect(() => { const t = setTimeout(() => { setRes(null); setEvs(defaults()); }, IDLE_MS); return () => clearTimeout(t); }, [touched]);
  useEffect(() => () => abort.current?.abort(), []);
  const issue = useMemo(() => timelineIssue(start, evs), [start, evs]);
  const touch = () => setTouched(Date.now());

  const add = (e: Ev) => { touch(); setEvs((l) => [...l, e]); };
  const setAt = (i: number, e: Ev) => { touch(); setEvs((l) => l.map((x, k) => (k === i ? e : x))); };
  const delAt = (i: number) => { touch(); setEvs((l) => l.filter((_, k) => k !== i)); };
  const move = (i: number, d: -1 | 1) => { touch(); setEvs((l) => { const n = [...l]; const j = i + d; if (j < 0 || j >= n.length) return l; [n[i], n[j]] = [n[j]!, n[i]!]; return n; }); };

  async function run() {
    if (!client || !spec) return;
    touch(); setErr(null);
    if (!isInt(start)) { setErr({ text: "起始时钟必须是整数微秒" }); return; }
    const w = toWire(evs);
    if ("error" in w) { setErr({ text: `第 ${w.index + 1} 步：${w.error}`, index: w.index }); return; }
    abort.current?.abort();
    const ac = new AbortController(); abort.current = ac;
    setBusy(true);
    try {
      const body = { spec, start_micros: parseJson(start.trim()), events: w.events, limits: { output_rows: 1000, timeout_ms: 5000 } };
      const r = await client.request("POST", "/v1/preview", { body, signal: ac.signal });
      setRes(obj(r.data)); setFocus(null);
    } catch (e) {
      if (ac.signal.aborted) return;
      setRes(null); setErr({ text: asApiError(e).message });
    } finally { if (abort.current === ac) setBusy(false); }
  }
  function cancel() { abort.current?.abort(); setBusy(false); setErr({ text: "已取消；服务端会在请求断开后停止执行并释放额度。" }); }
  function doExport() {
    const blob = new Blob([stringifyJson({ start_micros: start, events: toWire(evs), result: res })], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "sparrow-preview.json"; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
    setExporting(false);
  }

  const steps = Array.isArray(res?.steps) ? (res!.steps as Json[]).map((s) => obj(s) ?? {}) : [];
  const rows = steps.flatMap((s, i) => (Array.isArray(s.rows) ? (s.rows as Json[]) : []).map((r) => ({ step: i, row: obj(r) ?? {} })));
  const cols = Array.from(new Set(rows.flatMap((r) => Object.keys(r.row))));
  const shown = focus === null ? rows : rows.filter((r) => r.step === focus);
  const lc = lastClock(start, evs);

  return (
    <Card title="受控预览" sub="虚拟时钟 · 内存输入与捕获 · 不连接 Source/Sink · 不涉及 checkpoint"
      actions={busy ? <button className="btn" onClick={cancel}><Icon name="stop" size={14} />取消</button>
        : <button className="btn primary" onClick={() => void run()} disabled={!spec || !!issue}><Icon name="play" size={14} />运行</button>}>
      {!spec ? <div className="hint">完整配置 JSON 可解析后才能预览。</div> : (
        <div className="stack">
          <div className="row small">
            <label className="row" style={{ gap: 6 }}><span className="muted">起始时钟</span>
              <input className="input mono" style={{ width: 150, height: 30 }} value={start} onChange={(e) => { touch(); setStart(e.target.value); }} aria-label="起始时钟（微秒）" /></label>
            <span className="muted">μs，固定值，不读取服务器墙钟</span>
          </div>
          <ol className="pv-list" aria-label="预览事件">
            {evs.map((e, i) => (
              <li key={i} className={`pv-ev${err?.index === i ? " bad" : ""}`}>
                <span className="pv-idx mono">{i + 1}</span>
                <Pill tone={TONE[e.type]}>{TYPE_LABEL[e.type]}</Pill>
                {e.type === "data" && <input className="input mono pv-in" value={e.row} onChange={(x) => setAt(i, { ...e, row: x.target.value })} aria-label={`第 ${i + 1} 步数据`} spellCheck={false} />}
                {e.type === "advance_clock" && <><input className="input mono pv-num" value={e.to} onChange={(x) => setAt(i, { ...e, to: x.target.value })} aria-label={`第 ${i + 1} 步时钟`} /><span className="muted small">{fmtMicros(isInt(e.to) ? e.to.trim() : null)}</span></>}
                {e.type === "watermark" && <><input className="input mono pv-num" value={e.micros} onChange={(x) => setAt(i, { ...e, micros: x.target.value })} aria-label={`第 ${i + 1} 步水位线`} /><span className="muted small">{fmtMicros(isInt(e.micros) ? e.micros.trim() : null)}</span></>}
                {e.type === "eof" && <span className="muted small">输入结束；事件时间窗口在此全部关闭</span>}
                <span className="spacer" />
                <button className="btn ghost icon sm" onClick={() => move(i, -1)} disabled={i === 0} aria-label={`上移第 ${i + 1} 步`}>↑</button>
                <button className="btn ghost icon sm" onClick={() => move(i, 1)} disabled={i === evs.length - 1} aria-label={`下移第 ${i + 1} 步`}>↓</button>
                <button className="btn ghost icon sm" onClick={() => delAt(i)} aria-label={`删除第 ${i + 1} 步`}><Icon name="x" size={14} /></button>
              </li>
            ))}
          </ol>
          <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>
            <button className="btn sm" onClick={() => add({ type: "data", row: (evs.filter((x) => x.type === "data").pop() as { row: string } | undefined)?.row ?? "{}" })}><Icon name="plus" size={13} />数据</button>
            <button className="btn sm" onClick={() => add({ type: "advance_clock", to: String(lc + 1_000_000n) })}><Icon name="clock" size={13} />时钟 +1s</button>
            <button className="btn sm" onClick={() => add({ type: "watermark", micros: "0" })}><Icon name="activity" size={13} />水位线</button>
            <button className="btn sm" onClick={() => add({ type: "eof" })} disabled={evs.some((x) => x.type === "eof")}>EOF</button>
            <span className="spacer" />
            <button className="btn ghost sm" onClick={() => { touch(); setEvs(defaults()); setRes(null); setErr(null); }}>重置</button>
          </div>
          {issue && <div className="hint tone-warn" role="status">⚠ {issue}</div>}
          {err && <div className="err-box mono small" role="alert">{err.text}</div>}
          {res && (
            <>
              <div className="row small" style={{ flexWrap: "wrap" }}>
                <Pill tone={res.complete === true ? "ok" : "warn"}>{res.complete === true ? "完整" : "不完整"}</Pill>
                <span className="muted">输入 {numText(res.input_rows)} 行 → 输出 {numText(res.output_rows)} 行</span>
                <span className="muted">· 最终时钟 {fmtMicros(numText(res.final_clock_micros))} · 水位 {fmtMicros(numText(res.final_watermark_micros))}</span>
                {numText(res.future_dropped) !== "0" && <Pill tone="warn">丢弃 {numText(res.future_dropped)}</Pill>}
                <span className="spacer" />
                <button className="btn ghost sm" onClick={() => setExporting(true)}><Icon name="download" size={13} />导出</button>
                <button className="btn ghost sm" onClick={() => { setRes(null); setFocus(null); }}>清空结果</button>
              </div>
              <div className="pv-results">
              <div className="table-wrap">
                <table className="table compact pv-steps">
                  <thead><tr><th>#</th><th>事件</th><th>时钟</th><th>水位</th><th>输出</th></tr></thead>
                  <tbody>{steps.map((s, i) => {
                    const n = Number(numText(s.output_rows) ?? 0);
                    const e = evs[i];
                    return (
                      <tr key={i} className={focus === i ? "sel" : undefined} onClick={() => n && setFocus(focus === i ? null : i)} style={{ cursor: n ? "pointer" : undefined }}>
                        <td className="mono">{i + 1}</td><td>{e ? TYPE_LABEL[e.type] : "?"}</td>
                        <td className="mono">{fmtMicros(numText(s.clock_micros))}</td><td className="mono">{fmtMicros(numText(s.watermark_micros))}</td>
                        <td>{n ? <Pill tone="info">{n} 行</Pill> : <span className="muted">—</span>}</td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
              {rows.length === 0 ? <div className="hint">没有输出行。时间窗口只在时钟/水位越过边界或 EOF 时输出。</div> : (
                <div className="table-wrap">
                  <table className="table compact">
                    <thead><tr><th>步骤</th>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                    <tbody>{shown.map((r, i) => <tr key={i}><td className="mono">{r.step + 1}</td>{cols.map((c) => <td key={c} className="mono">{r.row[c] === undefined || r.row[c] === null ? <span className="muted">null</span> : numText(r.row[c]) ?? (typeof r.row[c] === "string" ? (r.row[c] as string) : stringifyJson(r.row[c]))}</td>)}</tr>)}</tbody>
                  </table>
                </div>
              )}
              </div>
              <div className="hint">
                未验证：{(Array.isArray(res.not_verified) ? res.not_verified : []).map((x) => NOT_VERIFIED_ZH[str(x) ?? ""] ?? str(x)).join("、")}。
                {Array.isArray(res.ignored_fields) && res.ignored_fields.length > 0 && <> 未参与的配置：{(res.ignored_fields as Json[]).map((x) => <code key={String(x)} style={{ marginRight: 4 }}>{str(x)}</code>)}。</>}
                样本与结果只在当前页面内存中，闲置 5 分钟、离开页面或注销后清除。
              </div>
            </>
          )}
        </div>
      )}
      {exporting && (
        <Modal title="导出预览数据" onClose={() => setExporting(false)}
          footer={<><button className="btn ghost" onClick={() => setExporting(false)}>取消</button><button className="btn primary" onClick={doExport}>我了解，导出</button></>}>
          <p>导出文件包含你输入的样本事件和输出结果，可能含业务数据。文件不会上传到服务器，请妥善保管。</p>
        </Modal>
      )}
    </Card>
  );
}
