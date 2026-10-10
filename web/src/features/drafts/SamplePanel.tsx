import { useEffect, useState } from "react";
import { useAuth } from "../../auth/AuthContext";
import { asApiError } from "../../api/useLoad";
import { numText, obj, parseJson, str, stringifyJson, type Json } from "../../api/json";
import { Card, Pill } from "../../components/ui";
import { Icon } from "../../components/Icon";

const EXAMPLE = '[\n  {"device_id": "d1", "temperature": 31.5, "ts": 1700000000000000},\n  {"device_id": "d2", "temperature": 18.0, "ts": 1700000001000000}\n]';

/**
 * Finite sample query (/v1/query): inline rows only, no Source/Sink I/O, no
 * recovery. Samples and results live only in this component's memory and are
 * dropped on navigation/logout and after 5 idle minutes.
 */
export function SamplePanel({ spec }: { spec: Record<string, Json> | null }) {
  const { client } = useAuth();
  const [rows, setRows] = useState(EXAMPLE);
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<Record<string, Json> | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [touched, setTouched] = useState(Date.now());
  useEffect(() => {
    const t = setTimeout(() => { setRes(null); }, 5 * 60_000);
    return () => clearTimeout(t);
  }, [touched]);
  const sql = spec ? str(spec.sql) : null;
  const stream = spec ? str(spec.stream) : null;

  async function run() {
    if (!client || !sql || !stream) return;
    setBusy(true); setErr(null); setTouched(Date.now());
    try {
      const parsed = parseJson(rows);
      if (!Array.isArray(parsed)) throw new Error("样本必须是 JSON 数组");
      const v = await client.send("POST", "/v1/query", { sql, inputs: [{ stream, rows: parsed }], limits: { output_rows: 200, timeout_ms: 5000 } });
      setRes(obj(v));
    } catch (e) { setErr(e instanceof SyntaxError || !(e as { kind?: string }).kind ? String((e as Error).message ?? e) : asApiError(e).message); setRes(null); }
    finally { setBusy(false); }
  }
  const out = Array.isArray(res?.rows) ? (res!.rows as Json[]).map((r) => obj(r) ?? {}) : [];
  const cols = Array.from(new Set(out.flatMap((r) => Object.keys(r))));
  return (
    <Card title="样本查询" sub="有限执行：内联样本，不连接 Source/Sink，不涉及恢复"
      actions={<button className="btn" onClick={run} disabled={busy || !sql}><Icon name="play" size={14} />{busy ? "运行中…" : "运行"}</button>}>
      {!sql ? <div className="hint">需要可解析且含顶层 <code>sql</code> 的配置。</div> : (
        <div className="stack">
          <label><span className="field-label">输入样本（{stream} 的行，JSON 数组）</span>
            <textarea className="input mono raw-json" style={{ minHeight: 110 }} spellCheck={false} value={rows} onChange={(e) => setRows(e.target.value)} aria-label="样本输入" />
          </label>
          {err && <div className="err-box mono small" role="alert">{err}</div>}
          {res && (
            <>
              <div className="row small">
                <Pill tone={res.complete === true ? "ok" : "warn"}>{res.complete === true ? "完整" : "不完整"}</Pill>
                <span className="muted">输入 {numText(res.input_rows)} 行 → 输出 {numText(res.output_rows)} 行</span>
                {numText(res.future_dropped) !== "0" && <Pill tone="warn">丢弃 {numText(res.future_dropped)}</Pill>}
                <span className="spacer" /><button className="btn ghost" onClick={() => setRes(null)}>清空</button>
              </div>
              {out.length === 0 ? <div className="hint">没有输出行。</div> : (
                <div className="table-wrap">
                  <table className="table compact">
                    <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                    <tbody>{out.map((r, i) => <tr key={i}>{cols.map((c) => <td key={c} className="mono">{r[c] === undefined || r[c] === null ? <span className="muted">null</span> : numText(r[c]) ?? (typeof r[c] === "string" ? (r[c] as string) : stringifyJson(r[c]))}</td>)}</tr>)}</tbody>
                  </table>
                </div>
              )}
              <div className="hint">结果只保存在当前页面内存中，离开页面或闲置 5 分钟后清除。</div>
            </>
          )}
        </div>
      )}
    </Card>
  );
}
