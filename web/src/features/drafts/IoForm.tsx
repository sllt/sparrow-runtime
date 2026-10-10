// Source/Sink editor. Common kinds get a dedicated form; every other kind the
// build supports is edited as raw JSON (no form ≠ unsupported). Fields the
// form does not know are kept verbatim — the form only writes its own keys.
import { useMemo, useState } from "react";
import { obj, str, type Json } from "../../api/json";
import { pretty } from "../../lib/specText";
import { parseJson } from "../../api/json";

type FieldDef = { key: string; label: string; kind: "text" | "bool" | "secret"; placeholder?: string; hint?: string };

export const FORMS: Record<"source" | "sink", Record<string, FieldDef[]>> = {
  source: {
    mqtt: [
      { key: "host", label: "Broker 主机", kind: "text", placeholder: "mqtt.internal" },
      { key: "port", label: "端口", kind: "text", placeholder: "1883" },
      { key: "topic", label: "主题", kind: "text", placeholder: "sensors/+/json" },
      { key: "client_id", label: "Client ID", kind: "text" },
      { key: "use_demo_io", label: "使用内置演示 I/O", kind: "bool", hint: "仅用于演示/测试环境" },
    ],
    file: [{ key: "path", label: "文件路径", kind: "text", placeholder: "data/input.ndjson", hint: "受服务端文件根目录策略约束" }],
    nats: [
      { key: "url", label: "服务器 URL", kind: "text", placeholder: "nats://nats:4222" },
      { key: "subject", label: "Subject", kind: "text" },
    ],
    jetstream: [
      { key: "url", label: "服务器 URL", kind: "text", placeholder: "nats://nats:4222" },
      { key: "stream", label: "Stream", kind: "text" },
      { key: "consumer", label: "Durable consumer", kind: "text" },
      { key: "subject", label: "过滤 subject", kind: "text" },
    ],
    http_poll: [{ key: "url", label: "URL", kind: "text" }],
  },
  sink: {
    http: [
      { key: "url", label: "URL", kind: "text", placeholder: "https://hooks.internal/ingest", hint: "目标须在 allowlist 内" },
      { key: "use_demo_io", label: "使用内置演示 I/O", kind: "bool" },
    ],
    file: [{ key: "path", label: "文件路径", kind: "text" }],
    mqtt: [
      { key: "host", label: "Broker 主机", kind: "text" },
      { key: "port", label: "端口", kind: "text" },
      { key: "topic", label: "主题", kind: "text" },
    ],
    nats: [
      { key: "url", label: "服务器 URL", kind: "text" },
      { key: "subject", label: "Subject", kind: "text" },
    ],
    jetstream: [
      { key: "url", label: "服务器 URL", kind: "text" },
      { key: "subject", label: "Subject", kind: "text" },
    ],
    log: [],
  },
};

export function IoEditor({ role, value, onChange, kinds, readOnly }: { role: "source" | "sink"; value: Json | undefined; onChange: (v: Json) => void; kinds: string[]; readOnly?: boolean }) {
  const o = obj(value) ?? {};
  const kind = str(o.kind) ?? "";
  const form = FORMS[role][kind];
  const [raw, setRaw] = useState<string | null>(null);
  const rawText = useMemo(() => raw ?? pretty(o), [raw, o]);
  const [rawErr, setRawErr] = useState<string | null>(null);
  const set = (k: string, v: Json | undefined) => {
    const next: Record<string, Json> = {};
    let placed = false;
    for (const [kk, vv] of Object.entries(o)) { if (kk === k) { placed = true; if (v !== undefined) next[kk] = v; } else next[kk] = vv; }
    if (!placed && v !== undefined) next[k] = v;
    onChange(next);
  };
  const allKinds = Array.from(new Set([...kinds, kind].filter(Boolean))).sort();
  return (
    <div className="stack">
      <label><span className="field-label">{role === "source" ? "Source 类型" : "Sink 类型"}</span>
        <select className="input" value={kind} disabled={readOnly} onChange={(e) => onChange({ kind: e.target.value })} aria-label={`${role} kind`}>
          {allKinds.map((k) => <option key={k} value={k}>{k}{kinds.includes(k) ? "" : "（本构建未声明）"}</option>)}
        </select>
      </label>
      {kind && !kinds.includes(kind) && <div className="banner tone-warn"><span>本构建 capabilities 未声明 <code>{kind}</code>；发布时服务端会拒绝。</span></div>}
      {form ? (
        <div className="grid cols-2">
          {form.map((f) => f.kind === "bool" ? (
            <label key={f.key} className="row check full"><input type="checkbox" disabled={readOnly} checked={o[f.key] === true} onChange={(e) => set(f.key, e.target.checked ? true : undefined)} /> {f.label}{f.hint && <span className="hint">（{f.hint}）</span>}</label>
          ) : (
            <label key={f.key}><span className="field-label">{f.label}</span>
              <input className="input" disabled={readOnly} value={typeof o[f.key] === "string" ? (o[f.key] as string) : o[f.key] === undefined ? "" : String(o[f.key])} placeholder={f.placeholder}
                onChange={(e) => {
                  const t = e.target.value;
                  if (t === "") return set(f.key, undefined);
                  set(f.key, f.key === "port" && /^\d+$/.test(t) ? Number(t) : t);
                }} />
              {f.hint && <span className="hint">{f.hint}</span>}
            </label>
          ))}
        </div>
      ) : kind ? <div className="hint">“{kind}” 没有专用表单，请在下方 JSON 中编辑；这不代表不支持。</div> : null}
      <details className="raw" open={!form}>
        <summary>完整 {role} JSON（保留所有字段，包括表单未覆盖的）</summary>
        <textarea className="input mono raw-json" spellCheck={false} readOnly={readOnly} value={rawText} aria-label={`${role} JSON`} onBlur={() => { if (!rawErr) setRaw(null); }}
          onChange={(e) => {
            setRaw(e.target.value);
            try { const v = parseJson(e.target.value); if (!obj(v)) throw new Error("必须是对象"); setRawErr(null); onChange(v); }
            catch (err) { setRawErr(err instanceof Error ? err.message : String(err)); }
          }} />
        {rawErr && <div className="hint" style={{ color: "var(--bad)" }}>JSON 无效：{rawErr}（未应用）</div>}
      </details>
    </div>
  );
}
