import { useState } from "react";
import { useLoad } from "../../api/useLoad";
import { numText, obj, str, stringifyJson, type Json } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton, StateView, Tabs } from "../../components/ui";

type Tab = "tables" | "plugins";
const PREFERRED = ["name", "version", "revision", "latest_revision", "kind", "role", "row_count", "rows_count", "enabled", "state", "sha256", "manifest_sha256", "digest", "signature", "pins"];

function cell(v: Json | undefined): string {
  if (v === undefined || v === null) return "—";
  if (typeof v === "boolean") return v ? "是" : "否";
  const n = numText(v);
  if (n !== null) return n;
  if (typeof v === "string") return v.length > 24 && /^[0-9a-f]+$/.test(v) ? `${v.slice(0, 12)}…` : v;
  const s = stringifyJson(v);
  return s.length > 60 ? `${s.slice(0, 60)}…` : s;
}

function Generic({ items, empty }: { items: Record<string, Json>[]; empty: string }) {
  if (items.length === 0) return <StateView icon="inbox" title={empty} />;
  const keys = Array.from(new Set(items.flatMap((i) => Object.keys(i))));
  const cols = [...PREFERRED.filter((k) => keys.includes(k)), ...keys.filter((k) => !PREFERRED.includes(k))].slice(0, 7);
  return (
    <div className="table-wrap">
      <table className="table">
        <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>{items.map((it, i) => <tr key={i}>{cols.map((c) => <td key={c} className={c.includes("sha") || c === "digest" ? "mono" : undefined} title={typeof it[c] === "string" ? (it[c] as string) : undefined}>{c === "enabled" ? (it[c] === true ? <Pill tone="ok">启用</Pill> : it[c] === false ? <Pill tone="idle">停用</Pill> : "—") : cell(it[c])}</td>)}</tr>)}</tbody>
      </table>
    </div>
  );
}

/** Read-only in K5.2; sensitive operations (publish/rollback/GC, install/enable) arrive in K5.5. */
export default function ResourcesPage() {
  const [tab, setTab] = useState<Tab>("tables");
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
          <div className="page-sub">版本、哈希与启用状态（只读）。发布、回退、GC、安装与启停将在后续批次接入，并要求明确审批。</div>
        </div>
      </div>
      <Tabs<Tab> value={tab} onChange={setTab} tabs={[{ key: "tables", label: "参考表", badge: st.data ? <span className="chip">{st.data.tables.length}</span> : undefined }, { key: "plugins", label: "插件", badge: st.data ? <span className="chip">{pkgs.length}</span> : undefined }]} />
      {st.loading && !st.data ? <Skeleton rows={5} /> : st.error && !st.data ? <ErrorView error={st.error} onRetry={reload} /> : tab === "tables" ? (
        <Card pad={false}><Generic items={st.data!.tables} empty="没有参考表" /></Card>
      ) : (
        <div className="stack">
          {plugins?.__error ? <div className="banner tone-warn">{str(plugins.__error)}</div> : (
            <div className="row" style={{ flexWrap: "wrap", gap: 8 }}>
              {(["native_allowed", "script_allowed", "wasm_allowed", "external_allowed", "signature_required"] as const).map((k) => (
                <Pill key={k} tone={plugins?.[k] === true ? (k === "signature_required" ? "ok" : "info") : "idle"}>{k}: {plugins?.[k] === true ? "是" : "否"}</Pill>
              ))}
            </div>
          )}
          <Card pad={false}><Generic items={pkgs} empty="没有已安装的插件包" /></Card>
        </div>
      )}
    </>
  );
}
