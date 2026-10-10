import { obj, str, type Json } from "../../api/json";

/** Older capability entries carry no `roles`; their direction is fixed by contract. */
const LEGACY_ROLES: Record<string, string[]> = { mqtt: ["source"], http: ["sink"], http_push: ["source"], mqtt_sink: ["sink"], file: ["source", "sink"], jetstream: ["source"] };

/** Connector kinds by role, only those enabled in this build. */
export function connectorKinds(caps: Json): { source: string[]; sink: string[]; disabled: string[] } {
  const out = { source: new Set<string>(), sink: new Set<string>(["log"]), disabled: [] as string[] };
  const list = obj(caps)?.connectors;
  if (Array.isArray(list)) for (const c of list) {
    const o = obj(c);
    const kind = str(o?.kind);
    if (!o || !kind) continue;
    if (o.enabled_by_build === false) { out.disabled.push(kind); continue; }
    const roles = Array.isArray(o.roles) ? o.roles : LEGACY_ROLES[kind] ?? (kind.endsWith("_sink") ? ["sink"] : ["source"]);
    for (const r of roles) {
      if (r === "source") out.source.add(kind);
      if (r === "sink") out.sink.add(kind.replace(/_sink$/, ""));
    }
  }
  return { source: [...out.source].sort(), sink: [...out.sink].sort(), disabled: out.disabled };
}
