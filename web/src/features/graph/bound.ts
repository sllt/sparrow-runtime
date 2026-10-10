import { obj, str, type Json } from "../../api/json";

export interface BoundNodeInfo { id: number; kind: string; schema: { name: string; type: string; nullable: boolean }[] }
export function parseBoundNodes(v: Json | undefined): Map<number, BoundNodeInfo> | null {
  if (!Array.isArray(v)) return null;
  const m = new Map<number, BoundNodeInfo>();
  for (const x of v) {
    const o = obj(x); const id = Number(String(o?.id));
    if (!o || !Number.isSafeInteger(id)) continue;
    const schema = (Array.isArray(o.output_schema) ? o.output_schema : []).map((f) => ({ name: str(obj(f)?.name) ?? "?", type: str(obj(f)?.type) ?? "?", nullable: obj(f)?.nullable === true }));
    m.set(id, { id, kind: str(o.bound_kind) ?? "", schema });
  }
  return m;
}
