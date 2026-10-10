import { numText, obj, str, type Json } from "../../api/json";

export interface DraftSummary {
  id: string; etag: string; pipeline: string; mode: string; baseEtag: string | null;
  updatedBy: string; updatedAt: number | null; textBytes: number;
}
export interface Draft extends DraftSummary { text: string; metadata: Json }

export function parseDraft(v: Json): Draft {
  const o = obj(v) ?? {};
  return {
    ...parseSummary(v),
    text: str(o.text) ?? "",
    metadata: o.metadata ?? {},
  };
}
export function parseSummary(v: Json): DraftSummary {
  const o = obj(v) ?? {};
  return {
    id: str(o.id) ?? "", etag: str(o.etag) ?? "", pipeline: str(o.pipeline) ?? "", mode: str(o.mode) ?? "sql",
    baseEtag: str(o.base_etag), updatedBy: str(o.updated_by) ?? "", updatedAt: Number(numText(o.updated_at_ms) ?? NaN) || null,
    textBytes: Number(numText(o.text_bytes) ?? 0),
  };
}
export const MODE_LABEL: Record<string, string> = { sql: "SQL", graph: "Graph", json: "JSON" };
