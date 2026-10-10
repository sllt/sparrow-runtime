// Lossless helpers over PipelineSpec text. Edits parse with lossless-json
// (numbers keep their literal text, key order and array order preserved) and
// re-serialise; unknown fields are carried through untouched.
import { stringify } from "lossless-json";
import { parseJson, obj, str, type Json } from "../api/json";

export type ParseResult = { ok: true; value: Record<string, Json> } | { ok: false; error: string };

export function parseSpec(text: string): ParseResult {
  try {
    const v = obj(parseJson(text));
    if (!v) return { ok: false, error: "顶层必须是 JSON 对象" };
    return { ok: true, value: v };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

export function pretty(v: unknown): string {
  const out = stringify(v, undefined, 2);
  if (out === undefined) throw new Error("not serialisable");
  return out;
}
const indent = pretty;

/** Replace one top-level field, preserving everything else. */
export function setField(text: string, key: string, value: Json | undefined): string | null {
  const p = parseSpec(text);
  if (!p.ok) return null;
  const next: Record<string, Json> = {};
  let placed = false;
  for (const [k, v] of Object.entries(p.value)) {
    if (k === key) { if (value !== undefined) next[k] = value; placed = true; } else next[k] = v;
  }
  if (!placed && value !== undefined) next[key] = value;
  return indent(next);
}

export function getSql(text: string): string | null {
  const p = parseSpec(text);
  return p.ok ? str(p.value.sql) : null;
}

export function summarizeIo(v: Record<string, Json> | null): { source: string; sink: string } {
  const k = (x: Json | undefined) => str(obj(x)?.kind) ?? "—";
  return { source: k(v?.source), sink: k(v?.sink) };
}

export const emptySpec = (stream: string) => indent({
  version: 1,
  stream,
  sql: `SELECT * FROM ${stream}`,
  source: { kind: "mqtt", topic: "sensors/json", client_id: "sparrow" },
  sink: { kind: "log" },
  delivery: "live_best_effort",
  recovery: "restart_fresh",
});
