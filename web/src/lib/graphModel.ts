// Pure, lossless edits over a GraphSpec value. The canvas never owns the
// truth: every gesture becomes one of these functions on the `graph` JSON,
// so unknown node fields, number literals and key order survive untouched.
// Coordinates live in draft metadata (`layout`), never in the spec.
import { LosslessNumber } from "lossless-json";
import { numText, obj, str, type Json } from "../api/json";

export type Node = Record<string, Json>;
export type Graph = Record<string, Json>;
export type Pos = { x: number; y: number };
export type Layout = Record<string, Pos>;

export interface EdgeInfo {
  id: string; from: number; to: number; index: number;
  /** Human labels derived only from explicit spec fields. */
  labels: string[];
  bestEffort: boolean; side: string | null;
}

export const SOURCE_KINDS = new Set(["memory_source"]);
export const SINK_KINDS = new Set(["capture_sink", "best_effort_sink"]);
export const ROUTER_KINDS = new Set(["branch", "route", "switch"]);
export const MERGE_KINDS = new Set(["union_all", "interval_join", "window_join"]);
export const MAX_NODES = 256;

const n = (v: Json | undefined): number | null => {
  const t = numText(v);
  if (t === null || !/^\d+$/.test(t)) return null;
  const x = Number(t);
  return Number.isSafeInteger(x) ? x : null;
};
const num = (x: number): Json => new LosslessNumber(String(x));

export function nodes(g: Graph | null): Node[] {
  return Array.isArray(g?.nodes) ? (g!.nodes as Json[]).map((x) => obj(x)).filter((x): x is Node => x !== null) : [];
}
export const nodeId = (x: Node) => n(x.id);
export const outs = (x: Node): number[] => (Array.isArray(x.out) ? (x.out as Json[]).map(n).filter((v): v is number => v !== null) : []);

export function edges(g: Graph | null): EdgeInfo[] {
  const res: EdgeInfo[] = [];
  const joins = new Map<number, Record<string, Json>>();
  for (const x of nodes(g)) { const j = obj(x.stream_join); const id = nodeId(x); if (j && id !== null) joins.set(id, j); }
  for (const src of nodes(g)) {
    const from = nodeId(src);
    if (from === null) continue;
    const routes = Array.isArray(src.routes) ? (src.routes as Json[]).map(obj) : [];
    const best = new Set(Array.isArray(src.best_effort) ? (src.best_effort as Json[]).map(n) : []);
    const side = obj(src.side_output);
    const sideTo = n(side?.to);
    const def = n(src.default_out);
    outs(src).forEach((to, index) => {
      const labels: string[] = [];
      routes.forEach((r, i) => { if (n(r?.to) === to) labels.push(`case ${i + 1}`); });
      if (def === to) labels.push("default");
      if (sideTo === to) labels.push(`side: ${str(side?.kind) ?? "?"}`);
      if (best.has(to)) labels.push("best-effort");
      const j = joins.get(to);
      if (j && n(j.left_input) === from) labels.push("left");
      if (j && n(j.right_input) === from) labels.push("right");
      if (outs(src).length > 1 && labels.length === 0) labels.push(`out ${index + 1}`);
      res.push({ id: `${from}->${to}#${index}`, from, to, index, labels, bestEffort: best.has(to), side: sideTo === to ? str(side?.kind) : null });
    });
  }
  return res;
}

/** Inbound edges per node, in spec order (nodes array order, then out order). */
export function inputsOf(g: Graph | null, id: number): number[] {
  return edges(g).filter((e) => e.to === id).map((e) => e.from);
}

function withNodes(g: Graph, list: Node[]): Graph {
  const next: Graph = {};
  let placed = false;
  for (const [k, v] of Object.entries(g)) { if (k === "nodes") { next.nodes = list; placed = true; } else next[k] = v; }
  if (!placed) next.nodes = list;
  return next;
}
function setKey(o: Node, key: string, v: Json | undefined): Node {
  const next: Node = {};
  let placed = false;
  for (const [k, x] of Object.entries(o)) { if (k === key) { placed = true; if (v !== undefined) next[k] = v; } else next[k] = x; }
  if (!placed && v !== undefined) next[key] = v;
  return next;
}
export const setNodeField = setKey;

export function hasCycle(g: Graph): boolean {
  const adj = new Map<number, number[]>();
  for (const x of nodes(g)) { const id = nodeId(x); if (id !== null) adj.set(id, outs(x)); }
  const state = new Map<number, 1 | 2>();
  const visit = (v: number): boolean => {
    const s = state.get(v);
    if (s === 1) return true;
    if (s === 2) return false;
    state.set(v, 1);
    for (const w of adj.get(v) ?? []) if (visit(w)) return true;
    state.set(v, 2);
    return false;
  };
  for (const v of adj.keys()) if (visit(v)) return true;
  return false;
}

export type EditResult = { ok: true; graph: Graph } | { ok: false; reason: string };

export function connect(g: Graph, from: number, to: number): EditResult {
  if (from === to) return { ok: false, reason: "不能连接到自身" };
  const list = nodes(g);
  const src = list.find((x) => nodeId(x) === from), dst = list.find((x) => nodeId(x) === to);
  if (!src || !dst) return { ok: false, reason: "节点不存在" };
  if (SINK_KINDS.has(str(src.kind) ?? "")) return { ok: false, reason: "Sink 节点没有输出端口" };
  if (SOURCE_KINDS.has(str(dst.kind) ?? "")) return { ok: false, reason: "Source 节点没有输入端口" };
  if (outs(src).includes(to)) return { ok: false, reason: "这两个节点已经相连" };
  const outArr = Array.isArray(src.out) ? [...(src.out as Json[])] : [];
  outArr.push(num(to));
  // Wiring into a Join fills an unset explicit left/right slot; it never
  // silently reassigns one that is already bound.
  let joined = dst;
  const j = obj(dst.stream_join);
  if (j) {
    const ids = new Set(list.map(nodeId));
    const free = (k: string) => { const v = n(j[k]); return v === null || v === 0 || !ids.has(v); };
    const slot = free("left_input") ? "left_input" : free("right_input") ? "right_input" : null;
    if (slot) joined = setKey(dst, "stream_join", setKey(j, slot, num(from)));
  }
  const next = withNodes(g, list.map((x) => (x === src ? setKey(x, "out", outArr) : x === dst ? joined : x)));
  if (hasCycle(next)) return { ok: false, reason: "该连线会形成环；Graph 必须是有向无环图" };
  return { ok: true, graph: next };
}

/** Remove one edge and every explicit reference to that destination on the source. */
function dropRefs(x: Node, to: number): Node {
  let y = setKey(x, "out", (Array.isArray(x.out) ? (x.out as Json[]) : []).filter((v) => n(v) !== to));
  if (Array.isArray(y.routes)) { const r = (y.routes as Json[]).filter((v) => n(obj(v)?.to) !== to); y = setKey(y, "routes", r.length ? r : undefined); }
  if (Array.isArray(y.best_effort)) { const b = (y.best_effort as Json[]).filter((v) => n(v) !== to); y = setKey(y, "best_effort", b.length ? b : undefined); }
  if (n(y.default_out) === to) y = setKey(y, "default_out", undefined);
  if (n(obj(y.side_output)?.to) === to) y = setKey(y, "side_output", undefined);
  return y;
}

export function disconnect(g: Graph, from: number, to: number): Graph {
  return withNodes(g, nodes(g).map((x) => (nodeId(x) === from ? dropRefs(x, to) : x)));
}

export function removeNode(g: Graph, id: number): Graph {
  return withNodes(g, nodes(g).filter((x) => nodeId(x) !== id).map((x) => (outs(x).includes(id) ? dropRefs(x, id) : x)));
}

export function nextId(g: Graph): number {
  return nodes(g).reduce((m, x) => Math.max(m, nodeId(x) ?? 0), 0) + 1;
}

export function addNode(g: Graph, template: Node): { graph: Graph; id: number } | null {
  if (nodes(g).length >= MAX_NODES) return null;
  const id = nextId(g);
  const node: Node = { id: num(id) };
  for (const [k, v] of Object.entries(template)) if (k !== "id") node[k] = v;
  if (!SINK_KINDS.has(str(node.kind) ?? "") && node.out === undefined) node.out = [];
  return { graph: withNodes(g, [...nodes(g), node]), id };
}

/** Copy keeps parameters but never connections (they would silently fan out). */
export function duplicateNode(g: Graph, id: number): { graph: Graph; id: number } | null {
  const src = nodes(g).find((x) => nodeId(x) === id);
  if (!src) return null;
  let copy: Node = { ...src };
  for (const k of ["out", "routes", "best_effort", "default_out", "side_output"]) copy = setKey(copy, k, undefined);
  if (!SINK_KINDS.has(str(src.kind) ?? "")) copy.out = [];
  return addNode(g, copy);
}

export function replaceNode(g: Graph, id: number, node: Node): Graph {
  return withNodes(g, nodes(g).map((x) => (nodeId(x) === id ? node : x)));
}

/** Layered layout by longest path from sources; stable within a layer by spec order. */
export function autoLayout(g: Graph): Layout {
  const list = nodes(g);
  const depth = new Map<number, number>();
  const ids = list.map(nodeId).filter((v): v is number => v !== null);
  for (const id of ids) depth.set(id, 0);
  for (let pass = 0; pass < ids.length; pass++) {
    let changed = false;
    for (const x of list) {
      const id = nodeId(x); if (id === null) continue;
      for (const o of outs(x)) if (depth.has(o) && depth.get(o)! < depth.get(id)! + 1) { depth.set(o, depth.get(id)! + 1); changed = true; }
    }
    if (!changed) break;
  }
  const rows = new Map<number, number>();
  const out: Layout = {};
  for (const id of ids) {
    const d = depth.get(id)!;
    const r = rows.get(d) ?? 0;
    rows.set(d, r + 1);
    out[String(id)] = { x: d * 260, y: r * 130 };
  }
  return out;
}

export function readLayout(meta: Record<string, Json> | null): Layout {
  const l = obj(meta?.layout);
  const res: Layout = {};
  for (const [k, v] of Object.entries(l ?? {})) {
    const o = obj(v); const x = Number(numText(o?.x)), y = Number(numText(o?.y));
    if (Number.isFinite(x) && Number.isFinite(y)) res[k] = { x, y };
  }
  return res;
}
export function layoutJson(l: Layout): Json {
  const o: Record<string, Json> = {};
  for (const [k, p] of Object.entries(l)) o[k] = { x: Math.round(p.x), y: Math.round(p.y) };
  return o;
}

/** Local structural hints (the server's validate is the authority). */
export function localIssues(g: Graph): { node: number | null; text: string }[] {
  const res: { node: number | null; text: string }[] = [];
  const list = nodes(g);
  const ids = new Set<number>();
  for (const x of list) {
    const id = nodeId(x);
    if (id === null) { res.push({ node: null, text: "存在缺少数字 id 的节点" }); continue; }
    if (ids.has(id)) res.push({ node: id, text: `节点 id ${id} 重复` });
    ids.add(id);
  }
  for (const x of list) {
    const id = nodeId(x); if (id === null) continue;
    const kind = str(x.kind) ?? "";
    const ins = inputsOf(g, id).length, o = outs(x).length;
    for (const t of outs(x)) if (!ids.has(t)) res.push({ node: id, text: `输出指向不存在的节点 ${t}` });
    if (SOURCE_KINDS.has(kind)) { if (ins) res.push({ node: id, text: "Source 不能有输入" }); if (!o) res.push({ node: id, text: "Source 尚未连接下游" }); }
    else if (SINK_KINDS.has(kind)) { if (ins !== 1) res.push({ node: id, text: `Sink 需要恰好 1 个输入（当前 ${ins}）` }); }
    else if (kind === "union_all") { if (ins < 2 || ins > 16) res.push({ node: id, text: `UnionAll 需要 2–16 个输入（当前 ${ins}）` }); }
    else if (kind === "interval_join" || kind === "window_join") {
      if (ins !== 2) res.push({ node: id, text: `Join 需要恰好 2 个输入（当前 ${ins}）` });
      const j = obj(x.stream_join), from = inputsOf(g, id);
      for (const k of ["left_input", "right_input"]) if (!from.includes(n(j?.[k]) ?? -1)) res.push({ node: id, text: `stream_join.${k} 未指向一个已连接的输入` });
    }
    else { if (ins !== 1) res.push({ node: id, text: `需要恰好 1 个输入（当前 ${ins}）` }); if (!o) res.push({ node: id, text: "尚未连接下游" }); }
  }
  if (hasCycle(g)) res.push({ node: null, text: "图中存在环" });
  return res;
}

/** Server error messages say `node {id}: …`; map that to the canvas. */
export function errorNode(message: string | null | undefined): number | null {
  const m = /node (\d+)/.exec(message ?? "");
  return m ? Number(m[1]) : null;
}

export interface PaletteItem { kind: string; label: string; group: string; hint: string; template: Node }
const col = (name: string): Json => ({ k: "col", name });
export const PALETTE: PaletteItem[] = [
  { group: "输入输出", kind: "memory_source", label: "Source", hint: "从 Stream 读取（连接参数见 Source / Sink）", template: { kind: "memory_source", table: "sensors", out: [] } },
  { group: "输入输出", kind: "capture_sink", label: "Sink", hint: "输出到 Sink 连接", template: { kind: "capture_sink", name: "output" } },
  { group: "无状态", kind: "filter", label: "Filter", hint: "按谓词过滤行", template: { kind: "filter", predicate: { k: "bin", op: ">", left: col("v"), right: { k: "lit", value: { t: "int64", v: new LosslessNumber("0") } } }, out: [] } },
  { group: "无状态", kind: "project", label: "Project", hint: "选择/计算列", template: { kind: "project", exprs: [{ expr: col("device_id"), alias: "device_id" }], out: [] } },
  { group: "有状态", kind: "window_agg", label: "Window Agg", hint: "窗口聚合", template: { kind: "window_agg", keys: ["device_id"], window: { kind: "processing_time", size_micros: new LosslessNumber("1000000") }, aggs: [{ fn: "count", expr: col("v"), alias: "n" }], out: [] } },
  { group: "有状态", kind: "dedup", label: "Dedup", hint: "按键去重", template: { kind: "dedup", keys: ["device_id"], out: [] } },
  { group: "有状态", kind: "lookup", label: "Lookup", hint: "关联参考表", template: { kind: "lookup", table: "", on: [], keep: [], out: [] } },
  { group: "有状态", kind: "interval_join", label: "Interval Join", hint: "两路直接 Source 的受限 Join（left/right 显式）", template: { kind: "interval_join", stream_join: { left_input: num(0), right_input: num(0), left_keys: ["device_id"], right_keys: ["device_id"], left_time: "ts", right_time: "ts", mode: "inner", before_micros: new LosslessNumber("1000000"), after_micros: new LosslessNumber("1000000") }, out: [] } },
  { group: "无状态", kind: "unnest", label: "Unnest", hint: "展开数组列（有行/字节上限）", template: { kind: "unnest", unnest: { expr: col("items"), as_field: "item", max_rows: num(64), max_bytes: new LosslessNumber("65536") }, out: [] } },
  { group: "拓扑", kind: "branch", label: "Branch", hint: "复制到多个下游", template: { kind: "branch", out: [] } },
  { group: "拓扑", kind: "route", label: "Route", hint: "按谓词分流（case 顺序显式）", template: { kind: "route", route_mode: "first_match", routes: [], out: [] } },
  { group: "拓扑", kind: "union_all", label: "UnionAll", hint: "合并 2–16 路", template: { kind: "union_all", out: [] } },
];

export function emptyGraph(stream: string): Graph {
  return {
    version: num(1), pipeline_id: num(1), revision_id: num(1),
    nodes: [
      { id: num(1), kind: "memory_source", table: stream, out: [num(2)] },
      { id: num(2), kind: "capture_sink", name: "output" },
    ],
  };
}

/** Deleting a node must not leave a hidden `graph_io` binding behind. */
export function pruneGraphIo(io: Json | undefined, g: Graph): Json | undefined {
  const o = obj(io);
  if (!o) return io;
  const ids = new Set(nodes(g).map(nodeId).filter((v) => v !== null).map(String));
  const out: Record<string, Json> = {};
  for (const [k, v] of Object.entries(o)) {
    const m = (k === "sources" || k === "sinks") ? obj(v) : null;
    if (!m) { out[k] = v; continue; }
    const kept: Record<string, Json> = {};
    for (const [id, b] of Object.entries(m)) if (ids.has(id)) kept[id] = b;
    out[k] = kept;
  }
  return out;
}
