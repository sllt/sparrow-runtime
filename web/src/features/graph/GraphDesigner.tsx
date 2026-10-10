import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import {
  ReactFlow, Background, Controls, MiniMap, Handle, Position, MarkerType,
  type Node as RFNode, type Edge as RFEdge, type NodeProps, type NodeChange, type Connection, type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { parseJson, obj, str, type Json } from "../../api/json";
import { Icon } from "../../components/Icon";
import { Kbd, Pill } from "../../components/ui";
import { CodeEditor } from "../../components/CodeEditor";
import { pretty } from "../../lib/specText";
import {
  PALETTE, SINK_KINDS, SOURCE_KINDS, ROUTER_KINDS, addNode, autoLayout, connect, disconnect, duplicateNode, edges as specEdges,
  inputsOf, localIssues, nodeId, nodes as specNodes, outs, removeNode, replaceNode, setNodeField, type Graph, type Layout, type Node,
} from "../../lib/graphModel";

import type { BoundNodeInfo } from "./bound";

type NodeData = { node: Node; issues: string[]; bound: BoundNodeInfo | null; serverError: boolean; boundStale: boolean };
const KIND_ICON: Record<string, string> = { memory_source: "stream", capture_sink: "upload", best_effort_sink: "upload", filter: "filter", project: "code", map: "code", lookup: "database", branch: "branch", route: "branch", switch: "branch", union_all: "branch", plugin_transform: "puzzle" };

function SpecNode({ data, selected }: NodeProps<RFNode<NodeData>>) {
  const kind = str(data.node.kind) ?? "?";
  const id = nodeId(data.node);
  const sub = str(data.node.table) ?? str(data.node.name) ?? (Array.isArray(data.node.keys) ? `keys: ${(data.node.keys as Json[]).map(String).join(", ")}` : null);
  const tone = data.serverError ? "bad" : data.issues.length ? "warn" : data.bound && !data.boundStale ? "ok" : "idle";
  return (
    <div className={`gnode tone-${tone}${selected ? " sel" : ""}`} title={data.issues.join("\n") || undefined}>
      {!SOURCE_KINDS.has(kind) && <Handle type="target" position={Position.Left} />}
      <div className="gnode-head">
        <Icon name={KIND_ICON[kind] ?? (kind.includes("window") || kind.includes("tumble") || kind.includes("hop") ? "clock" : "cpu")} size={15} />
        <span className="gnode-kind">{kind}</span>
        <span className="gnode-id mono">#{id}</span>
      </div>
      {sub && <div className="gnode-sub mono">{sub}</div>}
      <div className="gnode-foot">
        {data.bound ? <span className={data.boundStale ? "muted" : undefined}>{data.bound.schema.length} 列{data.boundStale ? "（未刷新）" : ""}</span> : <span className="muted">schema 待检查</span>}
        {data.issues.length > 0 && <span className="gnode-warn"><Icon name="alert" size={12} />{data.issues.length}</span>}
      </div>
      {!SINK_KINDS.has(kind) && <Handle type="source" position={Position.Right} />}
    </div>
  );
}
const nodeTypes = { spec: SpecNode };

type Snap = { graph: Graph; layout: Layout };
type Sel = { node: number } | { edge: string } | null;

export interface GraphDesignerProps {
  graph: Graph; layout: Layout; readOnly: boolean;
  onChange: (graph: Graph, layout: Layout) => void;
  bound: Map<number, BoundNodeInfo> | null; boundStale: boolean;
  serverErrorNode: number | null; serverError: string | null;
}

export default function GraphDesigner({ graph, layout, readOnly, onChange, bound, boundStale, serverErrorNode, serverError }: GraphDesignerProps) {
  const [sel, setSel] = useState<Sel>(null);
  const [toast, setToast] = useState<string | null>(null);
  const undo = useRef<Snap[]>([]), redo = useRef<Snap[]>([]);
  const [, bump] = useState(0);
  const [drag, setDrag] = useState<Layout | null>(null); // live positions while dragging
  const canvas = useRef<HTMLDivElement | null>(null);
  const focusCanvas = () => canvas.current?.focus({ preventScroll: true });
  const rf = useRef<ReactFlowInstance<RFNode<NodeData>, RFEdge> | null>(null);

  const issues = useMemo(() => localIssues(graph), [graph]);
  const list = useMemo(() => specNodes(graph), [graph]);
  const edgeList = useMemo(() => specEdges(graph), [graph]);
  // Nodes without a stored position get a deterministic layered spot.
  const auto = useMemo(() => autoLayout(graph), [graph]);
  const pos = (id: number) => drag?.[id] ?? layout[String(id)] ?? auto[String(id)] ?? { x: 0, y: 0 };

  const commit = useCallback((g: Graph, l: Layout) => {
    undo.current.push({ graph, layout }); if (undo.current.length > 100) undo.current.shift();
    redo.current = []; bump((x) => x + 1);
    onChange(g, l);
  }, [graph, layout, onChange]);
  const travel = (from: typeof undo, to: typeof redo) => {
    const s = from.current.pop(); if (!s) return;
    to.current.push({ graph, layout }); bump((x) => x + 1); onChange(s.graph, s.layout);
  };
  const flash = (t: string) => { setToast(t); window.setTimeout(() => setToast((x) => (x === t ? null : x)), 3500); };

  const rfNodes: RFNode<NodeData>[] = list.map((x) => {
    const id = nodeId(x) ?? -1;
    return {
      id: String(id), type: "spec", position: pos(id), selected: sel !== null && "node" in sel && sel.node === id,
      draggable: !readOnly, connectable: !readOnly, deletable: !readOnly,
      data: { node: x, issues: issues.filter((i) => i.node === id).map((i) => i.text), bound: bound?.get(id) ?? null, serverError: serverErrorNode === id, boundStale },
    };
  });
  const rfEdges: RFEdge[] = edgeList.map((e) => ({
    id: e.id, source: String(e.from), target: String(e.to), label: e.labels.join(" · ") || undefined,
    selected: sel !== null && "edge" in sel && sel.edge === e.id, deletable: !readOnly,
    className: e.bestEffort ? "gedge best" : e.side ? "gedge side" : "gedge",
    type: "smoothstep", markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
  }));

  const onNodesChange = (changes: NodeChange<RFNode<NodeData>>[]) => {
    for (const c of changes) {
      if (c.type === "position" && c.position) {
        const p = c.position;
        if (c.dragging) setDrag((d) => ({ ...(d ?? {}), [c.id]: p }));
        else if (!readOnly) { const next = { ...layout, ...(drag ?? {}), [c.id]: p }; setDrag(null); commit(graph, next); }
      }
      if (c.type === "select" && c.selected) setSel({ node: Number(c.id) });
    }
  };
  const onConnect = (c: Connection) => {
    if (readOnly) return;
    const r = connect(graph, Number(c.source), Number(c.target));
    if (r.ok) commit(r.graph, layout); else flash(r.reason);
  };
  const delSel = () => {
    if (readOnly || !sel) return;
    if ("node" in sel) { const l = { ...layout }; delete l[String(sel.node)]; commit(removeNode(graph, sel.node), l); }
    else { const e = edgeList.find((x) => x.id === sel.edge); if (e) commit(disconnect(graph, e.from, e.to), layout); }
    setSel(null);
  };
  const dup = () => {
    if (readOnly || !sel || !("node" in sel)) return;
    const r = duplicateNode(graph, sel.node); if (!r) return flash("已达节点上限");
    const p = pos(sel.node);
    commit(r.graph, { ...layout, [String(r.id)]: { x: p.x + 40, y: p.y + 90 } }); setSel({ node: r.id });
  };
  const add = (k: string) => {
    const item = PALETTE.find((p) => p.kind === k); if (!item || readOnly) return;
    const r = addNode(graph, item.template); if (!r) return flash("已达节点上限（256）");
    const vp = rf.current?.getViewport();
    const x = vp ? (-vp.x + 120) / vp.zoom : 0;
    let y = vp ? (-vp.y + 60) / vp.zoom : 0;
    // Never drop a node on top of another one.
    const taken = list.map((m) => pos(nodeId(m) ?? -1));
    for (let i = 0; i < 40 && taken.some((p) => Math.abs(p.x - x) < 220 && Math.abs(p.y - y) < 100); i++) y += 110;
    commit(r.graph, { ...layout, [String(r.id)]: { x, y } }); setSel({ node: r.id }); focusCanvas();
  };
  const tidy = () => { if (!readOnly) { commit(graph, autoLayout(graph)); window.setTimeout(() => rf.current?.fitView({ padding: 0.2, maxZoom: 1 }), 30); } };

  const onKey = (e: KeyboardEvent | globalThis.KeyboardEvent) => {
    const t = e.target as HTMLElement;
    if (t.closest(".cm-editor, input, textarea, select, [role=dialog]")) return;
    // Window-level so a click that leaves focus on <body> still works;
    // anything focused outside the designer keeps its own keys.
    if (t !== document.body && !t.closest(".gd")) return;
    const mod = e.ctrlKey || e.metaKey;
    if (mod && e.key.toLowerCase() === "z") { e.preventDefault(); if (e.shiftKey) travel(redo, undo); else travel(undo, redo); }
    else if (mod && e.key.toLowerCase() === "y") { e.preventDefault(); travel(redo, undo); }
    else if (mod && e.key.toLowerCase() === "d") { e.preventDefault(); dup(); }
    else if (e.key === "Delete" || e.key === "Backspace") { e.preventDefault(); delSel(); }
    else if (e.key === "Escape") setSel(null);
  };

  const keyRef = useRef(onKey); keyRef.current = onKey;
  useEffect(() => {
    const h = (e: globalThis.KeyboardEvent) => keyRef.current(e);
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, []);
  useEffect(() => { if (serverErrorNode !== null) setSel({ node: serverErrorNode }); }, [serverErrorNode]);

  const selNode = sel && "node" in sel ? list.find((x) => nodeId(x) === sel.node) ?? null : null;
  const selEdge = sel && "edge" in sel ? edgeList.find((x) => x.id === sel.edge) ?? null : null;
  const groups = [...new Set(PALETTE.map((p) => p.group))];

  return (
    <div className="gd">
      <aside className="gd-palette" aria-label="节点面板">
        {groups.map((g) => (
          <div key={g} className="gd-group">
            <div className="nav-group">{g}</div>
            {PALETTE.filter((p) => p.group === g).map((p) => (
              <button key={p.kind} className="gd-item" disabled={readOnly} onClick={() => add(p.kind)} title={p.hint} aria-label={`添加 ${p.label} 节点`}>
                <Icon name={KIND_ICON[p.kind] ?? "clock"} size={15} /><span>{p.label}</span><Icon name="plus" size={13} className="muted" />
              </button>
            ))}
          </div>
        ))}
      </aside>
      <div className="gd-canvas" ref={canvas} tabIndex={0} aria-label="Graph 画布">
        <div className="gd-toolbar">
          <button className="btn ghost sm" onClick={() => travel(undo, redo)} disabled={readOnly || !undo.current.length} title="撤销 (Ctrl Z)">撤销</button>
          <button className="btn ghost sm" onClick={() => travel(redo, undo)} disabled={readOnly || !redo.current.length} title="重做 (Ctrl Shift Z)">重做</button>
          <span className="gd-sep" />
          <button className="btn ghost sm" onClick={tidy} disabled={readOnly}>自动布局</button>
          <button className="btn ghost sm" onClick={() => rf.current?.fitView({ padding: 0.2, maxZoom: 1 })}>适应画布</button>
          <span className="spacer" />
          {issues.length ? <Pill tone="warn">{issues.length} 个结构提示</Pill> : <Pill tone="ok">结构完整</Pill>}
        </div>
        <ReactFlow<RFNode<NodeData>, RFEdge>
          nodes={rfNodes} edges={rfEdges} nodeTypes={nodeTypes}
          onInit={(i) => { rf.current = i; }} fitView fitViewOptions={{ padding: 0.2, maxZoom: 1 }}
          onNodesChange={onNodesChange} onConnect={onConnect}
          onEdgeClick={(_, e) => { setSel({ edge: e.id }); focusCanvas(); }} onPaneClick={() => setSel(null)} onNodeClick={(_, nd) => { setSel({ node: Number(nd.id) }); focusCanvas(); }}
          deleteKeyCode={null} nodesDraggable={!readOnly} nodesConnectable={!readOnly} minZoom={0.2} maxZoom={2}
          proOptions={{ hideAttribution: true }}>
          <Background gap={20} size={1} />
          <Controls showInteractive={false} />
          {list.length >= 8 && <MiniMap pannable zoomable className="gd-minimap" position="top-right" />}
        </ReactFlow>
        {toast && <div className="gd-toast" role="status">{toast}</div>}
        {list.length === 0 && <div className="gd-empty">从左侧添加节点开始，或在“完整配置 JSON”中粘贴 GraphSpec。</div>}
      </div>
      <aside className="gd-inspector" aria-label="属性">
        {selNode ? <NodeInspector key={String(nodeId(selNode))} graph={graph} node={selNode} readOnly={readOnly} bound={bound?.get(nodeId(selNode)!) ?? null} boundStale={boundStale}
          serverError={serverErrorNode === nodeId(selNode) ? serverError : null}
          issues={issues.filter((i) => i.node === nodeId(selNode)).map((i) => i.text)}
          onApply={(nn) => commit(replaceNode(graph, nodeId(selNode)!, nn), layout)} onDelete={delSel} onDuplicate={dup} />
          : selEdge ? <EdgeInspector graph={graph} edge={selEdge} readOnly={readOnly} onDelete={delSel}
            onToggleBest={() => {
              const src = list.find((x) => nodeId(x) === selEdge.from)!;
              const cur = Array.isArray(src.best_effort) ? (src.best_effort as Json[]) : [];
              const next = selEdge.bestEffort ? cur.filter((v) => String(v) !== String(selEdge.to)) : [...cur, parseJson(String(selEdge.to))];
              commit(replaceNode(graph, selEdge.from, setNodeField(src, "best_effort", next.length ? next : undefined)), layout);
            }} />
          : <div className="gd-help">
            <h3 className="card-title">Graph 设计器</h3>
            <p className="muted small">拖动右侧端口到另一节点的左侧端口来连线。画布只是 GraphSpec 的视图：所有编辑都直接写回配置 JSON，坐标单独存在草稿元数据中。</p>
            {issues.length > 0 && <ul className="gd-issues">{issues.map((i, k) => <li key={k}>{i.node !== null && <button className="link" onClick={() => setSel({ node: i.node! })}>#{i.node}</button>} {i.text}</li>)}</ul>}
            <div className="small muted stack" style={{ gap: 4, marginTop: 12 }}>
              <div><Kbd>Del</Kbd> 删除所选　<Kbd>Ctrl D</Kbd> 复制节点</div>
              <div><Kbd>Ctrl Z</Kbd> 撤销　<Kbd>Ctrl Shift Z</Kbd> 重做</div>
              <div>端口类型与输出列来自服务端“检查”，不在浏览器猜测。</div>
            </div>
          </div>}
      </aside>
    </div>
  );
}

function NodeInspector({ graph, node, readOnly, bound, boundStale, serverError, issues, onApply, onDelete, onDuplicate }: {
  graph: Graph; node: Node; readOnly: boolean; bound: BoundNodeInfo | null; boundStale: boolean; serverError: string | null; issues: string[];
  onApply: (n: Node) => void; onDelete: () => void; onDuplicate: () => void;
}) {
  const id = nodeId(node)!;
  const body = useMemo(() => { const o: Node = {}; for (const [k, v] of Object.entries(node)) if (k !== "id" && k !== "out") o[k] = v; return o; }, [node]);
  const [text, setText] = useState(() => pretty(body));
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => { setText(pretty(body)); setErr(null); }, [body]);
  const original = pretty(body);
  const apply = () => {
    try {
      const o = obj(parseJson(text)); if (!o) throw new Error("必须是 JSON 对象");
      if (!str(o.kind)) throw new Error("缺少 kind");
      const next: Node = { id: node.id ?? null };
      for (const [k, v] of Object.entries(o)) if (k !== "id" && k !== "out") next[k] = v;
      if (node.out !== undefined) next.out = node.out;
      setErr(null); onApply(next);
    } catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
  };
  const ins = inputsOf(graph, id), os = outs(node);
  const kind = str(node.kind) ?? "";
  return (
    <div className="stack" style={{ gap: 12 }}>
      <div className="row"><h3 className="card-title">{kind}</h3><span className="mono muted">#{id}</span><span className="spacer" />
        <button className="btn ghost icon sm" onClick={onDuplicate} disabled={readOnly} title="复制节点 (Ctrl D)" aria-label="复制节点"><Icon name="file" size={15} /></button>
        <button className="btn ghost icon sm" onClick={onDelete} disabled={readOnly} title="删除节点 (Del)" aria-label="删除节点"><Icon name="trash" size={15} /></button>
      </div>
      {serverError && <div className="err-box small"><strong>服务端检查：</strong>{serverError}</div>}
      {issues.map((t) => <div key={t} className="hint tone-warn">⚠ {t}</div>)}
      <div className="kv small">
        <span className="muted">输入</span><span className="mono">{ins.length ? ins.map((x, i) => `${(kind.endsWith("_join") ? ["#1 ", "#2 "][i] ?? "" : "")}${x}`).join(", ") : "—"}</span>
        <span className="muted">输出</span><span className="mono">{os.length ? os.join(", ") : "—"}{ROUTER_KINDS.has(kind) && os.length > 1 ? "（顺序即 out 数组顺序）" : ""}</span>
      </div>
      <div>
        <div className="row small" style={{ marginBottom: 6 }}><strong>输出 schema</strong><span className="spacer" />{bound && boundStale && <Pill tone="muted">未刷新</Pill>}</div>
        {bound ? (bound.schema.length ? (
          <table className="table compact"><tbody>{bound.schema.map((f) => <tr key={f.name}><td className="mono">{f.name}</td><td><span className="chip" title={f.nullable ? "可为空" : "非空"}>{f.type}{f.nullable ? "?" : ""}</span></td></tr>)}</tbody></table>
        ) : <div className="muted small">无输出列（Sink）</div>) : <div className="muted small">运行“检查”后显示服务端绑定的列与类型。</div>}
      </div>
      <div>
        <div className="row small" style={{ marginBottom: 6 }}><strong>参数</strong><span className="muted">（id 与连线由画布管理）</span></div>
        <CodeEditor language="json" label={`节点 ${id} 参数`} value={text} onChange={setText} readOnly={readOnly} minHeight={180} onSave={apply} />
        {err && <div className="hint tone-bad" role="alert">{err}</div>}
        <div className="row" style={{ marginTop: 8 }}>
          <button className="btn sm primary" onClick={apply} disabled={readOnly || text === original}>应用参数</button>
          <button className="btn sm ghost" onClick={() => { setText(original); setErr(null); }} disabled={text === original}>还原</button>
        </div>
      </div>
    </div>
  );
}

function EdgeInspector({ graph, edge, readOnly, onDelete, onToggleBest }: { graph: Graph; edge: ReturnType<typeof specEdges>[number]; readOnly: boolean; onDelete: () => void; onToggleBest: () => void }) {
  const src = specNodes(graph).find((x) => nodeId(x) === edge.from);
  const kind = str(src?.kind) ?? "";
  return (
    <div className="stack" style={{ gap: 12 }}>
      <div className="row"><h3 className="card-title">连线</h3><span className="mono muted">{edge.from} → {edge.to}</span><span className="spacer" />
        <button className="btn ghost icon sm" onClick={onDelete} disabled={readOnly} aria-label="删除连线" title="删除连线 (Del)"><Icon name="trash" size={15} /></button></div>
      <div className="kv small">
        <span className="muted">位置</span><span>out[{edge.index}]</span>
        <span className="muted">语义</span><span>{edge.labels.length ? edge.labels.join(" · ") : "普通数据流"}</span>
      </div>
      {ROUTER_KINDS.has(kind) && (
        <label className="check"><input type="checkbox" checked={edge.bestEffort} disabled={readOnly} onChange={onToggleBest} /> best-effort（下游慢时允许丢弃，显式有损）</label>
      )}
      <p className="muted small">删除连线会同时移除该源节点上指向此目标的 route case、default_out、side_output 与 best_effort 引用。</p>
    </div>
  );
}
