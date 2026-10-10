import { describe, expect, it } from "vitest";
import { parseJson, stringifyJson } from "../api/json";
import { pruneGraphIo, addNode, autoLayout, connect, disconnect, duplicateNode, edges, errorNode, localIssues, removeNode, type Graph } from "./graphModel";

const g0 = () => parseJson(`{"version":1,"pipeline_id":9007199254740993,"revision_id":1,"x_unknown":{"a":1.50},"nodes":[
 {"id":1,"kind":"memory_source","table":"s","out":[2]},
 {"id":2,"kind":"route","route_mode":"first_match","routes":[{"predicate":{"k":"col","name":"hot"},"to":3}],"default_out":4,"best_effort":[4],"out":[3,4]},
 {"id":3,"kind":"capture_sink","name":"a"},
 {"id":4,"kind":"capture_sink","name":"b","future_field":true}]}`) as Graph;

describe("graphModel", () => {
  it("derives edge semantics only from explicit fields", () => {
    const e = edges(g0());
    expect(e.map((x) => [x.from, x.to, x.labels.join("|")])).toEqual([[1, 2, ""], [2, 3, "case 1"], [2, 4, "default|best-effort"]]);
  });
  it("keeps unknown fields, big ints and number literals through edits", () => {
    const r = addNode(g0(), { kind: "filter", predicate: { k: "col", name: "ok" } });
    expect(r!.id).toBe(5);
    const s = stringifyJson(r!.graph);
    expect(s).toContain("9007199254740993");
    expect(s).toContain('"a":1.50');
    expect(s).toContain('"future_field":true');
    expect(s.indexOf('"version"')).toBeLessThan(s.indexOf('"nodes"'));
  });
  it("disconnect drops route/default/best_effort references to that target", () => {
    const g = disconnect(g0(), 2, 4);
    const s = stringifyJson(g);
    expect(s).not.toContain("default_out");
    expect(s).not.toContain("best_effort");
    expect(s).toContain('"routes":[{');
    const g2 = removeNode(g0(), 3);
    expect(stringifyJson(g2)).not.toContain('"routes"');
  });
  it("rejects cycles, self loops and sink outputs", () => {
    let g = addNode(g0(), { kind: "filter" })!.graph;
    g = (connect(g, 1, 5) as { ok: true; graph: Graph }).graph;
    expect(connect(g, 5, 5).ok).toBe(false);
    expect(connect(g, 3, 5).ok).toBe(false);
    const g2 = parseJson(`{"nodes":[{"id":1,"kind":"filter","out":[2]},{"id":2,"kind":"filter","out":[]}]}`) as Graph;
    const r = connect(g2, 2, 1);
    expect(r.ok).toBe(false);
  });
  it("duplicate copies params but not connections", () => {
    const r = duplicateNode(g0(), 2)!;
    const s = stringifyJson(r.graph);
    expect(s).toContain('{"id":5,"kind":"route","route_mode":"first_match","out":[]}');
  });
  it("local issues and layout", () => {
    expect(localIssues(g0())).toEqual([]);
    const g = removeNode(g0(), 2);
    expect(localIssues(g).map((x) => x.text)).toContain("Source 尚未连接下游");
    const l = autoLayout(g0());
    expect(l["4"]!.x).toBe(520);
    expect(errorNode("node 12: invalid input/output arity")).toBe(12);
  });
  it("join slots are explicit: wiring fills free left/right, labels follow stream_join", () => {
    let g = parseJson(`{"nodes":[{"id":1,"kind":"memory_source","out":[]},{"id":2,"kind":"memory_source","out":[]},{"id":3,"kind":"interval_join","stream_join":{"left_input":0,"right_input":0},"out":[]}]}`) as Graph;
    g = (connect(g, 2, 3) as { ok: true; graph: Graph }).graph;
    g = (connect(g, 1, 3) as { ok: true; graph: Graph }).graph;
    expect(stringifyJson(g)).toContain('"stream_join":{"left_input":2,"right_input":1}');
    expect(edges(g).map((e) => e.labels.join())).toEqual(["right", "left"]);
  });
  it("removing a node prunes its graph_io binding", () => {
    const io = parseJson(`{"sources":{"1":{"kind":"mqtt"}},"sinks":{"3":{"kind":"log"},"4":{"kind":"http"}},"idle_after_ms":5}`);
    expect(stringifyJson(pruneGraphIo(io, removeNode(g0(), 4)))).toBe('{"sources":{"1":{"kind":"mqtt"}},"sinks":{"3":{"kind":"log"}},"idle_after_ms":5}');
  });
});
