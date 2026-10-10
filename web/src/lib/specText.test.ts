import { describe, expect, it } from "vitest";
import { getSql, parseSpec, setField } from "./specText";
import { hunks, lineDiff } from "./diff";
import { connectorKinds } from "../features/drafts/caps";
import { parseJson } from "../api/json";

describe("specText lossless edits", () => {
  const text = '{"version":1,"stream":"s","sql":"SELECT 1","source":{"kind":"file","offset":18446744073709551615,"ratio":1.0,"x":1e2},"out":["b","a"],"zz_unknown":{"k":[3,2,1]}}';
  it("setField preserves big ints, float literals, key and array order, unknown fields", () => {
    const next = setField(text, "sql", "SELECT 2")!;
    expect(next).toContain("18446744073709551615");
    expect(next).toContain("1.0");
    expect(next).toContain("1e2");
    expect(getSql(next)).toBe("SELECT 2");
    const keys = Object.keys((parseSpec(next) as { value: object }).value);
    expect(keys).toEqual(["version", "stream", "sql", "source", "out", "zz_unknown"]);
    expect(next.indexOf('"b"')).toBeLessThan(next.indexOf('"a"'));
    expect(next).toMatch(/"k": \[\s*3,\s*2,\s*1\s*\]/);
  });
  it("setField returns null for unparsable text and never guesses", () => {
    expect(setField("{ nope", "sql", "x")).toBeNull();
    expect(parseSpec("[1]").ok).toBe(false);
  });
  it("removing a field keeps the rest", () => {
    const next = setField(text, "zz_unknown", undefined)!;
    expect(next).not.toContain("zz_unknown");
    expect(next).toContain("18446744073709551615");
  });
});

describe("diff", () => {
  it("marks additions/removals and collapses unchanged runs", () => {
    const a = Array.from({ length: 30 }, (_, i) => `l${i}`).join("\n");
    const b = a.replace("l15", "L15");
    const d = lineDiff(a, b)!;
    expect(d.filter((x) => x.kind === "add").map((x) => x.text)).toEqual(["L15"]);
    expect(d.filter((x) => x.kind === "del").map((x) => x.text)).toEqual(["l15"]);
    const h = hunks(d);
    expect(h.some((x) => x.kind === "gap")).toBe(true);
  });
  it("refuses quadratic blow-up", () => {
    const a = Array.from({ length: 3000 }, (_, i) => `a${i}`).join("\n");
    const b = Array.from({ length: 3000 }, (_, i) => `b${i}`).join("\n");
    expect(lineDiff(a, b)).toBeNull();
  });
});

describe("connector kinds", () => {
  it("uses roles, strips _sink and drops build-disabled kinds", () => {
    const caps = parseJson('{"connectors":[{"kind":"mqtt","roles":["source"]},{"kind":"mqtt_sink"},{"kind":"nats","enabled_by_build":false},{"kind":"plugin","roles":["source","sink"]}]}');
    const k = connectorKinds(caps);
    expect(k.source).toEqual(["mqtt", "plugin"]);
    expect(k.sink).toEqual(["log", "mqtt", "plugin"]);
    expect(k.disabled).toEqual(["nats"]);
  });
});
