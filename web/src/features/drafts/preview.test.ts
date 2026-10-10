import { describe, expect, it } from "vitest";
import { stringifyJson } from "../../api/json";
import { fmtMicros, timelineIssue, toWire } from "./PreviewPanel";

describe("preview timeline", () => {
  it("keeps integer literals exact and validates rows", () => {
    const w = toWire([{ type: "data", row: '{"id":"a","big":9007199254740993,"f":1.50}' }, { type: "advance_clock", to: "9007199254740993" }, { type: "eof" }]);
    expect("events" in w && stringifyJson(w.events)).toBe('[{"type":"data","row":{"id":"a","big":9007199254740993,"f":1.50}},{"type":"advance_clock","to_micros":9007199254740993},{"type":"eof"}]');
    expect(toWire([{ type: "data", row: "[1]" }])).toEqual({ error: "数据必须是 JSON 对象", index: 0 });
    expect(toWire([{ type: "watermark", micros: "1.5" }])).toMatchObject({ index: 0 });
  });
  it("flags backwards time and events after EOF locally", () => {
    expect(timelineIssue("10", [{ type: "advance_clock", to: "5" }])).toContain("倒退");
    expect(timelineIssue("0", [{ type: "watermark", micros: "5" }, { type: "watermark", micros: "4" }])).toContain("水位线");
    expect(timelineIssue("0", [{ type: "eof" }, { type: "data", row: "{}" }])).toContain("EOF");
    expect(timelineIssue("0", [{ type: "advance_clock", to: "1000000" }, { type: "advance_clock", to: "1000000" }])).toBeNull();
    expect(fmtMicros("1500000")).toBe("1.5 s");
    expect(fmtMicros("-1")).toBe("未设置");
  });
});
