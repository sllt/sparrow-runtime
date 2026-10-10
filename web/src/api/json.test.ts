import { describe, expect, it } from "vitest";
import { LosslessNumber, bigint, numText, parseJson, stringifyJson, obj } from "./json";

describe("lossless JSON", () => {
  it("keeps u64 revisions/offsets beyond 2^53 exactly", () => {
    const text = '{"revision":18446744073709551615,"offset":9007199254740993,"small":42}';
    const v = obj(parseJson(text))!;
    expect(v.revision).toBeInstanceOf(LosslessNumber);
    expect(numText(v.revision)).toBe("18446744073709551615");
    expect(numText(v.offset)).toBe("9007199254740993");
    expect(v.small).toBe(42);
    expect(stringifyJson(v)).toBe(text);
    expect(bigint(v.offset)! + 1n).toBe(9007199254740994n);
  });
  it("does not rewrite float literals on round-trip", () => {
    const text = '{"a":10.209999999999999,"b":1.5,"c":1e400,"d":-0.0}';
    expect(stringifyJson(parseJson(text))).toBe(text);
  });
  it("preserves array order and unknown fields", () => {
    const text = '{"out":["z","a","m"],"future_field":{"x":[3,1,2]}}';
    expect(stringifyJson(parseJson(text))).toBe(text);
  });
  it("native JSON.parse would have lost precision (control)", () => {
    expect(String(JSON.parse("9007199254740993"))).not.toBe("9007199254740993");
  });
});
