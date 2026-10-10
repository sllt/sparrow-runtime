import { describe, expect, it } from "vitest";
import { rate, RateSeries } from "./rates";
import { p99, quantileText } from "./histogram";
import { deriveView, checkpointRisk } from "./health";
import { parseJson } from "../api/json";
import { fmtInt } from "./format";

describe("rates", () => {
  it("resets on attempt change and counter regression", () => {
    expect(rate(null, { attempt: "1", at: 0, value: 10n }).kind).toBe("warming");
    expect(rate({ attempt: "1", at: 0, value: 10n }, { attempt: "1", at: 2000, value: 30n })).toEqual({ kind: "rate", perSec: 10 });
    expect(rate({ attempt: "1", at: 0, value: 10n }, { attempt: "2", at: 2000, value: 30n }).kind).toBe("reset");
    expect(rate({ attempt: "1", at: 0, value: 10n }, { attempt: "1", at: 2000, value: 3n }).kind).toBe("reset");
    expect(rate({ attempt: "1", at: 0, value: 10n }, { attempt: null, at: 2000, value: 3n }).kind).toBe("unknown");
  });
  it("series is cleared when attempt changes", () => {
    const s = new RateSeries();
    s.push("k", { attempt: "1", at: 0, value: 0n });
    s.push("k", { attempt: "1", at: 1000, value: 5n });
    expect(s.get("k")).toEqual([5]);
    s.push("k", { attempt: "2", at: 2000, value: 1n });
    expect(s.get("k")).toEqual([]);
  });
  it("differences exact counters beyond 2^53", () => {
    const r = rate({ attempt: "a", at: 0, value: 9007199254740993n }, { attempt: "a", at: 1000, value: 9007199254740995n });
    expect(r).toEqual({ kind: "rate", perSec: 2 });
  });
});

describe("p99 is a bucket upper bound", () => {
  it("insufficient samples are not presented as a latency", () => {
    expect(quantileText(p99({ samples: 12, p99_upper_us: 1024 }))).toBe("样本不足（12/100）");
    expect(quantileText(p99({ samples: 500, p99_upper_us: 2048 }))).toBe("≤ 2 ms");
    expect(quantileText(p99({ samples: 500, p99_upper_us: null }))).toContain("无上界");
    expect(quantileText(p99(undefined))).toBe("无观测");
  });
});

describe("health derivation never shows unobserved data as healthy", () => {
  const running = parseJson('{"revision":3,"actual":{"status":"running","attempt_id":7,"revision":3},"observation":{"available":true,"running_revision":3,"runtime_progress":{"ingested_rows":1,"emitted_rows":1},"diagnosis_reasons":[]}}');
  it("fresh running with observation is running", () => expect(deriveView("p", running, true).health).toBe("running"));
  it("stale data is stale even if last body said running", () => expect(deriveView("p", running, false).health).toBe("stale"));
  it("no actual is unknown", () => expect(deriveView("p", parseJson('{"revision":1}'), true).health).toBe("unknown"));
  it("running without observation is unavailable", () =>
    expect(deriveView("p", parseJson('{"actual":{"status":"running"},"observation":{"available":false,"reason":"no_active_attempt"}}'), true).health).toBe("unavailable"));
  it("restart_blocked wins", () => expect(deriveView("p", parseJson('{"actual":{"status":"failed","restart_blocked":true}}'), true).health).toBe("blocked"));
  it("checkpoint n/a is not ok", () => expect(checkpointRisk(null).level).toBe("na"));
  it("formats big ints exactly", () => expect(fmtInt(parseJson("18446744073709551615"))).toBe("18,446,744,073,709,551,615"));
});

describe("diagnosis reasons", () => {
  it("benign/backpressure reasons do not degrade; failures do", () => {
    const body = (r: string) => parseJson(`{"actual":{"status":"running"},"observation":{"available":true,"diagnosis_reasons":["${r}"]}}`);
    expect(deriveView("p", body("no_current_pressure_evidence"), true).health).toBe("running");
    expect(deriveView("p", body("sink_queue_nonempty"), true).health).toBe("running");
    expect(deriveView("p", body("sink_retry_or_failure"), true).health).toBe("degraded");
  });
});
