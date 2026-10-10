// Lossless JSON layer. Every API response is read as text and parsed here so
// u64/i64 revisions, offsets and microsecond times never pass through a JS
// number. Integers outside the safe range stay as `LosslessNumber`; floats
// keep their literal text, so re-serialising does not rewrite them.
import { isInteger, LosslessNumber, parse, stringify } from "lossless-json";

export { LosslessNumber };

export type Json = null | boolean | string | number | LosslessNumber | Json[] | { [k: string]: Json };

function reviver(value: string): number | LosslessNumber {
  if (isInteger(value)) {
    const n = Number(value);
    return Number.isSafeInteger(n) ? n : new LosslessNumber(value);
  }
  // Non-integers: keep exact literal text unless it round-trips through a double.
  const f = Number(value);
  return String(f) === value ? f : new LosslessNumber(value);
}

export function parseJson(text: string): Json {
  return parse(text, null, reviver) as Json;
}

export function stringifyJson(value: unknown): string {
  const out = stringify(value);
  if (out === undefined) throw new Error("value is not JSON-serialisable");
  return out;
}

/** Display/ID helper: exact decimal text of any JSON number. */
export function numText(v: unknown): string | null {
  if (typeof v === "number") return Number.isFinite(v) ? String(v) : null;
  if (v instanceof LosslessNumber) return v.value;
  return null;
}

/** For charts/ratios only (controlled approximation); never for IDs/config. */
export function approx(v: unknown): number | null {
  if (typeof v === "number") return v;
  if (v instanceof LosslessNumber) return Number(v.value);
  return null;
}

/** Exact integer as BigInt for diffing counters. */
export function bigint(v: unknown): bigint | null {
  const t = numText(v);
  if (t === null || !/^-?\d+$/.test(t)) return null;
  return BigInt(t);
}

export function obj(v: unknown): Record<string, Json> | null {
  return v && typeof v === "object" && !Array.isArray(v) && !(v instanceof LosslessNumber)
    ? (v as Record<string, Json>)
    : null;
}

export function str(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}
