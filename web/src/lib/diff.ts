// Bounded line diff (LCS) for publish review. Large inputs degrade to a
// "too large" marker instead of an O(n*m) blow-up in the browser.
export type DiffLine = { kind: "same" | "add" | "del"; text: string; a?: number; b?: number };

export const MAX_DIFF_CELLS = 4_000_000;

export function lineDiff(before: string, after: string): DiffLine[] | null {
  const a = before === "" ? [] : before.split("\n");
  const b = after === "" ? [] : after.split("\n");
  // Trim common prefix/suffix first (typical edits are local).
  let p = 0;
  while (p < a.length && p < b.length && a[p] === b[p]) p++;
  let s = 0;
  while (s < a.length - p && s < b.length - p && a[a.length - 1 - s] === b[b.length - 1 - s]) s++;
  const am = a.slice(p, a.length - s), bm = b.slice(p, b.length - s);
  if (am.length * bm.length > MAX_DIFF_CELLS) return null;
  const n = am.length, m = bm.length;
  const dp: Uint32Array[] = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) dp[i]![j] = am[i] === bm[j] ? dp[i + 1]![j + 1]! + 1 : Math.max(dp[i + 1]![j]!, dp[i]![j + 1]!);
  const out: DiffLine[] = [];
  for (let i = 0; i < p; i++) out.push({ kind: "same", text: a[i]!, a: i + 1, b: i + 1 });
  let i = 0, j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && am[i] === bm[j]) { out.push({ kind: "same", text: am[i]!, a: p + i + 1, b: p + j + 1 }); i++; j++; }
    else if (j < m && (i >= n || dp[i]![j + 1]! >= dp[i + 1]![j]!)) { out.push({ kind: "add", text: bm[j]!, b: p + j + 1 }); j++; }
    else { out.push({ kind: "del", text: am[i]!, a: p + i + 1 }); i++; }
  }
  for (let k = 0; k < s; k++) out.push({ kind: "same", text: a[a.length - s + k]!, a: a.length - s + k + 1, b: b.length - s + k + 1 });
  return out;
}

/** Collapse long unchanged runs to keep the review readable. */
export function hunks(lines: DiffLine[], context = 3): (DiffLine | { kind: "gap"; count: number })[] {
  const keep = new Array(lines.length).fill(false);
  lines.forEach((l, i) => { if (l.kind !== "same") for (let k = Math.max(0, i - context); k <= Math.min(lines.length - 1, i + context); k++) keep[k] = true; });
  const out: (DiffLine | { kind: "gap"; count: number })[] = [];
  let gap = 0;
  lines.forEach((l, i) => {
    if (keep[i]) { if (gap) { out.push({ kind: "gap", count: gap }); gap = 0; } out.push(l); } else gap++;
  });
  if (gap) out.push({ kind: "gap", count: gap });
  return out;
}
