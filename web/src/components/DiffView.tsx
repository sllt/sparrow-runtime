import { useMemo } from "react";
import { hunks, lineDiff } from "../lib/diff";

export function DiffView({ before, after, beforeLabel, afterLabel }: { before: string; after: string; beforeLabel: string; afterLabel: string }) {
  const d = useMemo(() => lineDiff(before, after), [before, after]);
  if (!d) return <div className="banner tone-warn">配置过大，无法在浏览器中逐行比较；请下载两份配置离线比较。</div>;
  const add = d.filter((l) => l.kind === "add").length, del = d.filter((l) => l.kind === "del").length;
  return (
    <div className="diff" role="region" aria-label="配置差异">
      <div className="diff-head"><span className="mono">− {beforeLabel}</span><span className="mono">+ {afterLabel}</span><span className="spacer" /><span className="diff-add">+{add}</span><span className="diff-del">−{del}</span></div>
      <pre className="diff-body">
        {add + del === 0 ? <div className="diff-line same"><span className="ln" /><span className="ln" />  （无差异）</div> : hunks(d).map((l, i) => l.kind === "gap"
          ? <div key={i} className="diff-gap">… {l.count} 行未变更 …</div>
          : <div key={i} className={`diff-line ${l.kind}`}><span className="ln">{l.a ?? ""}</span><span className="ln">{l.b ?? ""}</span>{l.kind === "add" ? "+ " : l.kind === "del" ? "- " : "  "}{l.text}</div>)}
      </pre>
    </div>
  );
}
