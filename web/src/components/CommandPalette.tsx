import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { obj } from "../api/json";
import { Icon } from "./Icon";

interface Item { label: string; hint: string; to: string; icon: string }

export function CommandPalette({ onClose, nav }: { onClose: () => void; nav: { to: string; label: string; icon: string }[] }) {
  const { client } = useAuth();
  const go = useNavigate();
  const [q, setQ] = useState("");
  const [sel, setSel] = useState(0);
  const [pipes, setPipes] = useState<string[]>([]);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    input.current?.focus();
    const ac = new AbortController();
    client?.get("/v1/pipelines", ac.signal).then((v) => {
      const p = obj(v)?.pipelines;
      if (Array.isArray(p)) setPipes(p.filter((x): x is string => typeof x === "string").slice(0, 200));
    }).catch(() => {});
    return () => ac.abort();
  }, [client]);

  const items = useMemo<Item[]>(() => {
    const all: Item[] = [
      ...nav.map((n) => ({ label: n.label, hint: "页面", to: n.to, icon: n.icon })),
      ...pipes.map((p) => ({ label: p, hint: "流水线", to: `/pipelines/${encodeURIComponent(p)}`, icon: "pipelines" })),
    ];
    const s = q.trim().toLowerCase();
    return (s ? all.filter((i) => i.label.toLowerCase().includes(s)) : all).slice(0, 30);
  }, [q, pipes, nav]);

  function pick(i: Item | undefined) {
    if (!i) return;
    go(i.to);
    onClose();
  }

  return (
    <div className="palette-back" onMouseDown={onClose}>
      <div className="palette" role="dialog" aria-modal="true" aria-label="快速跳转" onMouseDown={(e) => e.stopPropagation()}>
        <input ref={input} className="input" placeholder="搜索页面或流水线…" value={q} aria-label="搜索"
          onChange={(e) => { setQ(e.target.value); setSel(0); }}
          onKeyDown={(e) => {
            if (e.key === "Escape") onClose();
            if (e.key === "ArrowDown") { e.preventDefault(); setSel((s) => Math.min(items.length - 1, s + 1)); }
            if (e.key === "ArrowUp") { e.preventDefault(); setSel((s) => Math.max(0, s - 1)); }
            if (e.key === "Enter") pick(items[sel]);
          }} />
        <ul role="listbox">
          {items.length === 0 && <li className="muted">没有匹配项</li>}
          {items.map((i, k) => (
            <li key={i.to} role="option" aria-selected={k === sel} onMouseEnter={() => setSel(k)} onClick={() => pick(i)}>
              <Icon name={i.icon} size={16} /> <span>{i.label}</span> <span className="spacer" /> <span className="small muted">{i.hint}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
