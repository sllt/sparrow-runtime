import { useEffect, useRef, type ReactNode } from "react";
import { Icon } from "./Icon";

/** Accessible dialog: Esc closes, focus moves in and returns on close. */
export function Modal({ title, sub, onClose, children, footer, wide }: { title: string; sub?: ReactNode; onClose: () => void; children: ReactNode; footer?: ReactNode; wide?: boolean }) {
  const box = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const prev = document.activeElement as HTMLElement | null;
    const first = box.current?.querySelector<HTMLElement>("input,textarea,select,button:not([data-close])");
    (first ?? box.current)?.focus();
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") { e.stopPropagation(); onClose(); } };
    window.addEventListener("keydown", onKey, true);
    return () => { window.removeEventListener("keydown", onKey, true); prev?.focus?.(); };
  }, [onClose]);
  return (
    <div className="palette-back" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className={`modal ${wide ? "wide" : ""}`} role="dialog" aria-modal="true" aria-label={title} ref={box} tabIndex={-1}>
        <header className="modal-head">
          <div>
            <h2 className="card-title">{title}</h2>
            {sub && <div className="card-sub">{sub}</div>}
          </div>
          <button className="btn ghost icon" data-close onClick={onClose} aria-label="关闭"><Icon name="x" size={16} /></button>
        </header>
        <div className="modal-body">{children}</div>
        {footer && <footer className="modal-foot">{footer}</footer>}
      </div>
    </div>
  );
}
