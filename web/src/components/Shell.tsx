import { useEffect, useState, type ReactNode } from "react";
import { NavLink, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { Icon } from "./Icon";
import { Kbd, Pill } from "./ui";
import { currentTheme, setTheme } from "./theme";
import { CommandPalette } from "./CommandPalette";

const NAV = [
  { to: "/", label: "总览", icon: "dashboard", key: "1" },
  { to: "/pipelines", label: "流水线", icon: "pipelines", key: "2" },
  { to: "/audit", label: "审计摘要", icon: "audit", key: "3" },
  { to: "/instance", label: "实例与权限", icon: "server", key: "4" },
];

const ROLE_LABEL = { viewer: "只读观察者", operator: "操作员", admin: "管理员" } as const;

function crumbs(path: string): { label: string; to?: string }[] {
  if (path === "/") return [{ label: "总览" }];
  if (path === "/pipelines") return [{ label: "流水线" }];
  if (path.startsWith("/pipelines/")) return [{ label: "流水线", to: "/pipelines" }, { label: decodeURIComponent(path.slice(11)) }];
  if (path === "/audit") return [{ label: "审计摘要" }];
  if (path === "/instance") return [{ label: "实例与权限" }];
  return [];
}

export function Shell({ children }: { children: ReactNode }) {
  const { me, logout } = useAuth();
  const nav = useNavigate();
  const loc = useLocation();
  const [theme, setT] = useState(currentTheme());
  const [collapsed, setCollapsed] = useState(false);
  const [palette, setPalette] = useState(false);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement;
      const typing = t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable;
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); setPalette(true); return; }
      if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === "/" ) { e.preventDefault(); setPalette(true); }
      const n = NAV.find((x) => x.key === e.key);
      if (n) nav(n.to);
      if (e.key === "t") toggleTheme();
      if (e.key === "[") setCollapsed((c) => !c);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    setT(next);
  }

  return (
    <div className={`app ${collapsed ? "collapsed" : ""}`}>
      <a href="#main" className="sr-only">跳到主要内容</a>
      <aside className="sidebar" aria-label="主导航">
        <div className="brand">
          <div className="brand-mark"><Icon name="activity" size={18} /></div>
          <div className="brand-text">
            <div className="brand-name">Sparrow</div>
            <div className="brand-sub">运维工作台</div>
          </div>
        </div>
        <div className="nav-section">运维</div>
        {NAV.map((n) => (
          <NavLink key={n.to} to={n.to} end={n.to === "/"} className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`} title={n.label}>
            <Icon name={n.icon} />
            <span className="nav-label">{n.label}</span>
            <Kbd>{n.key}</Kbd>
          </NavLink>
        ))}
        <div className="sidebar-foot">
          <div className="row" style={{ padding: "0 8px" }}>
            <div className="state-icon" style={{ width: 32, height: 32, margin: 0, borderRadius: 9 }}><Icon name="key" size={16} /></div>
            <div className="sidebar-foot-text" style={{ minWidth: 0 }}>
              <div className="row-name" style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{me?.actor}</div>
              <div className="row-meta">{me ? ROLE_LABEL[me.role] : ""}</div>
            </div>
          </div>
          <button className="nav-link btn ghost" style={{ justifyContent: "flex-start", height: 36 }} onClick={() => setCollapsed((c) => !c)} title="折叠侧栏 ([)">
            <Icon name="menu" /><span className="nav-label">折叠侧栏</span>
          </button>
          <button className="nav-link btn ghost" style={{ justifyContent: "flex-start", height: 36 }} onClick={() => logout()} title="注销">
            <Icon name="logout" /><span className="nav-label">注销</span>
          </button>
        </div>
      </aside>
      <div className="main">
        <header className="topbar">
          <nav className="crumbs" aria-label="面包屑">
            {crumbs(loc.pathname).map((c, i, a) => (
              <span key={i} className="row" style={{ gap: 8 }}>
                {c.to ? <NavLink to={c.to}>{c.label}</NavLink> : <span className={i === a.length - 1 ? "cur" : ""}>{c.label}</span>}
                {i < a.length - 1 && <Icon name="chevron" size={14} />}
              </span>
            ))}
          </nav>
          <div className="topbar-right">
            {me?.safeMode && <Pill tone="warn">安全模式</Pill>}
            {me?.draining && <Pill tone="warn">服务排空中</Pill>}
            {me?.role === "viewer" && <Pill tone="info" title="服务端只返回安全投影；写操作会被服务端拒绝">只读</Pill>}
            <button className="btn ghost" onClick={() => setPalette(true)} aria-label="快速跳转">
              <Icon name="search" size={16} /><span className="muted">快速跳转</span><Kbd>Ctrl K</Kbd>
            </button>
            <button className="btn ghost icon" onClick={toggleTheme} aria-label={theme === "dark" ? "切换到浅色" : "切换到深色"} title="切换主题 (T)">
              <Icon name={theme === "dark" ? "sun" : "moon"} />
            </button>
          </div>
        </header>
        <main id="main" className="content">{children}</main>
      </div>
      {palette && <CommandPalette onClose={() => setPalette(false)} nav={NAV} />}
    </div>
  );
}
