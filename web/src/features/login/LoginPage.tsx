import { useState, type FormEvent } from "react";
import { useAuth } from "../../auth/AuthContext";
import { ApiError } from "../../api/client";
import { Icon } from "../../components/Icon";

export function LoginPage() {
  const { login, notice } = useAuth();
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (!token.trim()) return;
    setBusy(true);
    setError(null);
    try {
      await login(token);
      setToken("");
    } catch (err) {
      const k = err instanceof ApiError ? err.kind : "server";
      setError(
        k === "unauthorized" ? "令牌无效：服务端拒绝了此令牌（401）。"
        : k === "offline" ? "无法连接 Sparrow 服务，请检查服务是否运行或网络连接。"
        : k === "rate_limited" ? "服务繁忙（429），请稍后再试。"
        : err instanceof Error ? err.message : "登录失败",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login-wrap">
      <div className="login-art">
        <div className="row"><div className="brand-mark"><Icon name="activity" size={18} /></div><strong style={{ fontSize: 18 }}>Sparrow</strong></div>
        <div>
          <h1>看清每一条流水线的真实运行状态</h1>
          <p>流量、背压、错误与恢复风险，一屏掌握。数据过期、无观测和未知状态都会明确标出，不会被显示成“健康”。</p>
          <div className="feat"><Icon name="shield" size={16} />服务端按角色授权，只读角色只拿到脱敏投影</div>
          <div className="feat"><Icon name="lock" size={16} />令牌只保存在本页内存，刷新或注销即清除</div>
          <div className="feat"><Icon name="layers" size={16} />离线静态资源，不访问任何外部服务</div>
        </div>
        <div style={{ opacity: .6, fontSize: 12 }}>Sparrow 单实例运维工作台 · K5</div>
      </div>
      <div className="login-panel">
        <form className="login-card stack" onSubmit={submit} autoComplete="off">
          <div>
            <h2 style={{ fontSize: 24 }}>登录</h2>
            <p className="muted" style={{ marginTop: 4 }}>输入服务端为你配置的访问令牌（Bearer token）。</p>
          </div>
          {notice && <div className="banner tone-warn" role="alert"><Icon name="alert" />{notice}</div>}
          {error && <div className="banner tone-bad" role="alert"><Icon name="alert" />{error}</div>}
          <div>
            <label className="field-label" htmlFor="token">访问令牌</label>
            <input id="token" className="input" type="password" value={token} onChange={(e) => setToken(e.target.value)}
              autoFocus spellCheck={false} autoComplete="off" placeholder="粘贴令牌" />
            <div className="hint" style={{ marginTop: 6 }}>不会写入 URL、浏览器存储或日志；刷新页面需要重新输入。</div>
          </div>
          <button className="btn primary lg" type="submit" disabled={busy || !token.trim()}>
            {busy ? "验证中…" : "登录"}
          </button>
        </form>
      </div>
    </div>
  );
}
