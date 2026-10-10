import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { createClient, type ApiClient, ApiError } from "../api/client";
import { obj, str, type Json } from "../api/json";

export interface Me {
  actor: string;
  role: "viewer" | "operator" | "admin";
  allowed: string[];
  authMode: string;
  safeMode: boolean;
  draining: boolean;
  /** Server contract differs from this build: forced read-only. */
  contractMismatch: string | null;
}

/** Must equal sparrow-server `ui::UI_CONTRACT`. */
export const UI_CONTRACT = 1;
const READ_ONLY = /(\.read|\.list|\.diagnose|^status\.|^audit\.)/;

interface AuthValue {
  client: ApiClient | null;
  me: Me | null;
  notice: string | null;
  login: (token: string) => Promise<void>;
  logout: (notice?: string) => void;
  expire: () => void;
  can: (action: string) => boolean;
}

const Ctx = createContext<AuthValue | null>(null);

export function parseMe(v: Json): Me {
  const o = obj(v);
  const role = str(o?.role);
  if (!o || !str(o.actor) || (role !== "viewer" && role !== "operator" && role !== "admin")) {
    throw new ApiError("bad_response", null, null, "auth/me 响应格式不符合合同", false);
  }
  const allowed = Array.isArray(o.allowed_actions) ? o.allowed_actions.filter((a): a is string => typeof a === "string") : [];
  const server = typeof o.ui_contract === "number" ? o.ui_contract : null;
  const mismatch = server === UI_CONTRACT ? null : `界面合同版本 ${UI_CONTRACT} 与服务端 ${server ?? "未声明"} 不一致：已切换为只读，请部署与服务端同一构建的界面。`;
  return {
    actor: str(o.actor)!,
    role: mismatch ? "viewer" : role,
    contractMismatch: mismatch,
    allowed: mismatch ? allowed.filter((a) => READ_ONLY.test(a)) : allowed,
    authMode: str(o.auth_mode) ?? "unknown",
    safeMode: o.safe_mode === true,
    draining: o.draining === true,
  };
}

/**
 * The token exists only inside the ApiClient closure held in React state.
 * Logout/expiry drops the client; every page-level poller is unmounted with it,
 * which aborts in-flight reads. Nothing is persisted.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<{ client: ApiClient; me: Me } | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const login = useCallback(async (token: string) => {
    const client = createClient(token.trim());
    const me = parseMe(await client.get("/v1/auth/me"));
    setNotice(null);
    setSession({ client, me });
  }, []);
  const logout = useCallback((n?: string) => {
    setSession(null);
    setNotice(n ?? null);
  }, []);
  const expire = useCallback(() => logout("登录已失效（服务端返回 401），请重新输入令牌。"), [logout]);

  const value = useMemo<AuthValue>(() => ({
    client: session?.client ?? null,
    me: session?.me ?? null,
    notice,
    login,
    logout,
    expire,
    can: (a) => session?.me.allowed.includes(a) ?? false,
  }), [session, notice, login, logout, expire]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth(): AuthValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("AuthProvider missing");
  return v;
}

export function useClient(): ApiClient {
  const c = useAuth().client;
  if (!c) throw new Error("not authenticated");
  return c;
}
