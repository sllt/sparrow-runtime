import { usePoll } from "../../api/usePoll";
import { useAuth } from "../../auth/AuthContext";
import { obj, str, numText } from "../../api/json";
import { Card, ErrorView, Pill, Skeleton } from "../../components/ui";

const GROUPS: [string, string][] = [["auth.", "鉴权"], ["capabilities", "能力"], ["metrics", "指标"], ["pipeline", "流水线"], ["plan", "计划"], ["graph", "图"], ["query", "查询"], ["stream", "Stream"], ["table", "参考表"], ["plugin", "插件"], ["outbox", "Outbox"], ["input_dlq", "输入 DLQ"], ["recovery", "恢复"], ["secret", "秘密"], ["allowlist", "Allowlist"], ["audit", "审计"], ["demo", "演示"]];

export default function InstancePage() {
  const { me } = useAuth();
  const [caps] = usePoll((c, s) => c.get("/v1/capabilities", s), "caps", 60_000);
  const c = obj(caps.data);
  return (
    <>
      <div className="page-head"><div><h1 className="page-title">实例与权限</h1><div className="page-sub">当前身份的服务端授权范围与构建能力。按钮隐藏不是安全边界，所有动作都由服务端判定。</div></div></div>
      <div className="grid two">
        <Card title="我的权限" sub={`${me?.actor} · ${me?.role} · ${me?.authMode}`}>
          <div className="stack">
            {GROUPS.map(([p, label]) => {
              const acts = me?.allowed.filter((a) => a.startsWith(p)) ?? [];
              if (!acts.length) return null;
              return <div key={p}><div className="small muted" style={{ marginBottom: 6 }}>{label}</div><div className="row" style={{ flexWrap: "wrap", gap: 6 }}>{acts.map((a) => <span key={a} className="chip">{a}</span>)}</div></div>;
            })}
          </div>
        </Card>
        <Card title="构建能力" sub="静态实现清单，用于筛选候选控件；不代表某个配置一定可运行">
          {caps.error && !c ? <ErrorView error={caps.error} /> : !c ? <Skeleton /> : (
            <dl className="kv">
              {Object.entries(c).slice(0, 24).map(([k, val]) => (
                <div key={k} style={{ display: "contents" }}>
                  <dt className="mono">{k}</dt>
                  <dd>{typeof val === "boolean" ? <Pill tone={val ? "ok" : "idle"}>{val ? "是" : "否"}</Pill> : str(val) ?? numText(val) ?? (Array.isArray(val) ? `${val.length} 项` : "对象")}</dd>
                </div>
              ))}
            </dl>
          )}
        </Card>
      </div>
    </>
  );
}
