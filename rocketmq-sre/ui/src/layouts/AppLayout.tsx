import {
  Activity,
  BookOpenCheck,
  Bot,
  Boxes,
  CalendarCheck2,
  ChartNoAxesCombined,
  CircleHelp,
  ClipboardList,
  Clock3,
  DatabaseZap,
  GitBranch,
  Gauge,
  ListChecks,
  LogOut,
  MessageSquareText,
  Network,
  PackageSearch,
  RadioTower,
  SearchCode,
  ShieldCheck,
  Siren,
  Workflow,
  TrendingUp,
  UserRound,
} from "lucide-react";
import { useEffect, useState } from "react";
import {
  NavLink,
  Outlet,
  useLocation,
  useNavigate,
} from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/badge";
import { useSreData } from "@/data/SreDataContext";
import {
  parseReadOnlyUrlContext,
  withoutReadOnlyUrlContext,
} from "@/hooks/useClusterScope";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const groups = [
  {
    label: "态势",
    items: [
      { to: "/", label: "总览", icon: Gauge, end: true },
      { to: "/clusters", label: "集群接入", icon: RadioTower },
      { to: "/assets", label: "资产视图", icon: PackageSearch },
      { to: "/topology", label: "拓扑关系", icon: GitBranch },
    ],
  },
  {
    label: "诊断",
    items: [
      { to: "/ask", label: "Ask SRE", icon: MessageSquareText },
      { to: "/incidents", label: "事件诊断", icon: Siren },
      { to: "/inspections", label: "巡检建议", icon: ListChecks },
      { to: "/forecasts", label: "容量预测", icon: TrendingUp },
      { to: "/operations", label: "值班运营", icon: CalendarCheck2 },
      {
        to: "/action-items",
        label: "复盘改进",
        icon: ClipboardList,
        end: false,
      },
    ],
  },
  {
    label: "变更",
    items: [
      { to: "/changes", label: "变更中心", icon: Workflow, end: false },
    ],
  },
  {
    label: "证据",
    items: [
      { to: "/evidence", label: "证据浏览器", icon: DatabaseZap },
      { to: "/journeys", label: "消息旅程", icon: SearchCode },
      { to: "/coverage", label: "诊断覆盖", icon: Boxes },
      { to: "/knowledge", label: "知识库", icon: BookOpenCheck },
    ],
  },
  {
    label: "平台",
    items: [
      { to: "/models", label: "模型能力", icon: Bot },
      {
        to: "/autonomy",
        label: "自治运营",
        icon: ChartNoAxesCombined,
      },
      { to: "/system", label: "系统状态", icon: Activity },
    ],
  },
];

export function AppLayout() {
  const auth = useAuth();
  const { clusters, loading } = useSreData();
  const location = useLocation();
  const navigate = useNavigate();
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 30_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (loading) {
      return;
    }
    const result = parseReadOnlyUrlContext(
      location.search,
      clusters.map((cluster) => cluster.id),
    );
    if (result.status === "invalid") {
      navigate(
        {
          pathname: location.pathname,
          search: withoutReadOnlyUrlContext(location.search),
        },
        { replace: true },
      );
      return;
    }
    if (
      result.status === "valid" &&
      location.pathname !== "/assets" &&
      location.pathname !== "/ask"
    ) {
      navigate(
        {
          pathname:
            result.context.resourceKind === "cluster"
              ? "/ask"
              : "/assets",
          search: location.search,
        },
        { replace: true },
      );
    }
  }, [clusters, loading, location.pathname, location.search, navigate]);

  return (
    <TooltipProvider delayDuration={250}>
      <div className="app-shell">
        <aside className="sidebar">
          <div className="brand">
            <span className="brand-mark" aria-hidden="true">
              <Network size={21} />
            </span>
            <span>
              <strong>RocketMQ</strong>
              <small>Rust AI SRE</small>
            </span>
          </div>

          <nav aria-label="主导航" className="sidebar-navigation">
            {groups.map((group) => (
              <section className="nav-group" key={group.label}>
                <h2>{group.label}</h2>
                {group.items.map(({ to, label, icon: Icon, end }) => (
                  <NavLink
                    className={({ isActive }) =>
                      `nav-item${isActive ? " active" : ""}`
                    }
                    end={end}
                    key={to}
                    to={to}
                  >
                    <Icon aria-hidden="true" size={17} />
                    <span>{label}</span>
                  </NavLink>
                ))}
              </section>
            ))}
          </nav>

          <div className="sidebar-spacer" />
          <div className="boundary-note">
            <ShieldCheck aria-hidden="true" size={16} />
            <div>
              <strong>分层变更安全边界</strong>
              <span>诊断默认只读；变更仅限审批、围栏和类型化 Agent。</span>
            </div>
          </div>
          <div className="sidebar-meta">
            <span>v0.4.0 · Phase 04 in progress</span>
            <span>Bounded operations only</span>
          </div>
        </aside>

        <div className="workspace">
          <header className="utility-bar">
            <div className="utility-product">
              <Badge variant="outline">SUPERVISED</Badge>
              <span>独立 AI SRE 运维面</span>
            </div>
            <div className="utility-actions">
              <span className="utility-identity">
                <UserRound aria-hidden="true" size={14} />
                <span>
                  <strong>{auth.session?.displayName ?? "未登录"}</strong>
                  <small>
                    {auth.mode === "development" ? "DEV" : "OIDC"} ·{" "}
                    {auth.session?.clusterIds.length ?? 0} clusters
                  </small>
                </span>
              </span>
              <span>
                <Clock3 aria-hidden="true" size={14} />
                {now.toLocaleString("zh-CN", {
                  hour12: false,
                  timeZone: "Asia/Shanghai",
                })}
              </span>
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    aria-label="关于受控变更边界"
                    className="icon-button"
                    type="button"
                  >
                    <CircleHelp size={16} />
                  </button>
                </TooltipTrigger>
                <TooltipContent>
                  诊断保持只读；仅审批后的 R1/R2 类型化动作可以进入 Executor。
                </TooltipContent>
              </Tooltip>
              {auth.mode === "oidc" && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <button
                      aria-label="退出登录"
                      className="icon-button"
                      onClick={() => void auth.signOut()}
                      type="button"
                    >
                      <LogOut size={16} />
                    </button>
                  </TooltipTrigger>
                  <TooltipContent>退出当前 OIDC 会话</TooltipContent>
                </Tooltip>
              )}
            </div>
          </header>
          <main className="main-content">
            <Outlet />
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
