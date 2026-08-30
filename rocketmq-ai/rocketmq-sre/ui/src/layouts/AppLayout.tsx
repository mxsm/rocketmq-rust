import {
  Activity,
  ArchiveRestore,
  BadgeCheck,
  BookOpenCheck,
  Bot,
  Boxes,
  CalendarCheck2,
  ChartNoAxesCombined,
  CircleHelp,
  ClipboardList,
  Clock3,
  CircleDollarSign,
  DatabaseZap,
  Globe2,
  GitBranch,
  Gauge,
  ListChecks,
  LogOut,
  MessageSquareText,
  Network,
  PackageSearch,
  PlugZap,
  RadioTower,
  Rocket,
  SearchCode,
  ScrollText,
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
import {
  type OperatorLocale,
  type OperatorTimeZone,
  useOperatorPreferences,
} from "@/preferences/OperatorPreferences";

const groups = [
  {
    label: "Fleet",
    items: [
      { to: "/fleet", label: "Fleet 态势", icon: Globe2, end: true },
      {
        to: "/fleet/compliance",
        label: "资产与合规",
        icon: BadgeCheck,
      },
      { to: "/fleet/dr", label: "灾备中心", icon: ArchiveRestore },
    ],
  },
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
      { to: "/changes/releases", label: "发布护航", icon: Rocket },
      { to: "/changes/integrations", label: "企业集成", icon: PlugZap },
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
      { to: "/governance", label: "治理中心", icon: ScrollText },
      { to: "/finops", label: "模型与成本", icon: CircleDollarSign },
      {
        to: "/autonomy",
        label: "自治运营",
        icon: ChartNoAxesCombined,
      },
      { to: "/system", label: "系统状态", icon: Activity },
    ],
  },
];

const englishLabels: Record<string, string> = {
  Fleet: "Fleet",
  态势: "Situation",
  诊断: "Diagnosis",
  变更: "Change",
  证据: "Evidence",
  平台: "Platform",
  "Fleet 态势": "Fleet Overview",
  资产与合规: "Asset & Compliance",
  灾备中心: "DR Center",
  总览: "Overview",
  集群接入: "Cluster Onboarding",
  资产视图: "Assets",
  拓扑关系: "Topology",
  "Ask SRE": "Ask SRE",
  事件诊断: "Incidents",
  巡检建议: "Inspections",
  容量预测: "Forecasts",
  值班运营: "Operations",
  复盘改进: "Action Items",
  变更中心: "Change Center",
  发布护航: "Release Escort",
  企业集成: "Integrations",
  证据浏览器: "Evidence Explorer",
  消息旅程: "Message Journey",
  诊断覆盖: "Coverage",
  知识库: "Knowledge",
  模型能力: "Models",
  治理中心: "Governance",
  模型与成本: "Model & Cost",
  自治运营: "Autonomy",
  系统状态: "System",
};

export function AppLayout() {
  const auth = useAuth();
  const { clusters, loading } = useSreData();
  const preferences = useOperatorPreferences();
  const location = useLocation();
  const navigate = useNavigate();
  const [now, setNow] = useState(() => new Date());
  const isEnglish = preferences.locale === "en-US";

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
                <h2>
                  {isEnglish
                    ? (englishLabels[group.label] ?? group.label)
                    : group.label}
                </h2>
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
                    <span>
                      {isEnglish ? (englishLabels[label] ?? label) : label}
                    </span>
                  </NavLink>
                ))}
              </section>
            ))}
          </nav>

          <div className="sidebar-spacer" />
          <div className="boundary-note">
            <ShieldCheck aria-hidden="true" size={16} />
            <div>
              <strong>
                {isEnglish
                  ? "Layered change boundary"
                  : "分层变更安全边界"}
              </strong>
              <span>
                {isEnglish
                  ? "Diagnosis is read-only; changes require approval, fencing and a typed Agent."
                  : "诊断默认只读；变更仅限审批、围栏和类型化 Agent。"}
              </span>
            </div>
          </div>
          <div className="sidebar-meta">
            <span>v0.5.0 · Enterprise Fleet</span>
            <span>Typed, fenced operations only</span>
          </div>
        </aside>

        <div className="workspace">
          <header className="utility-bar">
            <div className="utility-product">
              <Badge variant="outline">ENTERPRISE</Badge>
              <span>
                {isEnglish
                  ? "Independent AI SRE plane"
                  : "独立 AI SRE 运维面"}
              </span>
            </div>
            <div className="utility-actions">
              <select
                aria-label={isEnglish ? "Language" : "语言"}
                className="utility-select"
                onChange={(event) =>
                  preferences.setLocale(event.target.value as OperatorLocale)
                }
                value={preferences.locale}
              >
                <option value="zh-CN">中文</option>
                <option value="en-US">EN</option>
              </select>
              <select
                aria-label={isEnglish ? "Time zone" : "时区"}
                className="utility-select utility-timezone"
                onChange={(event) =>
                  preferences.setTimeZone(
                    event.target.value as OperatorTimeZone,
                  )
                }
                value={preferences.timeZone}
              >
                <option value="Asia/Shanghai">UTC+8 · Shanghai</option>
                <option value="Asia/Singapore">UTC+8 · Singapore</option>
                <option value="UTC">UTC</option>
              </select>
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
                {preferences.formatDateTime(now)}
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
