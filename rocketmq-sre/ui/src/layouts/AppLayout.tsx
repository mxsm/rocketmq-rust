import {
  Activity,
  Boxes,
  CircleHelp,
  Clock3,
  DatabaseZap,
  Gauge,
  Network,
  RadioTower,
  ShieldCheck,
} from "lucide-react";
import { useEffect, useState } from "react";
import { NavLink, Outlet } from "react-router-dom";

import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const items = [
  { to: "/", label: "总览", icon: Gauge, end: true },
  { to: "/clusters", label: "集群", icon: RadioTower },
  { to: "/evidence", label: "证据工作台", icon: DatabaseZap },
  { to: "/coverage", label: "证据覆盖", icon: Boxes },
  { to: "/system", label: "系统状态", icon: Activity },
];

export function AppLayout() {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 30_000);
    return () => window.clearInterval(timer);
  }, []);

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

          <nav aria-label="主导航">
            {items.map(({ to, label, icon: Icon, end }) => (
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
          </nav>

          <div className="sidebar-spacer" />
          <div className="boundary-note">
            <ShieldCheck aria-hidden="true" size={16} />
            <div>
              <strong>只读模式</strong>
              <span>Phase 00 不提供任何变更或执行能力。</span>
            </div>
          </div>
          <div className="sidebar-meta">
            <span>v0.1.0 · Phase 00</span>
            <span>execution_supported=false</span>
          </div>
        </aside>

        <div className="workspace">
          <header className="utility-bar">
            <div className="utility-product">
              <Badge variant="outline">READ ONLY</Badge>
              <span>独立 AI SRE 运维面</span>
            </div>
            <div className="utility-actions">
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
                    aria-label="关于只读边界"
                    className="icon-button"
                    type="button"
                  >
                    <CircleHelp size={16} />
                  </button>
                </TooltipTrigger>
                <TooltipContent>
                  数据只用于观测和诊断，不会修改 RocketMQ。
                </TooltipContent>
              </Tooltip>
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
