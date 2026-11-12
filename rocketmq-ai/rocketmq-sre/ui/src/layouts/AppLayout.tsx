import {
  LogOut,
  Moon,
  Network,
  ShieldCheck,
  Sun,
  UserRound,
} from "lucide-react";
import { Link, NavLink, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/badge";
import {
  WorkspaceScopeProvider,
  useWorkspaceScope,
} from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import {
  type OperatorLocale,
  type OperatorTimeZone,
  useOperatorPreferences,
} from "@/preferences/OperatorPreferences";

import { GlobalScopeBar } from "./GlobalScopeBar";
import { navigationForPath, workspaceNavigation } from "./navigation";

export function AppLayout() {
  return (
    <WorkspaceScopeProvider>
      <WorkspaceShell />
    </WorkspaceScopeProvider>
  );
}

function WorkspaceShell() {
  const auth = useAuth();
  const { clusters, loading, error } = useSreData();
  const preferences = useOperatorPreferences();
  const scope = useWorkspaceScope();
  const location = useLocation();
  const m = useWorkspaceMessages();
  const activeGroup = navigationForPath(location.pathname);
  const english = preferences.locale === "en-US";

  return (
    <div className="app-shell investigation-shell">
      <a className="skip-link" href="#workspace-content">
        {english ? "Skip to workspace" : "跳到工作区"}
      </a>
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
        <nav
          aria-label={english ? "Workspaces" : "主导航"}
          className="sidebar-navigation"
        >
          {workspaceNavigation.map(({ id, label, icon: Icon, to }) => (
            <Link
              aria-current={activeGroup.id === id ? "page" : undefined}
              aria-label={m[label]}
              className={`nav-item workspace-nav-item${activeGroup.id === id ? " active" : ""}`}
              key={id}
              title={m[label]}
              to={{
                pathname: to,
                search: scope.invalid ? "" : scope.searchFor(),
              }}
            >
              <Icon aria-hidden="true" size={18} />
              <span>{m[label]}</span>
            </Link>
          ))}
        </nav>
        <div className="boundary-note">
          <ShieldCheck aria-hidden="true" size={16} />
          <div>
            <strong>{english ? "Supervised changes" : "受监督变更"}</strong>
            <span>
              {english
                ? "Diagnosis is read-only. Changes follow existing approval and fencing."
                : "诊断保持只读；变更沿用审批与围栏。"}
            </span>
          </div>
        </div>
        <div className="sidebar-meta">
          <span>v0.5.0 · SRE</span>
        </div>
      </aside>
      <div className="workspace">
        <header className="utility-bar">
          <div className="utility-product">
            <Badge variant="outline">SRE</Badge>
            <strong>{m[activeGroup.label]}</strong>
          </div>
          <div className="utility-actions">
            <select
              aria-label={english ? "Language" : "语言"}
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
              aria-label={english ? "Time zone" : "时区"}
              className="utility-select utility-timezone"
              onChange={(event) =>
                preferences.setTimeZone(event.target.value as OperatorTimeZone)
              }
              value={preferences.timeZone}
            >
              <option value="Asia/Shanghai">UTC+8 · Shanghai</option>
              <option value="Asia/Singapore">UTC+8 · Singapore</option>
              <option value="UTC">UTC</option>
            </select>
            <button
              aria-label={
                preferences.theme === "dark" ? m.lightTheme : m.darkTheme
              }
              className="icon-button"
              onClick={() =>
                preferences.setTheme(
                  preferences.theme === "dark" ? "light" : "dark",
                )
              }
              type="button"
            >
              {preferences.theme === "dark" ? (
                <Sun size={17} />
              ) : (
                <Moon size={17} />
              )}
            </button>
            <span className="utility-identity">
              <UserRound aria-hidden="true" size={15} />
              <span>
                <strong>
                  {auth.session?.displayName ??
                    (english ? "Signed out" : "未登录")}
                </strong>
                <small>
                  {auth.mode === "development" ? "DEV" : "OIDC"} ·{" "}
                  {auth.session?.clusterIds.length ?? 0} clusters
                </small>
              </span>
            </span>
            {auth.mode === "oidc" && (
              <button
                aria-label={english ? "Sign out" : "退出登录"}
                className="icon-button"
                onClick={() => void auth.signOut()}
                type="button"
              >
                <LogOut size={16} />
              </button>
            )}
          </div>
        </header>
        <GlobalScopeBar key={scope.scopeKey} />
        <nav
          aria-label={`${m[activeGroup.label]} · ${english ? "pages" : "二级导航"}`}
          className="section-navigation"
        >
          {activeGroup.items.map(({ to, label, ...rest }) => (
            <NavLink
              className={({ isActive }) =>
                `section-nav-item${isActive ? " active" : ""}`
              }
              end={"end" in rest ? rest.end : false}
              key={to}
              to={to}
            >
              {m[label]}
            </NavLink>
          ))}
        </nav>
        <main className="main-content" id="workspace-content" tabIndex={-1}>
          {loading && !clusters.length ? (
            <div className="page">
              <div className="state-panel" role="status">
                {english
                  ? "Loading authorized workspace…"
                  : "正在加载授权工作区…"}
              </div>
            </div>
          ) : scope.invalid ? null : error && !scope.clusterId ? (
            <div className="page">
              <div className="state-panel" role="alert">
                {error}
              </div>
            </div>
          ) : (
            <WorkspaceOutlet key={`${scope.scopeKey}:${location.pathname}`} />
          )}
        </main>
      </div>
    </div>
  );
}

function WorkspaceOutlet() {
  return <Outlet />;
}
