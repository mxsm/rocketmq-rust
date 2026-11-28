import {
  ChevronDown,
  ChevronRight,
  ChevronsUpDown,
  LogOut,
  Moon,
  Share2,
  Sun,
  UserRound,
} from "lucide-react";
import { useState } from "react";
import { Link, NavLink, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "@/auth/AuthContext";
import { WorkspaceScopeProvider, useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import {
  type OperatorLocale,
  type OperatorTimeZone,
  useOperatorPreferences,
} from "@/preferences/OperatorPreferences";

import { GlobalScopeBar } from "./GlobalScopeBar";
import { navigationForPath, workspaceNavigation } from "./navigation";
import { ShellDisclosure } from "./ShellDisclosure";
import "./workspace-shell.css";

export function AppLayout() {
  return (
    <WorkspaceScopeProvider>
      <WorkspaceShell />
    </WorkspaceScopeProvider>
  );
}

function WorkspaceShell() {
  const { clusters, loading, error } = useSreData();
  const preferences = useOperatorPreferences();
  const scope = useWorkspaceScope();
  const location = useLocation();
  const m = useWorkspaceMessages();
  const activeGroup = navigationForPath(location.pathname);
  const english = preferences.locale === "en-US";
  const [navigation, setNavigation] = useState({
    path: location.pathname,
    expanded: activeGroup.id,
  });
  const expandedGroup =
    navigation.path === location.pathname ? navigation.expanded : activeGroup.id;
  const today = new Intl.DateTimeFormat(preferences.locale, {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: preferences.timeZone,
  }).format(new Date());

  return (
    <div className="app-shell investigation-shell workspace-shell">
      <a className="skip-link" href="#workspace-content">
        {english ? "Skip to workspace" : "跳到工作区"}
      </a>
      <aside className="shell-sidebar">
        <div className="shell-brand">
          <Share2 aria-hidden="true" size={36} strokeWidth={1.65} />
          <span>
            <strong>RocketMQ</strong>
            <small>Rust AI SRE</small>
          </span>
        </div>
        <nav aria-label={english ? "Workspaces" : "主导航"} className="shell-sections">
          {workspaceNavigation.map(({ id, label, icon: Icon, to, items }) => {
            const expanded = expandedGroup === id;
            return (
              <div className="shell-section" key={id}>
                <div
                  className={`shell-section-heading${activeGroup.id === id ? " is-current" : ""}`}
                >
                  <Link
                    aria-current={activeGroup.id === id ? "location" : undefined}
                    onClick={() => setNavigation({ path: to, expanded: id })}
                    to={{ pathname: to, search: scope.invalid ? "" : scope.searchFor() }}
                  >
                    <Icon aria-hidden="true" size={20} strokeWidth={1.7} />
                    <span>{m[label]}</span>
                  </Link>
                  <button
                    aria-controls={`section-${id}`}
                    aria-expanded={expanded}
                    aria-label={english ? `${m[label]} pages` : `${m[label]}二级导航`}
                    onClick={() =>
                      setNavigation({ path: location.pathname, expanded: expanded ? "" : id })
                    }
                    type="button"
                  >
                    {expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
                  </button>
                </div>
                <div className="shell-subnav" hidden={!expanded} id={`section-${id}`}>
                  {items.map(({ to, label, ...rest }) => (
                    <NavLink
                      className={({ isActive }) =>
                        `shell-subnav-link${isActive ? " is-active" : ""}`
                      }
                      end={"end" in rest ? rest.end : false}
                      key={to}
                      to={to}
                    >
                      {m[label]}
                    </NavLink>
                  ))}
                </div>
              </div>
            );
          })}
        </nav>
        <OperatorControls />
      </aside>
      <div className="workspace">
        <header className="shell-toolbar">
          <GlobalScopeBar key={scope.scopeKey} />
          <span className="shell-today">
            {english ? "Today" : "当前日期"}
            <time>{today}</time>
          </span>
        </header>
        <main className="main-content" id="workspace-content" tabIndex={-1}>
          {loading && !clusters.length ? (
            <div className="page">
              <div className="state-panel" role="status">
                {english ? "Loading authorized workspace…" : "正在加载授权工作区…"}
              </div>
            </div>
          ) : scope.invalid ? null : error && !scope.clusterId ? (
            <div className="page">
              <div className="state-panel" role="alert">
                {error}
              </div>
            </div>
          ) : (
            <Outlet key={`${scope.scopeKey}:${location.pathname}`} />
          )}
        </main>
      </div>
    </div>
  );
}

function OperatorControls() {
  const auth = useAuth();
  const preferences = useOperatorPreferences();
  const english = preferences.locale === "en-US";
  const m = useWorkspaceMessages();
  const identity = auth.session?.displayName ?? (english ? "Signed out" : "未登录");
  return (
    <div className="shell-operator">
      <ShellDisclosure
        className="shell-account"
        label={english ? "Account and preferences" : "账号与偏好"}
        summary={
          <>
            <UserRound aria-hidden="true" size={18} />
            <span className="shell-identity">
              <strong>{identity}</strong>
              <small>
                {auth.mode === "development" ? "DEV" : "OIDC"} ·{" "}
                {auth.session?.clusterIds.length ?? 0} {english ? "clusters" : "个集群"}
              </small>
            </span>
            <ChevronsUpDown aria-hidden="true" size={14} />
          </>
        }
      >
        <div className="shell-preferences">
          <label>
            {english ? "Language" : "语言"}
            <select
              aria-label={english ? "Language" : "语言"}
              value={preferences.locale}
              onChange={(event) => preferences.setLocale(event.target.value as OperatorLocale)}
            >
              <option value="zh-CN">中文</option>
              <option value="en-US">EN</option>
            </select>
          </label>
          <label>
            {english ? "Time zone" : "时区"}
            <select
              aria-label={english ? "Time zone" : "时区"}
              value={preferences.timeZone}
              onChange={(event) => preferences.setTimeZone(event.target.value as OperatorTimeZone)}
            >
              <option value="Asia/Shanghai">UTC+8 · Shanghai</option>
              <option value="Asia/Singapore">UTC+8 · Singapore</option>
              <option value="UTC">UTC</option>
            </select>
          </label>
          <button
            type="button"
            onClick={() => preferences.setTheme(preferences.theme === "dark" ? "light" : "dark")}
          >
            {preferences.theme === "dark" ? <Sun size={16} /> : <Moon size={16} />}
            {preferences.theme === "dark" ? m.lightTheme : m.darkTheme}
          </button>
          {auth.mode === "oidc" && (
            <button onClick={() => void auth.signOut()} type="button">
              <LogOut size={16} />
              {english ? "Sign out" : "退出登录"}
            </button>
          )}
        </div>
      </ShellDisclosure>
    </div>
  );
}
