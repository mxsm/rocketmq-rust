import {
  UserManager,
  WebStorageStateStore,
  type User,
  type UserManagerSettings,
} from "oidc-client-ts";
import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

export type AuthMode = "development" | "oidc";
export type AuthStatus = "loading" | "authenticated" | "anonymous" | "error";

export interface AuthSession {
  subject: string;
  displayName: string;
  tenantId: string;
  clusterIds: string[];
  roles: string[];
  accessToken: string;
  expiresAt?: number;
}

export interface ApiRequestContext {
  token: string;
  tenantId: string;
  clusterIds: string[];
  subject: string;
  roles: string[];
}

interface AuthValue {
  mode: AuthMode;
  status: AuthStatus;
  session?: AuthSession;
  error?: string;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
  hasClusterScope: (clusterId: string) => boolean;
  requestContext?: ApiRequestContext;
}

const AuthContext = createContext<AuthValue | undefined>(undefined);

const DEFAULT_DEMO_CLUSTERS = [
  "10000000-0000-4000-8000-000000000001",
  "10000000-0000-4000-8000-000000000002",
  "10000000-0000-4000-8000-000000000003",
];

function list(value: string | undefined, fallback: string[]): string[] {
  const values = value
    ?.split(/[,\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  return values && values.length > 0 ? values : fallback;
}

function authMode(): AuthMode {
  return resolveAuthMode(
    import.meta.env.VITE_SRE_AUTH_MODE,
    import.meta.env.DEV,
  );
}

export function resolveAuthMode(
  configured: string | undefined,
  developmentBuild: boolean,
): AuthMode {
  if (configured === "development") {
    return "development";
  }
  if (configured === "oidc") {
    return "oidc";
  }
  return developmentBuild ? "development" : "oidc";
}

function developmentSession(): AuthSession {
  return {
    subject:
      import.meta.env.VITE_SRE_DEV_SUBJECT ?? "rocketmq-sre-development",
    displayName: import.meta.env.VITE_SRE_DEV_DISPLAY_NAME ?? "本地 SRE 工程师",
    tenantId:
      import.meta.env.VITE_SRE_DEV_TENANT ??
      "00000000-0000-4000-8000-000000000001",
    clusterIds: list(
      import.meta.env.VITE_SRE_DEV_CLUSTERS,
      DEFAULT_DEMO_CLUSTERS,
    ),
    roles: list(import.meta.env.VITE_SRE_DEV_ROLES, [
      "rocketmq:read",
      "rocketmq:diagnose",
      "operator",
      "approver",
      "model-governance",
    ]),
    accessToken:
      import.meta.env.VITE_SRE_DEV_TOKEN ?? "phase00-internal-token",
  };
}

function oidcSettings(): UserManagerSettings | undefined {
  const authority = import.meta.env.VITE_SRE_OIDC_AUTHORITY;
  const clientId = import.meta.env.VITE_SRE_OIDC_CLIENT_ID;
  if (!authority || !clientId) {
    return undefined;
  }
  return {
    authority,
    client_id: clientId,
    redirect_uri:
      import.meta.env.VITE_SRE_OIDC_REDIRECT_URI ??
      `${window.location.origin}/auth/callback`,
    post_logout_redirect_uri:
      import.meta.env.VITE_SRE_OIDC_POST_LOGOUT_REDIRECT_URI ??
      window.location.origin,
    response_type: "code",
    scope:
      import.meta.env.VITE_SRE_OIDC_SCOPE ??
      "openid profile rocketmq:read rocketmq:diagnose rocketmq:model-governance",
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    automaticSilentRenew: true,
    monitorSession: true,
  };
}

function stringClaim(
  profile: User["profile"],
  name: string,
): string | undefined {
  const value = profile[name];
  return typeof value === "string" ? value : undefined;
}

function stringListClaim(profile: User["profile"], name: string): string[] {
  const value = profile[name];
  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === "string");
  }
  return typeof value === "string" ? list(value, []) : [];
}

function sessionFromOidc(user: User): AuthSession {
  const subject = user.profile.sub;
  return {
    subject,
    displayName:
      stringClaim(user.profile, "name") ??
      stringClaim(user.profile, "preferred_username") ??
      subject,
    tenantId: stringClaim(user.profile, "rocketmq_tenant") ?? "",
    clusterIds: stringListClaim(user.profile, "rocketmq_clusters"),
    roles: [
      ...new Set([
        ...stringListClaim(user.profile, "roles"),
        ...list(user.scope, []),
      ]),
    ],
    accessToken: user.access_token,
    expiresAt: user.expires_at,
  };
}

function isOidcCallback() {
  const params = new URLSearchParams(window.location.search);
  return params.has("code") && params.has("state");
}

export function AuthProvider({ children }: PropsWithChildren) {
  const mode = useMemo(authMode, []);
  const manager = useMemo(() => {
    if (mode !== "oidc") {
      return undefined;
    }
    const settings = oidcSettings();
    return settings ? new UserManager(settings) : undefined;
  }, [mode]);
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [session, setSession] = useState<AuthSession>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    if (mode === "development") {
      setSession(developmentSession());
      setStatus("authenticated");
      return () => {
        active = false;
      };
    }
    if (!manager) {
      setError(
        "OIDC 模式缺少 VITE_SRE_OIDC_AUTHORITY 或 VITE_SRE_OIDC_CLIENT_ID。",
      );
      setStatus("error");
      return () => {
        active = false;
      };
    }

    const load = async () => {
      try {
        const user = isOidcCallback()
          ? await manager.signinRedirectCallback()
          : await manager.getUser();
        if (!active) {
          return;
        }
        if (isOidcCallback()) {
          window.history.replaceState({}, "", window.location.pathname);
        }
        if (!user || user.expired) {
          setStatus("anonymous");
          return;
        }
        const next = sessionFromOidc(user);
        if (!next.tenantId || next.clusterIds.length === 0) {
          setError("OIDC token 缺少 rocketmq_tenant 或 rocketmq_clusters claim。");
          setStatus("error");
          return;
        }
        setSession(next);
        setStatus("authenticated");
      } catch {
        if (active) {
          setError("OIDC 登录回调校验失败，请重新登录。");
          setStatus("error");
        }
      }
    };
    void load();
    return () => {
      active = false;
    };
  }, [manager, mode]);

  const signIn = useCallback(async () => {
    if (manager) {
      await manager.signinRedirect();
    }
  }, [manager]);

  const signOut = useCallback(async () => {
    if (manager) {
      await manager.signoutRedirect();
    }
  }, [manager]);

  const value = useMemo<AuthValue>(
    () => ({
      mode,
      status,
      session,
      error,
      signIn,
      signOut,
      hasClusterScope: (clusterId) =>
        session?.clusterIds.includes(clusterId) ?? false,
      requestContext: session
        ? {
            token: session.accessToken,
            tenantId: session.tenantId,
            clusterIds: session.clusterIds,
            subject: session.subject,
            roles: session.roles,
          }
        : undefined,
    }),
    [error, mode, session, signIn, signOut, status],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const value = useContext(AuthContext);
  if (!value) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return value;
}
