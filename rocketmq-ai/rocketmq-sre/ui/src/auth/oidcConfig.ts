import type { UserManagerSettings } from "oidc-client-ts";

export interface OidcConfig {
  authority?: string;
  clientId?: string;
  redirectUri?: string;
  postLogoutRedirectUri?: string;
  scope?: string;
}

declare global {
  interface Window {
    __ROCKETMQ_SRE_CONFIG__?: { oidc?: OidcConfig };
  }
}

export function resolveOidcSettings(
  runtime: OidcConfig | undefined,
  build: OidcConfig,
  origin: string,
): UserManagerSettings | undefined {
  // Explicit empty runtime values disable stale build settings as well.
  const config = { ...build, ...runtime };
  if (!config.authority || !config.clientId) {
    return undefined;
  }
  return {
    authority: config.authority,
    client_id: config.clientId,
    redirect_uri: config.redirectUri || `${origin}/auth/callback`,
    post_logout_redirect_uri: config.postLogoutRedirectUri || origin,
    response_type: "code",
    scope:
      config.scope ||
      "openid profile rocketmq:read rocketmq:diagnose rocketmq:model-governance",
  };
}
