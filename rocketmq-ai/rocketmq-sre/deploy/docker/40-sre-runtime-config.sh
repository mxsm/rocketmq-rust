#!/bin/sh
set -eu

# Expose only public OIDC settings. Authentication mode and tokens stay build-only.
output=/usr/share/nginx/html/runtime-config.js
temporary="$(mktemp "${output}.XXXXXX")"
trap 'rm -f "$temporary"' EXIT

printf 'window.__ROCKETMQ_SRE_CONFIG__ = ' > "$temporary"
jq --null-input --compact-output '{oidc: ({
    authority: env.VITE_SRE_OIDC_AUTHORITY,
    clientId: env.VITE_SRE_OIDC_CLIENT_ID,
    redirectUri: env.VITE_SRE_OIDC_REDIRECT_URI,
    postLogoutRedirectUri: env.VITE_SRE_OIDC_POST_LOGOUT_REDIRECT_URI,
    scope: env.VITE_SRE_OIDC_SCOPE
} | with_entries(select(.value != null)))}' >> "$temporary"
printf ';\n' >> "$temporary"
chmod 0644 "$temporary"
mv "$temporary" "$output"
