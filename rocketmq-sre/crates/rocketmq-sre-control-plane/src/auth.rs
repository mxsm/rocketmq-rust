// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::http::HeaderMap;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use jsonwebtoken::decode;
use jsonwebtoken::decode_header;
use jsonwebtoken::jwk::JwkSet;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;

use crate::ControlPlaneConfig;
use crate::ControlPlaneError;

#[derive(Clone, Debug)]
pub(crate) struct AuthContext {
    pub tenant_id: TenantId,
    pub subject: String,
    pub clusters: BTreeSet<ClusterId>,
    pub roles: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct AuthService {
    mode: Arc<AuthMode>,
}

enum AuthMode {
    Development {
        token: Arc<str>,
    },
    Oidc {
        issuer: String,
        audience: String,
        jwks_url: url::Url,
        client: reqwest::Client,
        keys: RwLock<JwkSet>,
    },
}

#[derive(Clone, Debug, Deserialize)]
struct OidcClaims {
    sub: String,
    scope: String,
    #[serde(default)]
    roles: Vec<String>,
    rocketmq_tenant: String,
    #[serde(default)]
    rocketmq_clusters: Vec<String>,
}

impl AuthService {
    pub(crate) fn development(token: impl Into<Arc<str>>) -> Self {
        Self {
            mode: Arc::new(AuthMode::Development { token: token.into() }),
        }
    }

    pub(crate) async fn from_config(config: &ControlPlaneConfig) -> Result<Self, ControlPlaneError> {
        if config.dev_auth_enabled() {
            return Ok(Self {
                mode: Arc::new(AuthMode::Development {
                    token: Arc::<str>::from(config.internal_token()),
                }),
            });
        }

        let issuer = config
            .oidc_issuer()
            .ok_or_else(|| ControlPlaneError::configuration("OIDC issuer is required"))?
            .to_owned();
        let audience = config
            .oidc_audience()
            .ok_or_else(|| ControlPlaneError::configuration("OIDC audience is required"))?
            .to_owned();
        let jwks_url = config
            .oidc_jwks_url()
            .ok_or_else(|| ControlPlaneError::configuration("OIDC JWKS URL is required"))?
            .clone();
        let mut client_builder = reqwest::Client::builder().https_only(true);
        if let Some(path) = config.oidc_ca_path() {
            let pem = std::fs::read(path)
                .map_err(|_| ControlPlaneError::configuration("OIDC CA certificate cannot be read"))?;
            let certificate = reqwest::Certificate::from_pem(&pem)
                .map_err(|_| ControlPlaneError::configuration("OIDC CA certificate is invalid"))?;
            client_builder = client_builder.add_root_certificate(certificate);
        }
        let client = client_builder
            .build()
            .map_err(|_| ControlPlaneError::configuration("OIDC HTTP client cannot be built"))?;
        let keys = fetch_jwks(&client, &jwks_url).await?;
        Ok(Self {
            mode: Arc::new(AuthMode::Oidc {
                issuer,
                audience,
                jwks_url,
                client,
                keys: RwLock::new(keys),
            }),
        })
    }

    pub(crate) async fn authorize(
        &self,
        headers: &HeaderMap,
        cluster: Option<ClusterId>,
    ) -> Result<AuthContext, ControlPlaneError> {
        match self.mode.as_ref() {
            AuthMode::Development { token } => authorize_development(headers, token, cluster),
            AuthMode::Oidc {
                issuer,
                audience,
                jwks_url,
                client,
                keys,
            } => authorize_oidc(headers, cluster, issuer, audience, jwks_url, client, keys).await,
        }
    }

    pub(crate) async fn ready(&self) -> bool {
        match self.mode.as_ref() {
            AuthMode::Development { .. } => true,
            AuthMode::Oidc { keys, .. } => !keys.read().await.keys.is_empty(),
        }
    }
}

fn authorize_development(
    headers: &HeaderMap,
    expected_token: &str,
    cluster: Option<ClusterId>,
) -> Result<AuthContext, ControlPlaneError> {
    let token = bearer(headers)?;
    let matches = token.len() == expected_token.len() && bool::from(token.as_bytes().ct_eq(expected_token.as_bytes()));
    if !matches {
        return Err(ControlPlaneError::Unauthorized);
    }
    let tenant_id = required_header(headers, "x-rocketmq-tenant")?
        .parse()
        .map_err(|_| ControlPlaneError::forbidden("tenant_mismatch", "tenant claim must be a UUID"))?;
    let clusters = parse_clusters(required_header(headers, "x-rocketmq-clusters")?)?;
    enforce_cluster_scope(&clusters, cluster)?;
    Ok(AuthContext {
        tenant_id,
        subject: header(headers, "x-rocketmq-subject")
            .unwrap_or("rocketmq-sre-development")
            .to_owned(),
        clusters,
        roles: BTreeSet::from([
            "diagnose".to_owned(),
            "operator".to_owned(),
            "approver".to_owned(),
            "rocketmq:diagnose".to_owned(),
            "rocketmq:onboard".to_owned(),
        ]),
    })
}

async fn authorize_oidc(
    headers: &HeaderMap,
    cluster: Option<ClusterId>,
    issuer: &str,
    audience: &str,
    jwks_url: &url::Url,
    client: &reqwest::Client,
    keys: &RwLock<JwkSet>,
) -> Result<AuthContext, ControlPlaneError> {
    let token = bearer(headers)?;
    let header = decode_header(token).map_err(|_| ControlPlaneError::Unauthorized)?;
    if header.alg != Algorithm::RS256 {
        return Err(ControlPlaneError::Unauthorized);
    }
    let kid = header.kid.ok_or(ControlPlaneError::Unauthorized)?;
    let mut jwk = keys.read().await.find(&kid).cloned();
    if jwk.is_none() {
        let refreshed = fetch_jwks(client, jwks_url).await?;
        jwk = refreshed.find(&kid).cloned();
        *keys.write().await = refreshed;
    }
    let decoding_key = DecodingKey::from_jwk(jwk.as_ref().ok_or(ControlPlaneError::Unauthorized)?)
        .map_err(|_| ControlPlaneError::Unauthorized)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    let claims = decode::<OidcClaims>(token, &decoding_key, &validation)
        .map_err(|_| ControlPlaneError::Unauthorized)?
        .claims;
    if !claims.scope.split_ascii_whitespace().any(|scope| {
        matches!(
            scope,
            "rocketmq:read"
                | "rocketmq:diagnose"
                | "rocketmq:sre"
                | "rocketmq:operate"
                | "rocketmq:approve"
                | "rocketmq:executor"
        )
    }) {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "token does not grant RocketMQ SRE read or diagnose scope",
        ));
    }
    let tenant_id = claims
        .rocketmq_tenant
        .parse()
        .map_err(|_| ControlPlaneError::forbidden("tenant_mismatch", "tenant claim must be a UUID"))?;
    let clusters = claims
        .rocketmq_clusters
        .iter()
        .map(|value| {
            value
                .parse()
                .map_err(|_| ControlPlaneError::forbidden("cluster_not_allowed", "cluster claim must be a UUID"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    enforce_cluster_scope(&clusters, cluster)?;
    let mut roles = claims.roles.into_iter().collect::<BTreeSet<_>>();
    for scope in claims.scope.split_ascii_whitespace() {
        match scope {
            "rocketmq:diagnose" | "rocketmq:sre" => {
                roles.insert(scope.to_owned());
            }
            "rocketmq:operate" => {
                roles.insert("operator".to_owned());
            }
            "rocketmq:approve" => {
                roles.insert("approver".to_owned());
            }
            "rocketmq:executor" => {
                roles.insert("executor_service".to_owned());
            }
            _ => {}
        }
    }
    Ok(AuthContext {
        tenant_id,
        subject: claims.sub,
        clusters,
        roles,
    })
}

async fn fetch_jwks(client: &reqwest::Client, url: &url::Url) -> Result<JwkSet, ControlPlaneError> {
    client
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map_err(ControlPlaneError::from)
}

fn parse_clusters(value: &str) -> Result<BTreeSet<ClusterId>, ControlPlaneError> {
    value
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|_| ControlPlaneError::forbidden("cluster_not_allowed", "cluster scope must contain UUIDs"))
        })
        .collect()
}

fn enforce_cluster_scope(
    clusters: &BTreeSet<ClusterId>,
    requested: Option<ClusterId>,
) -> Result<(), ControlPlaneError> {
    if requested.is_some_and(|cluster| !clusters.contains(&cluster)) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "requested cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

fn bearer(headers: &HeaderMap) -> Result<&str, ControlPlaneError> {
    header(headers, axum::http::header::AUTHORIZATION.as_str())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(ControlPlaneError::Unauthorized)
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ControlPlaneError> {
    header(headers, name).ok_or(ControlPlaneError::Unauthorized)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn development_auth_enforces_tenant_and_cluster_scope() {
        let auth = AuthService {
            mode: Arc::new(AuthMode::Development {
                token: Arc::from("secret"),
            }),
        };
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer secret".parse().expect("header"),
        );
        headers.insert("x-rocketmq-tenant", tenant.to_string().parse().expect("header"));
        headers.insert("x-rocketmq-clusters", cluster.to_string().parse().expect("header"));

        let context = auth.authorize(&headers, Some(cluster)).await.expect("authorized");
        assert_eq!(context.tenant_id, tenant);

        let denied = auth
            .authorize(&headers, Some(ClusterId::new()))
            .await
            .expect_err("cross-cluster access must fail");
        assert!(matches!(
            denied,
            ControlPlaneError::Forbidden {
                code: "cluster_not_allowed",
                ..
            }
        ));
    }
}
