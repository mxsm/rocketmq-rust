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

//! Development-only OAuth2/JWKS issuer for the Phase 00 Compose and Kind
//! environments. The rotating private keys are loaded from mounted fixtures
//! and are not included in the runtime image.

use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use axum::Form;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::post;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::Utc;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::encode;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::wait_for_signal_result;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tracing_subscriber::EnvFilter;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8092";
const DEFAULT_ISSUER: &str = "https://dev-issuer-tls:8443";
const DEFAULT_AUDIENCE: &str = "rocketmq-mcp";
const DEFAULT_CLIENT_ID: &str = "rocketmq-sre-connector";
const DEFAULT_CLIENT_SECRET: &str = "phase00-client-secret";
const DEFAULT_ADMIN_TOKEN: &str = "phase00-issuer-admin";
const DEFAULT_TENANT: &str = "00000000-0000-4000-8000-000000000002";
const DEFAULT_CLUSTER: &str = "sre-dev";
const RSA_MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const ROTATED_RSA_MODULUS: &str = "wgSHA3m657nyyP1uhnE5NH9J28hGl08yFfMT0IK4bnFar8rCTJEhusCj-wXGVyX2OkcAv1cGnkxd7vRiZ5YJdlmgp66oaQrLLuOvZqhFFaa494_eGo8nwZxRtyAZQBsg8JgtPBnGss-EqtrZMXdfOQ4S8-kQwepaAl8nV7szW_fhwSa85-FqTkQ_B3lGtFc_2Os_1IRNuIKTNqqMVEvG-vyim09YWLp-bMm-Ii0ymTrG3My1qLIPVf0-ZoJ6TCVJYdQFd2BrG40zkkF6Ln6lGuAjZDtun_yRH4z_-aiqAObF86TwMTD3kCYjgsQdc0FWf11Tax3VPogyvUN19iAfIQ";

#[derive(Clone)]
struct IssuerState {
    config: Arc<IssuerConfig>,
    signing_keys: Arc<[SigningKey; 2]>,
    generation: Arc<AtomicU64>,
}

struct SigningKey {
    encoding_key: EncodingKey,
    modulus: &'static str,
}

struct IssuerConfig {
    bind_addr: SocketAddr,
    issuer: String,
    audience: String,
    client_id: String,
    client_secret: String,
    admin_token: String,
    tenant: String,
    cluster: String,
    private_key_path: PathBuf,
    rotated_private_key_path: PathBuf,
}

impl IssuerConfig {
    fn from_env() -> Result<Self, IssuerError> {
        let private_key_path = required("ROCKETMQ_SRE_DEV_ISSUER_PRIVATE_KEY_PATH")?.into();
        let rotated_private_key_path = required("ROCKETMQ_SRE_DEV_ISSUER_ROTATED_PRIVATE_KEY_PATH")?.into();
        Ok(Self {
            bind_addr: env_or("ROCKETMQ_SRE_DEV_ISSUER_BIND_ADDR", DEFAULT_BIND_ADDR)
                .parse()
                .map_err(|_| IssuerError::Config("issuer bind address is invalid".to_owned()))?,
            issuer: env_or("ROCKETMQ_SRE_DEV_ISSUER_URL", DEFAULT_ISSUER),
            audience: env_or("ROCKETMQ_SRE_DEV_ISSUER_AUDIENCE", DEFAULT_AUDIENCE),
            client_id: env_or("ROCKETMQ_SRE_DEV_ISSUER_CLIENT_ID", DEFAULT_CLIENT_ID),
            client_secret: env_or("ROCKETMQ_SRE_DEV_ISSUER_CLIENT_SECRET", DEFAULT_CLIENT_SECRET),
            admin_token: env_or("ROCKETMQ_SRE_DEV_ISSUER_ADMIN_TOKEN", DEFAULT_ADMIN_TOKEN),
            tenant: env_or("ROCKETMQ_SRE_DEV_ISSUER_TENANT", DEFAULT_TENANT),
            cluster: env_or("ROCKETMQ_SRE_DEV_ISSUER_CLUSTER", DEFAULT_CLUSTER),
            private_key_path,
            rotated_private_key_path,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum IssuerError {
    #[error("invalid development issuer configuration: {0}")]
    Config(String),
    #[error("development issuer private key cannot be read")]
    PrivateKey,
    #[error("development issuer private key is invalid")]
    InvalidPrivateKey,
    #[error("development issuer listener failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Deserialize)]
struct TokenRequest {
    grant_type: String,
    scope: String,
    audience: String,
}

#[derive(Serialize)]
struct TokenClaims<'a> {
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    exp: usize,
    iat: usize,
    nbf: usize,
    scope: &'a str,
    roles: [&'static str; 1],
    client_id: &'a str,
    rocketmq_tenant: &'a str,
    rocketmq_clusters: [&'a str; 1],
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("phase00_dev_issuer=info")),
        )
        .json()
        .try_init()?;

    let config = IssuerConfig::from_env()?;
    let primary_key = read_signing_key(&config.private_key_path, RSA_MODULUS)?;
    let rotated_key = read_signing_key(&config.rotated_private_key_path, ROTATED_RSA_MODULUS)?;
    let bind_addr = config.bind_addr;
    let state = IssuerState {
        config: Arc::new(config),
        signing_keys: Arc::new([primary_key, rotated_key]),
        generation: Arc::new(AtomicU64::new(1)),
    };

    let runtime_owner = RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-sre-phase00-dev-issuer"))?;
    let service_result = runtime_owner.block_on(run(bind_addr, state));
    let shutdown_result = runtime_owner.shutdown_runtime_blocking();
    service_result?;
    shutdown_result?;
    Ok(())
}

fn read_signing_key(path: &Path, modulus: &'static str) -> Result<SigningKey, IssuerError> {
    let private_key = std::fs::read(path).map_err(|_| IssuerError::PrivateKey)?;
    let encoding_key = EncodingKey::from_rsa_pem(&private_key).map_err(|_| IssuerError::InvalidPrivateKey)?;
    Ok(SigningKey { encoding_key, modulus })
}

async fn run(bind_addr: SocketAddr, state: IssuerState) -> Result<(), IssuerError> {
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/.well-known/jwks.json", get(jwks))
        .route("/oauth/token", post(token))
        .route("/admin/rotate", post(rotate))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "Phase 00 development issuer is ready");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            if let Err(error) = wait_for_signal_result().await {
                tracing::warn!(error = %error, "issuer shutdown signal watcher failed");
            }
        })
        .await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "ready"}))
}

async fn jwks(State(state): State<IssuerState>) -> Json<serde_json::Value> {
    let generation = state.generation.load(Ordering::SeqCst);
    let signing_key = signing_key_for_generation(&state, generation);
    Json(json!({
        "keys": [{
            "kty": "RSA",
            "kid": key_id(generation),
            "alg": "RS256",
            "use": "sig",
            "key_ops": ["verify"],
            "n": signing_key.modulus,
            "e": "AQAB"
        }]
    }))
}

async fn token(
    State(state): State<IssuerState>,
    headers: HeaderMap,
    Form(request): Form<TokenRequest>,
) -> impl IntoResponse {
    if !valid_basic_credentials(&headers, &state.config)
        || request.grant_type != "client_credentials"
        || request.audience != state.config.audience
        || !request
            .scope
            .split_ascii_whitespace()
            .any(|scope| scope == "rocketmq:read")
    {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid_client"})));
    }

    let now = Utc::now().timestamp().max(0) as usize;
    let claims = TokenClaims {
        sub: "rocketmq-sre-connector",
        iss: &state.config.issuer,
        aud: &state.config.audience,
        exp: now + 120,
        iat: now,
        nbf: now,
        scope: &request.scope,
        roles: ["diagnose"],
        client_id: &state.config.client_id,
        rocketmq_tenant: &state.config.tenant,
        rocketmq_clusters: [&state.config.cluster],
    };
    let mut header = Header::new(Algorithm::RS256);
    let generation = state.generation.load(Ordering::SeqCst);
    header.kid = Some(key_id(generation));
    match encode(
        &header,
        &claims,
        &signing_key_for_generation(&state, generation).encoding_key,
    ) {
        Ok(access_token) => (
            StatusCode::OK,
            Json(json!({
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": 120,
                "scope": request.scope
            })),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "server_error"})),
        ),
    }
}

async fn rotate(State(state): State<IssuerState>, headers: HeaderMap) -> impl IntoResponse {
    let expected = format!("Bearer {}", state.config.admin_token);
    if headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()) != Some(expected.as_str()) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"})));
    }
    let generation = state.generation.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
    (StatusCode::OK, Json(json!({"kid": key_id(generation)})))
}

#[cfg(test)]
fn current_kid(state: &IssuerState) -> String {
    key_id(state.generation.load(Ordering::SeqCst))
}

fn key_id(generation: u64) -> String {
    format!("phase00-key-{generation}")
}

#[cfg(test)]
fn current_signing_key(state: &IssuerState) -> &SigningKey {
    signing_key_for_generation(state, state.generation.load(Ordering::SeqCst))
}

fn signing_key_for_generation(state: &IssuerState, generation: u64) -> &SigningKey {
    let generation = generation.saturating_sub(1);
    &state.signing_keys[generation as usize % state.signing_keys.len()]
}

fn valid_basic_credentials(headers: &HeaderMap, config: &IssuerConfig) -> bool {
    let Some(encoded) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
    else {
        return false;
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    decoded == format!("{}:{}", config.client_id, config.client_secret).as_bytes()
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn required(name: &str) -> Result<String, IssuerError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| IssuerError::Config(format!("required environment variable `{name}` is missing")))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn test_state() -> IssuerState {
        let private_key = include_bytes!("../../../../deploy/dev/fixtures/oauth-private-key.pem");
        let rotated_private_key = include_bytes!("../../../../deploy/dev/fixtures/oauth-rotated-private-key.pem");
        IssuerState {
            config: Arc::new(IssuerConfig {
                bind_addr: "127.0.0.1:0".parse().expect("test bind address"),
                issuer: DEFAULT_ISSUER.to_owned(),
                audience: DEFAULT_AUDIENCE.to_owned(),
                client_id: DEFAULT_CLIENT_ID.to_owned(),
                client_secret: DEFAULT_CLIENT_SECRET.to_owned(),
                admin_token: DEFAULT_ADMIN_TOKEN.to_owned(),
                tenant: DEFAULT_TENANT.to_owned(),
                cluster: DEFAULT_CLUSTER.to_owned(),
                private_key_path: PathBuf::from("fixture.pem"),
                rotated_private_key_path: PathBuf::from("rotated-fixture.pem"),
            }),
            signing_keys: Arc::new([
                SigningKey {
                    encoding_key: EncodingKey::from_rsa_pem(private_key)
                        .expect("development fixture should be a valid RSA key"),
                    modulus: RSA_MODULUS,
                },
                SigningKey {
                    encoding_key: EncodingKey::from_rsa_pem(rotated_private_key)
                        .expect("rotated development fixture should be a valid RSA key"),
                    modulus: ROTATED_RSA_MODULUS,
                },
            ]),
            generation: Arc::new(AtomicU64::new(1)),
        }
    }

    #[test]
    fn basic_credentials_are_exact() {
        let state = test_state();
        let encoded = STANDARD.encode(format!("{}:{}", state.config.client_id, state.config.client_secret));
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).expect("authorization header"),
        );

        assert!(valid_basic_credentials(&headers, &state.config));
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Basic Zm9vOmJhcg=="));
        assert!(!valid_basic_credentials(&headers, &state.config));
    }

    #[test]
    fn rotation_changes_key_identifier_and_public_key() {
        let state = test_state();
        assert_eq!(current_kid(&state), "phase00-key-1");
        assert_eq!(current_signing_key(&state).modulus, RSA_MODULUS);
        state.generation.fetch_add(1, Ordering::SeqCst);
        assert_eq!(current_kid(&state), "phase00-key-2");
        assert_eq!(current_signing_key(&state).modulus, ROTATED_RSA_MODULUS);
        assert_ne!(RSA_MODULUS, ROTATED_RSA_MODULUS);
    }
}
