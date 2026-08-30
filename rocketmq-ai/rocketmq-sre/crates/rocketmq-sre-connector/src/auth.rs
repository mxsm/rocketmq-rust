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

use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::config::ConnectorAuth;
use crate::config::SecretValue;

#[derive(Clone)]
struct CachedToken {
    value: SecretValue,
    expires_at: Instant,
}

pub(crate) struct TokenProvider {
    auth: ConnectorAuth,
    http: reqwest::Client,
    cached: Mutex<Option<CachedToken>>,
    timeout: Duration,
}

impl TokenProvider {
    pub(crate) fn new(auth: ConnectorAuth, http: reqwest::Client, timeout: Duration) -> Self {
        Self {
            auth,
            http,
            cached: Mutex::new(None),
            timeout,
        }
    }

    pub(crate) async fn token(&self, force_refresh: bool) -> Result<String, ConnectorError> {
        match &self.auth {
            ConnectorAuth::DevelopmentToken { token, .. } => Ok(token.expose().to_owned()),
            ConnectorAuth::OAuth2(config) => {
                let mut cached = self.cached.lock().await;
                if !force_refresh
                    && let Some(token) = cached.as_ref()
                    && token.expires_at > Instant::now() + Duration::from_secs(30)
                {
                    return Ok(token.value.expose().to_owned());
                }

                #[derive(Serialize)]
                struct TokenRequest<'a> {
                    grant_type: &'static str,
                    scope: String,
                    audience: &'a str,
                }

                let request = TokenRequest {
                    grant_type: "client_credentials",
                    scope: config.scopes.join(" "),
                    audience: &config.audience,
                };
                let mut response = tokio::time::timeout(
                    self.timeout,
                    self.http
                        .post(config.token_endpoint.clone())
                        .basic_auth(&config.client_id, Some(config.client_secret.expose()))
                        .form(&request)
                        .send(),
                )
                .await
                .map_err(|_| ConnectorError::source("OAuth token request timed out"))?
                .map_err(|error| ConnectorError::source(format!("OAuth token endpoint request failed: {error}")))?;
                if !response.status().is_success() {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::UnauthorizedScope,
                        false,
                        format!("OAuth token endpoint returned status {}", response.status()),
                    ));
                }
                if response.content_length().is_some_and(|length| length > 64 * 1024) {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::OutputTooLarge,
                        false,
                        "OAuth token response exceeds 65536 bytes",
                    ));
                }
                let mut body = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| ConnectorError::source("OAuth token endpoint returned an invalid response"))?
                {
                    if body.len().saturating_add(chunk.len()) > 64 * 1024 {
                        return Err(ConnectorError::new(
                            ConnectorErrorCode::OutputTooLarge,
                            false,
                            "OAuth token response exceeds 65536 bytes",
                        ));
                    }
                    body.extend_from_slice(&chunk);
                }
                let token_response: OAuthTokenResponse = serde_json::from_slice(&body)
                    .map_err(|_| ConnectorError::source("OAuth token endpoint returned an invalid response"))?;
                if token_response.access_token.trim().is_empty()
                    || !token_response.token_type.eq_ignore_ascii_case("bearer")
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::UnauthorizedScope,
                        false,
                        "OAuth token response did not contain a Bearer access token",
                    ));
                }
                let lifetime = Duration::from_secs(token_response.expires_in.unwrap_or(300).max(31));
                let token = CachedToken {
                    value: SecretValue::new(token_response.access_token),
                    expires_at: Instant::now() + lifetime,
                };
                let exposed = token.value.expose().to_owned();
                *cached = Some(token);
                Ok(exposed)
            }
        }
    }

    pub(crate) async fn invalidate(&self) {
        if matches!(self.auth, ConnectorAuth::OAuth2(_)) {
            *self.cached.lock().await = None;
        }
    }
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default = "bearer_token_type")]
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

fn bearer_token_type() -> String {
    "Bearer".to_owned()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::Json;
    use axum::Router;
    use axum::routing::post;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::config::OAuth2Config;

    #[test]
    fn oauth_response_defaults_to_bearer() {
        let response: OAuthTokenResponse = serde_json::from_value(serde_json::json!({
            "access_token": "opaque",
            "expires_in": 120
        }))
        .expect("OAuth response");
        assert_eq!(response.token_type, "Bearer");
    }

    #[tokio::test]
    async fn oauth_token_is_cached_and_force_refresh_is_bounded() {
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let app = Router::new().route(
            "/token",
            post(move || {
                let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    Json(json!({
                        "access_token": format!("token-{count}"),
                        "token_type": "Bearer",
                        "expires_in": 300
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock OAuth server");
        });
        let auth = ConnectorAuth::OAuth2(OAuth2Config {
            token_endpoint: Url::parse(&format!("http://{address}/token")).expect("URL"),
            client_id: "connector".to_owned(),
            client_secret_env: "TEST_SECRET".to_owned(),
            audience: "rocketmq-mcp".to_owned(),
            scopes: vec!["rocketmq:read".to_owned()],
            client_secret: SecretValue::new("secret".to_owned()),
        });
        let provider = TokenProvider::new(auth, reqwest::Client::new(), Duration::from_secs(1));

        assert_eq!(provider.token(false).await.expect("first token"), "token-1");
        assert_eq!(provider.token(false).await.expect("cached token"), "token-1");
        assert_eq!(provider.token(true).await.expect("refreshed token"), "token-2");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
    }
}
