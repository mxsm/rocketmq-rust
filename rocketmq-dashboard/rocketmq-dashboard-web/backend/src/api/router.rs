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
use crate::api::acl_api;
use crate::api::audit_api;
use crate::api::auth_api;
use crate::api::broker_api;
use crate::api::config_api;
use crate::api::consumer_api;
use crate::api::dashboard_api;
use crate::api::health_api;
use crate::api::message_api;
use crate::api::monitor_api;
use crate::api::producer_api;
use crate::api::topic_api;
use crate::middleware::audit_mutation;
use crate::middleware::http_trace_layer;
use crate::middleware::optional_auth;
use crate::middleware::require_auth;
use crate::state::AppState;
use axum::Router;
use axum::http::HeaderValue;
use axum::http::header::AUTHORIZATION;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::HeaderName;
use axum::middleware;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use tower_http::cors::CorsLayer;

pub fn build_router(state: AppState) -> Router {
    let protected_routes = Router::new()
        .route("/api/dashboard/overview", get(dashboard_api::overview))
        .route("/api/dashboard/topic-current", get(dashboard_api::topic_current))
        .route("/api/dashboard/brokers/history", get(dashboard_api::broker_history))
        .route("/api/dashboard/topics/history", get(dashboard_api::topic_history))
        .route("/api/topics", get(topic_api::list_topics).post(topic_api::create_topic))
        .route(
            "/api/topics/{topic}",
            get(topic_api::get_topic)
                .put(topic_api::update_topic)
                .delete(topic_api::delete_topic),
        )
        .route("/api/topics/{topic}/route", get(topic_api::topic_route))
        .route("/api/topics/{topic}/stats", get(topic_api::topic_stats))
        .route("/api/topics/{topic}/config", get(topic_api::topic_config))
        .route("/api/topics/{topic}/consumers", get(topic_api::topic_consumers))
        .route(
            "/api/topics/{topic}/test-message",
            post(topic_api::send_topic_test_message),
        )
        .route(
            "/api/topics/{topic}/consumer-offset/reset",
            post(topic_api::reset_topic_consumer_offset),
        )
        .route(
            "/api/topics/{topic}/consumer-offset/skip",
            post(topic_api::skip_topic_consumer_offset),
        )
        .route(
            "/api/topics/{topic}/brokers/{broker}",
            delete(topic_api::delete_topic_from_broker),
        )
        .route(
            "/api/consumers",
            get(consumer_api::list_consumers).post(consumer_api::create_consumer),
        )
        .route(
            "/api/consumers/{group}",
            get(consumer_api::consumer_summary)
                .put(consumer_api::update_consumer)
                .delete(consumer_api::delete_consumer),
        )
        .route(
            "/api/consumers/{group}/connections",
            get(consumer_api::consumer_connections),
        )
        .route("/api/consumers/{group}/progress", get(consumer_api::consumer_progress))
        .route("/api/consumers/{group}/config", get(consumer_api::consumer_config))
        .route(
            "/api/consumers/{group}/clients/{clientId}/running-info",
            get(consumer_api::consumer_running_info),
        )
        .route(
            "/api/consumers/{group}/clients/{clientId}/jstack",
            get(consumer_api::consumer_jstack),
        )
        .route("/api/consumers/{group}/brokers", get(consumer_api::consumer_brokers))
        .route("/api/consumers/{group}/reset-offset", post(consumer_api::reset_offset))
        .route("/api/producers", get(producer_api::list_producers))
        .route("/api/producers/connections", get(producer_api::producer_connections))
        .route("/api/brokers", get(broker_api::list_brokers))
        .route("/api/brokers/{broker_name}", get(broker_api::broker_runtime))
        .route("/api/brokers/{broker_name}/runtime", get(broker_api::broker_runtime))
        .route(
            "/api/brokers/{broker_name}/config",
            get(broker_api::broker_config).put(broker_api::update_broker_config),
        )
        .route("/api/messages", get(message_api::query_messages))
        .route("/api/messages/by-key", get(message_api::query_message_by_key))
        .route(
            "/api/messages/by-id/{message_id}",
            get(message_api::query_message_by_id),
        )
        .route("/api/messages/dlq", get(message_api::query_dlq_messages))
        .route("/api/messages/dlq/resend", post(message_api::resend_dlq_message))
        .route("/api/messages/dlq/export", get(message_api::export_dlq_messages))
        .route("/api/messages/{message_id}/trace", get(message_api::message_trace))
        .route("/api/messages/{message_id}/resend", post(message_api::resend_message))
        .route("/api/acl/users", get(acl_api::list_users).post(acl_api::create_user))
        .route(
            "/api/acl/users/{access_key}",
            put(acl_api::update_user).delete(acl_api::delete_user),
        )
        .route(
            "/api/acl/policies",
            get(acl_api::list_policies).post(acl_api::create_policy),
        )
        .route(
            "/api/acl/policies/{policy_name}",
            put(acl_api::update_policy).delete(acl_api::delete_policy),
        )
        .route(
            "/api/monitors/consumers",
            get(monitor_api::list_consumer_monitors).post(monitor_api::create_consumer_monitor),
        )
        .route(
            "/api/monitors/consumers/{consumer_group}",
            delete(monitor_api::delete_consumer_monitor),
        )
        .route("/api/config", get(config_api::get_config))
        .route(
            "/api/config/nameservers",
            get(config_api::get_nameserver_availability)
                .post(config_api::add_nameserver)
                .put(config_api::replace_nameservers),
        )
        .route("/api/config/nameservers/current", put(config_api::switch_nameserver))
        .route(
            "/api/config/nameservers/{endpoint_id}",
            delete(config_api::delete_nameserver),
        )
        .route("/api/config/vip-channel", put(config_api::set_vip_channel))
        .route("/api/config/tls", put(config_api::set_tls))
        .route("/api/config/proxies", post(config_api::add_proxy))
        .route("/api/config/proxies/current", put(config_api::switch_proxy))
        .route("/api/config/proxies/{endpoint_id}", delete(config_api::delete_proxy))
        // Authentication is outermost so the audit middleware receives the
        // repository-validated actor extension, never a client claim.
        .route_layer(middleware::from_fn_with_state(state.clone(), audit_mutation))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let session_status_route = Router::new()
        .route("/api/auth/session", get(auth_api::session))
        .layer(middleware::from_fn_with_state(state.clone(), optional_auth));
    let secured_auth_routes = Router::new()
        .route("/api/auth/logout", post(auth_api::logout))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let auth_routes = session_status_route.merge(secured_auth_routes);

    let admin_session_audit_routes = Router::new()
        .route("/api/auth/sessions", get(audit_api::list_sessions))
        .route("/api/auth/sessions/revoke-all", post(audit_api::revoke_all_sessions))
        .route("/api/audit/events", get(audit_api::list_events))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let router = Router::new()
        .route("/api/health", get(health_api::health))
        .route("/api/health/live", get(health_api::live))
        .route("/api/health/ready", get(health_api::ready))
        .route("/api/auth/login", post(auth_api::login))
        .merge(auth_routes)
        .merge(admin_session_audit_routes)
        .merge(protected_routes)
        .layer(http_trace_layer())
        .with_state(state.clone());

    let Some(origin) = state.auth_state.allowed_origin() else {
        return router;
    };
    router.layer(cors_layer(origin))
}

fn cors_layer(origin: HeaderValue) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(origin)
        .allow_credentials(true)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::DELETE,
        ])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static("x-dashboard-session"),
        ])
}

#[cfg(test)]
mod tests {
    use super::build_router;
    use super::cors_layer;
    use crate::config::AppConfig;
    use crate::config::AuthConfig;
    use crate::config::ServerConfig;
    use crate::config::SqlPoolConfig;
    use crate::config::StorageConfig;
    use crate::model::DashboardConfigView;
    use crate::model::LoginRequest;
    use crate::model::StorageBackend;
    use crate::service;
    use crate::state::AppState;
    use axum::Router;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::header::AUTHORIZATION;
    use axum::routing::get;
    use rocketmq_admin_core::client_adapter::ClientRuntime;
    use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_config(data_path: PathBuf) -> AppConfig {
        AppConfig {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
            },
            storage: StorageConfig {
                backend: StorageBackend::File,
                data_path,
                database_url: None,
                pool: SqlPoolConfig::default(),
            },
            auth: AuthConfig {
                login_required: true,
                username: "admin".to_string(),
                password: "test-password".to_string(),
                cookie_secure: false,
                ..AuthConfig::default()
            },
            dashboard_history_interval_secs: 0,
            dashboard_history_retention_days: 30,
            dashboard_history_retention_batch_size: 500,
            dashboard_history_lease_ttl_secs: 30,
            initial_config: DashboardConfigView::default(),
            admin_credentials: None,
        }
    }

    async fn test_state(owner: &RuntimeOwner, data_path: PathBuf) -> (AppState, Arc<ClientRuntime>) {
        let client_runtime = ClientRuntime::try_new(
            owner.root_context().component("router-auth-test-admin-client"),
            ClientRuntimeConfig::default(),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .expect("client runtime");
        let state = AppState::try_new_without_environment_convergence(test_config(data_path), client_runtime.clone())
            .await
            .expect("app state");
        (state, client_runtime)
    }

    #[tokio::test]
    async fn exact_origin_uses_credentialed_preflight_without_panicking() {
        let app = Router::new()
            .route("/api/ping", get(|| async { "ok" }))
            .layer(cors_layer(HeaderValue::from_static("https://console.example")));
        let request = Request::builder()
            .method("OPTIONS")
            .uri("/api/ping")
            .header("origin", "https://console.example")
            .header("access-control-request-method", "POST")
            .body(Body::empty())
            .expect("valid preflight request");

        let response = app.oneshot(request).await.expect("cors layer response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("https://console.example")
        );
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-credentials")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
    }

    #[test]
    fn session_status_propagates_ambiguous_credentials_and_accepts_identical_duplicates() {
        let directory = tempfile::tempdir().expect("temp dir");
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        owner.block_on(async {
            let (state, client_runtime) = test_state(&owner, directory.path().join("dashboard")).await;
            let session = service::login(
                &state,
                LoginRequest {
                    username: "admin".to_string(),
                    password: "test-password".to_string(),
                },
            )
            .await
            .expect("login");
            let token = session.session_id.expect("session token");
            let app = build_router(state.clone());

            let identical_request = Request::builder()
                .uri("/api/auth/session")
                .header("x-dashboard-session", &token)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header("cookie", format!("dashboard_session={token}"))
                .body(Body::empty())
                .expect("identical credential request");
            let identical_response = app.clone().oneshot(identical_request).await.expect("session response");
            assert_eq!(identical_response.status(), StatusCode::OK);
            let identical_body = to_bytes(identical_response.into_body(), 4_096)
                .await
                .expect("session body");
            let identical_json: serde_json::Value = serde_json::from_slice(&identical_body).expect("session json");
            assert_eq!(identical_json["data"]["authenticated"], true);

            for uri in ["/api/auth/session", "/api/dashboard/overview"] {
                let request = Request::builder()
                    .uri(uri)
                    .header("x-dashboard-session", "header-secret")
                    .header(AUTHORIZATION, "Bearer bearer-secret")
                    .header("cookie", "dashboard_session=cookie-secret")
                    .body(Body::empty())
                    .expect("conflicting credential request");
                let response = app.clone().oneshot(request).await.expect("error response");
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
                let body = to_bytes(response.into_body(), 4_096).await.expect("error body");
                let json: serde_json::Value = serde_json::from_slice(&body).expect("error json");
                assert_eq!(json["code"], "AUTH_TOKEN_AMBIGUOUS");
                assert_eq!(json["message"], "Ambiguous session credentials");
                let text = String::from_utf8(body.to_vec()).expect("utf-8 error body");
                assert!(!text.contains("header-secret"));
                assert!(!text.contains("bearer-secret"));
                assert!(!text.contains("cookie-secret"));
            }

            drop(app);
            drop(state);
            client_runtime.shutdown().await.log_if_unhealthy();
        });
        owner.shutdown_runtime_blocking().expect("runtime shutdown");
    }
}
