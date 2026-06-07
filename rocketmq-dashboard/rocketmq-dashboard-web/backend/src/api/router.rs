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
use crate::middleware::http_trace_layer;
use crate::middleware::require_auth;
use crate::state::AppState;
use axum::Router;
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
        .route("/api/consumers", get(consumer_api::list_consumers))
        .route("/api/consumers/{group}", get(consumer_api::consumer_progress))
        .route("/api/consumers/{group}/progress", get(consumer_api::consumer_progress))
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
            post(config_api::add_nameserver).put(config_api::replace_nameservers),
        )
        .route("/api/config/vip-channel", put(config_api::set_vip_channel))
        .route("/api/config/tls", put(config_api::set_tls))
        .route("/api/config/proxies", post(config_api::add_proxy))
        .route("/api/config/proxies/current", put(config_api::switch_proxy))
        .route("/api/config/proxies/{address}", delete(config_api::delete_proxy))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/api/health", get(health_api::health))
        .route("/api/auth/session", get(auth_api::session))
        .route("/api/auth/login", post(auth_api::login))
        .route("/api/auth/logout", post(auth_api::logout))
        .merge(protected_routes)
        .layer(CorsLayer::permissive())
        .layer(http_trace_layer())
        .with_state(state)
}
