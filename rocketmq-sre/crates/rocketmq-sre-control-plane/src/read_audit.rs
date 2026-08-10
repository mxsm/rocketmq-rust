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

use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use chrono::Utc;
use rocketmq_sre_contracts::CorrelationId;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::api::AppState;
use crate::auth::AuthContext;
use crate::observability::CORRELATION_ID_HEADER;
use crate::observability::CorrelationContext;

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// Adds one append-only, sanitized audit record for authenticated public API
/// requests. Health, metrics, and connector-internal routes are deliberately
/// outside this boundary.
pub(crate) async fn middleware(State(state): State<AppState>, mut request: Request<Body>, next: Next) -> Response {
    if !request.uri().path().starts_with("/v1/") {
        return next.run(request).await;
    }

    let correlation = CorrelationContext::from_optional_header(
        request
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
    );
    if let Ok(value) = HeaderValue::from_str(&correlation.header_value()) {
        request.headers_mut().insert(CORRELATION_ID_HEADER, value);
    }

    let auth = match state.auth.authorize(request.headers(), None).await {
        Ok(auth) => auth,
        Err(error) => return response_with_correlation(error.into_response(), correlation).await,
    };
    let descriptor = RequestDescriptor::from_request(&request);
    let response = next.run(request).await;
    let audit = ReadAuditRecord::from_response(&auth, correlation.id(), descriptor, &response);
    if let Err(error) = append(&state, &audit).await {
        tracing::warn!(
            error_class = audit_error_class(&error),
            correlation_id = %correlation.id(),
            "read audit record could not be persisted"
        );
    }
    response_with_correlation(response, correlation).await
}

fn audit_error_class(error: &ControlPlaneError) -> &'static str {
    match error {
        ControlPlaneError::Database(_) => "database",
        _ => "unexpected",
    }
}

async fn append(state: &AppState, record: &ReadAuditRecord) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO read_audit (
            audit_id, tenant_id, cluster_id, actor_subject, operation,
            resource_type, resource_id, correlation_id, request_hash,
            outcome, error_code, row_count, byte_count, occurred_at
         ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13, $14
         )",
    )
    .bind(record.audit_id)
    .bind(record.tenant_id)
    .bind(record.cluster_id)
    .bind(&record.actor_subject)
    .bind(record.operation)
    .bind(&record.resource_type)
    .bind(&record.resource_id)
    .bind(record.correlation_id)
    .bind(&record.request_hash)
    .bind(record.outcome)
    .bind(record.error_code)
    .bind(record.row_count)
    .bind(record.byte_count)
    .bind(record.occurred_at)
    .execute(&state.repository.pool)
    .await?;
    Ok(())
}

async fn response_with_correlation(mut response: Response, correlation: CorrelationContext) -> Response {
    if let Ok(value) = HeaderValue::from_str(&correlation.header_value()) {
        response.headers_mut().insert(CORRELATION_ID_HEADER, value);
    }
    if !response.status().is_client_error() && !response.status().is_server_error() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = to_bytes(body, MAX_ERROR_BODY_BYTES).await else {
        return bounded_error_response(
            parts.status,
            parts.headers,
            correlation,
            "source_unavailable",
            "error response exceeded the bounded envelope",
            true,
        );
    };
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    let Some(object) = value.as_object_mut() else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    object.insert(
        "schema_version".to_owned(),
        serde_json::Value::String("rocketmq-sre.error.v1".to_owned()),
    );
    object.insert(
        "correlation_id".to_owned(),
        serde_json::Value::String(correlation.id().to_string()),
    );
    let Ok(encoded) = serde_json::to_vec(&value) else {
        return bounded_error_response(
            parts.status,
            parts.headers,
            correlation,
            "source_unavailable",
            "error response could not be encoded",
            true,
        );
    };
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(encoded))
}

fn bounded_error_response(
    status: StatusCode,
    mut headers: HeaderMap,
    correlation: CorrelationContext,
    code: &'static str,
    message: &'static str,
    retryable: bool,
) -> Response {
    let encoded = match serde_json::to_vec(&serde_json::json!({
        "schema_version": "rocketmq-sre.error.v1",
        "code": code,
        "message": message,
        "retryable": retryable,
        "correlation_id": correlation.id(),
    })) {
        Ok(encoded) => encoded,
        Err(_) => b"{\"code\":\"source_unavailable\"}".to_vec(),
    };
    headers.remove(axum::http::header::CONTENT_LENGTH);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let mut response = Response::new(Body::from(encoded));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

#[derive(Debug)]
struct RequestDescriptor {
    operation: &'static str,
    resource_type: String,
    resource_id: Option<String>,
    request_hash: String,
}

impl RequestDescriptor {
    fn from_request(request: &Request<Body>) -> Self {
        let path = request.uri().path();
        let segments = path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let resource_type = segments.get(1).copied().unwrap_or("api").to_owned();
        let resource_id = segments
            .get(2)
            .and_then(|value| Uuid::parse_str(value).ok())
            .map(|value| value.to_string());
        let normalized_path = segments
            .iter()
            .map(|segment| {
                if Uuid::parse_str(segment).is_ok() {
                    ":id"
                } else {
                    segment
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        let mut query_keys = request
            .uri()
            .query()
            .into_iter()
            .flat_map(|query| query.split('&'))
            .map(|pair| pair.split_once('=').map_or(pair, |(key, _)| key))
            .filter(|key| !key.is_empty())
            .collect::<Vec<_>>();
        query_keys.sort_unstable();
        query_keys.dedup();
        let hash_input = format!(
            "{}\n/{normalized_path}\n{}",
            request.method().as_str(),
            query_keys.join("&")
        );
        let digest = Sha256::digest(hash_input.as_bytes());
        Self {
            operation: operation_for(request.method()),
            resource_type,
            resource_id,
            request_hash: format!("sha256:{}", rocketmq_sre_contracts::encode_lower_hex(digest)),
        }
    }
}

fn operation_for(method: &Method) -> &'static str {
    match *method {
        Method::GET | Method::HEAD => "read",
        Method::POST => "invoke",
        Method::PUT | Method::PATCH => "update",
        Method::DELETE => "delete",
        _ => "request",
    }
}

#[derive(Debug)]
struct ReadAuditRecord {
    audit_id: Uuid,
    tenant_id: Uuid,
    cluster_id: Option<Uuid>,
    actor_subject: String,
    operation: &'static str,
    resource_type: String,
    resource_id: Option<String>,
    correlation_id: Uuid,
    request_hash: String,
    outcome: &'static str,
    error_code: Option<&'static str>,
    row_count: Option<i64>,
    byte_count: Option<i64>,
    occurred_at: chrono::DateTime<Utc>,
}

impl ReadAuditRecord {
    fn from_response(
        auth: &AuthContext,
        correlation_id: CorrelationId,
        descriptor: RequestDescriptor,
        response: &Response,
    ) -> Self {
        let status = response.status();
        let byte_count = response
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok());
        Self {
            audit_id: Uuid::new_v4(),
            tenant_id: auth.tenant_id.as_uuid(),
            cluster_id: descriptor
                .resource_id
                .as_deref()
                .and_then(|value| Uuid::parse_str(value).ok())
                .filter(|cluster| {
                    descriptor.resource_type == "clusters"
                        && auth.clusters.iter().any(|allowed| allowed.as_uuid() == *cluster)
                })
                .or_else(|| {
                    (auth.clusters.len() == 1)
                        .then(|| auth.clusters.first().map(|cluster| cluster.as_uuid()))
                        .flatten()
                }),
            actor_subject: auth.subject.clone(),
            operation: descriptor.operation,
            resource_type: descriptor.resource_type,
            resource_id: descriptor.resource_id,
            correlation_id: correlation_id.as_uuid(),
            request_hash: descriptor.request_hash,
            outcome: outcome(status),
            error_code: error_code(status),
            row_count: None,
            byte_count,
            occurred_at: Utc::now(),
        }
    }
}

fn outcome(status: StatusCode) -> &'static str {
    if status == StatusCode::PARTIAL_CONTENT {
        "partial"
    } else if status.is_success() {
        "success"
    } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        "denied"
    } else {
        "failed"
    }
}

fn error_code(status: StatusCode) -> Option<&'static str> {
    match status {
        StatusCode::UNAUTHORIZED => Some("unauthorized_scope"),
        StatusCode::FORBIDDEN => Some("cluster_not_allowed"),
        StatusCode::NOT_FOUND => Some("source_unavailable"),
        StatusCode::CONFLICT => Some("capability_mismatch"),
        StatusCode::TOO_MANY_REQUESTS => Some("rate_limited"),
        status if status.is_server_error() => Some("source_unavailable"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use axum::http::Request;

    use super::*;

    #[test]
    fn request_hash_is_stable_without_retaining_query_values() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/v1/evidence/2a4bbca1-f3fe-4ea5-86aa-17689ccff507?token=secret&limit=50")
            .body(Body::empty())
            .expect("request");
        let equivalent = Request::builder()
            .method(Method::GET)
            .uri("/v1/evidence/6f643333-3f6c-4f87-ac7e-f9c73e2e1f25?limit=10&token=other")
            .body(Body::empty())
            .expect("request");

        let first = RequestDescriptor::from_request(&request);
        let second = RequestDescriptor::from_request(&equivalent);

        assert_eq!(first.request_hash, second.request_hash);
        assert!(!first.request_hash.contains("secret"));
        assert_eq!(first.resource_type, "evidence");
    }

    #[test]
    fn response_outcome_is_bounded_to_the_audit_contract() {
        assert_eq!(outcome(StatusCode::OK), "success");
        assert_eq!(outcome(StatusCode::PARTIAL_CONTENT), "partial");
        assert_eq!(outcome(StatusCode::FORBIDDEN), "denied");
        assert_eq!(outcome(StatusCode::BAD_GATEWAY), "failed");
    }

    #[tokio::test]
    async fn error_body_and_header_share_the_request_correlation_id() {
        let correlation = CorrelationContext::from_id(CorrelationId::new());
        let response = response_with_correlation(ControlPlaneError::Unauthorized.into_response(), correlation).await;
        let header = response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("correlation response header")
            .to_owned();
        let body = to_bytes(response.into_body(), MAX_ERROR_BODY_BYTES)
            .await
            .expect("bounded error body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("error envelope");

        assert_eq!(body["schema_version"], "rocketmq-sre.error.v1");
        assert_eq!(body["correlation_id"], header);
    }
}
