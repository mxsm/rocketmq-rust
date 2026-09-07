// Copyright 2026 The RocketMQ Rust Authors
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

use crate::error::DashboardError;
use axum::Json as AxumJson;
use axum::extract::Extension as AxumExtension;
use axum::extract::FromRequest;
use axum::extract::FromRequestParts;
use axum::extract::Path as AxumPath;
use axum::extract::Query as AxumQuery;
use axum::extract::Request;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug)]
pub(crate) struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = DashboardError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let AxumJson(value) = AxumJson::<T>::from_request(request, state)
            .await
            .map_err(map_json_rejection)?;
        Ok(Self(value))
    }
}

impl<T> IntoResponse for ApiJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        AxumJson(self.0).into_response()
    }
}

#[derive(Debug)]
pub(crate) struct ApiQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = DashboardError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let AxumQuery(value) = AxumQuery::<T>::from_request_parts(parts, state)
            .await
            .map_err(|source| {
                DashboardError::request_rejection(
                    StatusCode::BAD_REQUEST,
                    "INVALID_QUERY",
                    "Query parameters are invalid",
                    source,
                )
            })?;
        Ok(Self(value))
    }
}

#[derive(Debug)]
pub(crate) struct ApiPath<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = DashboardError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let AxumPath(value) = AxumPath::<T>::from_request_parts(parts, state)
            .await
            .map_err(|source| {
                DashboardError::request_rejection(
                    StatusCode::BAD_REQUEST,
                    "INVALID_PATH",
                    "Path parameters are invalid",
                    source,
                )
            })?;
        Ok(Self(value))
    }
}

#[derive(Debug)]
pub(crate) struct ApiExtension<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiExtension<T>
where
    S: Send + Sync,
    T: Clone + Send + Sync + 'static,
{
    type Rejection = DashboardError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let AxumExtension(value) = AxumExtension::<T>::from_request_parts(parts, state)
            .await
            .map_err(|source| {
                DashboardError::request_rejection(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MISSING_REQUEST_CONTEXT",
                    "Required request context is unavailable",
                    source,
                )
            })?;
        Ok(Self(value))
    }
}

fn map_json_rejection(source: JsonRejection) -> DashboardError {
    let (status, code, message) = match source.status() {
        StatusCode::PAYLOAD_TOO_LARGE => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            "Request body is too large",
        ),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Request Content-Type must be application/json",
        ),
        StatusCode::UNPROCESSABLE_ENTITY => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_JSON_DATA",
            "JSON body does not match the request schema",
        ),
        StatusCode::BAD_REQUEST => (
            StatusCode::BAD_REQUEST,
            "INVALID_JSON",
            "Request body is not valid JSON",
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "REQUEST_BODY_ERROR",
            "Request body could not be processed",
        ),
    };
    DashboardError::request_rejection(status, code, message, source)
}

#[cfg(test)]
mod tests {
    use super::ApiExtension;
    use super::ApiJson;
    use super::ApiPath;
    use super::ApiQuery;
    use axum::Router;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::routing::post;
    use serde::Deserialize;
    use tower::ServiceExt;

    #[derive(Deserialize)]
    struct CountInput {
        count: u32,
    }

    async fn json_handler(ApiJson(input): ApiJson<CountInput>) -> String {
        input.count.to_string()
    }

    async fn query_handler(ApiQuery(input): ApiQuery<CountInput>) -> String {
        input.count.to_string()
    }

    async fn path_handler(ApiPath(input): ApiPath<u32>) -> String {
        input.to_string()
    }

    async fn extension_handler(ApiExtension(value): ApiExtension<u32>) -> String {
        value.to_string()
    }

    async fn assert_rejection(
        app: Router,
        request: Request<Body>,
        expected_status: StatusCode,
        expected_code: &str,
        expected_message: &str,
    ) {
        let response = app.oneshot(request).await.expect("request completed");
        assert_eq!(response.status(), expected_status);
        let body = to_bytes(response.into_body(), 4_096).await.expect("read response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON response");
        assert_eq!(value["success"], false);
        assert_eq!(value["code"], expected_code);
        assert_eq!(value["message"], expected_message);
        assert_eq!(value["data"], serde_json::Value::Null);
        assert!(!String::from_utf8_lossy(&body).contains("sensitive-rejection-detail"));
    }

    #[tokio::test]
    async fn json_rejections_use_fixed_public_envelopes() {
        let app = Router::new().route("/", post(json_handler));
        assert_rejection(
            app.clone(),
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"count":"sensitive-rejection-detail"}"#))
                .expect("request"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_JSON_DATA",
            "JSON body does not match the request schema",
        )
        .await;
        assert_rejection(
            app,
            Request::post("/").body(Body::from(r#"{"count":1}"#)).expect("request"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Request Content-Type must be application/json",
        )
        .await;
    }

    #[tokio::test]
    async fn query_path_and_extension_rejections_use_fixed_public_envelopes() {
        assert_rejection(
            Router::new().route("/", get(query_handler)),
            Request::get("/?count=sensitive-rejection-detail")
                .body(Body::empty())
                .expect("request"),
            StatusCode::BAD_REQUEST,
            "INVALID_QUERY",
            "Query parameters are invalid",
        )
        .await;
        assert_rejection(
            Router::new().route("/{count}", get(path_handler)),
            Request::get("/sensitive-rejection-detail")
                .body(Body::empty())
                .expect("request"),
            StatusCode::BAD_REQUEST,
            "INVALID_PATH",
            "Path parameters are invalid",
        )
        .await;
        assert_rejection(
            Router::new().route("/", get(extension_handler)),
            Request::get("/").body(Body::empty()).expect("request"),
            StatusCode::INTERNAL_SERVER_ERROR,
            "MISSING_REQUEST_CONTEXT",
            "Required request context is unavailable",
        )
        .await;
    }
}
