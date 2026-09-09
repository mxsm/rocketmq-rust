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

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_proxy_core::context::{ProxyContextWithPrincipal, ProxyRequestMetadata};
#[cfg(test)]
use rocketmq_proxy_core::ProxyContext;
use rocketmq_proxy_core::{ProxyError, ProxyResult};
use rocketmq_transport::api::{RemotingRequest, SessionView};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tonic::{metadata::MetadataMap, Request};
use uuid::Uuid;

/// Local listener metadata attached to an accepted gRPC request.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcTransportContext {
    local_addr: String,
}

impl GrpcTransportContext {
    pub fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr: local_addr.to_string(),
        }
    }

    pub fn local_addr(&self) -> &str {
        self.local_addr.as_str()
    }
}

/// Extracts owned request metadata at the network ingress boundary.
pub trait ProxyContextExt: Sized {
    fn from_grpc_request<T>(rpc_name: &'static str, request: &Request<T>) -> ProxyResult<Self>;
    fn from_remoting_request(rpc_name: &'static str, request: &RemotingRequest) -> Self;
}

impl<P> ProxyContextExt for ProxyContextWithPrincipal<P> {
    fn from_grpc_request<T>(rpc_name: &'static str, request: &Request<T>) -> ProxyResult<Self> {
        let metadata = request.metadata();
        let deadline = parse_grpc_timeout_metadata(metadata)?;
        let received_at = Instant::now();
        let deadline_at = deadline.map(|timeout| received_at.checked_add(timeout).unwrap_or(received_at));

        Ok(Self::from_metadata(
            rpc_name,
            ProxyRequestMetadata {
                request_id: Uuid::new_v4().to_string(),
                remote_addr: request.remote_addr().map(|addr| addr.to_string()),
                local_addr: request
                    .extensions()
                    .get::<GrpcTransportContext>()
                    .map(GrpcTransportContext::local_addr)
                    .map(str::to_owned),
                client_id: metadata_string(metadata, "x-mq-client-id"),
                language: metadata_string(metadata, "x-mq-language"),
                client_version: metadata_string(metadata, "x-mq-client-version"),
                namespace: metadata_string(metadata, "x-mq-namespace"),
                connection_id: metadata_string(metadata, "x-mq-channel-id"),
                deadline_at,
                received_at,
            },
        ))
    }

    /// Builds Proxy metadata from immutable request and session views.
    ///
    /// Network addresses are read only from the transport-owned session. An
    /// embedded session deliberately produces no socket metadata.
    fn from_remoting_request(rpc_name: &'static str, request: &RemotingRequest) -> Self {
        let command = request.command();
        let received_at = Instant::now();
        let deadline_at = request
            .control()
            .deadline()
            .map(|deadline| received_at.checked_add(deadline.remaining()).unwrap_or(received_at));
        let (remote_addr, local_addr) = match request.session() {
            SessionView::Network {
                local_addr,
                remote_addr,
                ..
            } => (Some(remote_addr.to_string()), Some(local_addr.to_string())),
            SessionView::Embedded { .. } => (None, None),
            _ => (None, None),
        };

        Self::from_metadata(
            rpc_name,
            ProxyRequestMetadata {
                request_id: request.original_identity().original_opaque().to_string(),
                remote_addr,
                local_addr,
                client_id: remoting_ext_field(command, "clientID"),
                language: Some(format!("{:?}", command.language())),
                client_version: Some(command.version().to_string()),
                namespace: remoting_ext_field(command, "namespace"),
                connection_id: Some(format!("{:?}", request.session().id())),
                deadline_at,
                received_at,
            },
        )
    }
}

fn metadata_string(metadata: &MetadataMap, key: &'static str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn remoting_ext_field(request: &RemotingCommand, key: &str) -> Option<String> {
    request.ext_fields().and_then(|fields| {
        fields
            .iter()
            .find(|(field_key, _)| field_key.as_str() == key)
            .map(|(_, value)| value.to_string())
    })
}

fn parse_grpc_timeout_metadata(metadata: &MetadataMap) -> ProxyResult<Option<Duration>> {
    let Some(raw) = metadata.get("grpc-timeout") else {
        return Ok(None);
    };
    let raw = raw
        .to_str()
        .map_err(|_| ProxyError::invalid_metadata("grpc-timeout must be valid ASCII"))?;
    parse_grpc_timeout(raw)
        .ok_or_else(|| {
            ProxyError::invalid_metadata(format!(
                "grpc-timeout '{raw}' must use the gRPC timeout format <digits><H|M|S|m|u|n>",
            ))
        })
        .map(Some)
}

pub(crate) fn parse_grpc_timeout(raw: &str) -> Option<Duration> {
    if raw.len() < 2 {
        return None;
    }

    let (value, unit) = raw.split_at(raw.len() - 1);
    let value = value.parse::<u64>().ok()?;

    match unit {
        "H" => Some(Duration::from_secs(value.saturating_mul(60).saturating_mul(60))),
        "M" => Some(Duration::from_secs(value.saturating_mul(60))),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use tonic::Request;

    use super::parse_grpc_timeout;
    use super::GrpcTransportContext;
    use super::ProxyContext;
    use super::ProxyContextExt;
    use super::ProxyContextWithPrincipal;
    use crate::error::ProxyError;

    #[test]
    fn parse_grpc_timeout_supports_multiple_units() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("25m"), Some(Duration::from_millis(25)));
        assert_eq!(parse_grpc_timeout("100u"), Some(Duration::from_micros(100)));
    }

    #[test]
    fn parse_grpc_timeout_rejects_invalid_values() {
        assert_eq!(parse_grpc_timeout(""), None);
        assert_eq!(parse_grpc_timeout("abc"), None);
        assert_eq!(parse_grpc_timeout("12X"), None);
    }

    #[test]
    fn proxy_context_reads_local_addr_from_transport_context() {
        let mut request = Request::new(());
        request.extensions_mut().insert(GrpcTransportContext::new(
            "127.0.0.1:8080".parse().expect("socket addr"),
        ));
        request
            .metadata_mut()
            .insert("x-mq-client-id", "client-a".parse().expect("client id metadata"));

        let context = ProxyContext::from_grpc_request("QueryRoute", &request).expect("context should be constructed");
        assert_eq!(context.local_addr(), Some("127.0.0.1:8080"));
        assert_eq!(context.client_id(), Some("client-a"));
    }

    #[test]
    fn proxy_context_rejects_invalid_grpc_timeout_metadata() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("grpc-timeout", "bad-timeout".parse().expect("timeout metadata"));

        let error = ProxyContext::from_grpc_request("QueryRoute", &request).expect_err("context should reject timeout");
        assert!(matches!(error, ProxyError::InvalidMetadata { .. }));
        assert!(error.to_string().contains("grpc-timeout"));
    }

    #[test]
    fn proxy_context_freezes_grpc_timeout_as_an_absolute_deadline() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("grpc-timeout", "1S".parse().expect("timeout metadata"));
        let before = Instant::now();

        let context =
            ProxyContext::from_grpc_request("ReceiveMessage", &request).expect("context should freeze the timeout");
        let after = Instant::now();
        let deadline_at = context.deadline_at().expect("absolute deadline");

        assert!(deadline_at >= before + Duration::from_secs(1));
        assert!(deadline_at <= after + Duration::from_secs(1));
        assert!(context
            .deadline()
            .is_some_and(|remaining| { !remaining.is_zero() && remaining <= Duration::from_secs(1) }));
    }

    #[test]
    fn without_principal_preserves_metadata_and_drops_proof() {
        let mut context = ProxyContextWithPrincipal::<String>::for_internal_client("SendMessage", "client-a");
        context.set_authenticated_principal("alice".to_owned());

        let neutral = context.without_principal();

        assert_eq!(neutral.request_id(), context.request_id());
        assert_eq!(neutral.rpc_name(), "SendMessage");
        assert_eq!(neutral.client_id(), Some("client-a"));
        assert!(neutral.authenticated_principal().is_none());
        assert_eq!(context.authenticated_principal().map(String::as_str), Some("alice"));
    }
}
